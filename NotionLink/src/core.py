import sys
import os
import json
import logging
from logging.handlers import RotatingFileHandler
import traceback
import threading
import platform
import socket
import uuid
from collections import defaultdict

APP_VERSION = "5.0.0-ui"

config_file_path = "config.json"

default_config = {
    "server_port": 3030,
    "server_host": "http://localhost",
    "notion_token": "PLEASE_ENTER_YOUR_NEW_TOKEN_HERE",
    "page_mappings": [],
    "database_mappings": [],
    "tutorial_completed": False,
    "autostart_with_windows": False,
    "sentry_enabled": True
}

# Globals
observer = None
httpd = None
link_cache = {}
notion_status = "Notion: Checking..."
notification_batch = defaultdict(list)
notified_errors = set()
file_to_page_map = {}
offline_mode = False
last_network_notification_time = 0

pending_uploads = []
pending_uploads_lock = threading.Lock()
is_recovering_connection = False

if getattr(sys, 'frozen', False):
    path = os.path.dirname(sys.executable)
else:
    path = os.path.dirname(os.path.dirname(__file__))

log_dir = path
notionlog_path = os.path.join(log_dir, "notionlink.log")
errorlog_path = os.path.join(log_dir, "error.log")

sentry_sdk = None

logger = logging.getLogger("notionlink")
logger.setLevel(logging.INFO)

info_handler = RotatingFileHandler(notionlog_path, maxBytes=5*1024*1024, backupCount=1, encoding="utf-8")
info_handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
info_handler.setFormatter(formatter)

logger.addHandler(info_handler)

error_logger = logging.getLogger("notionlink.error")
error_logger.setLevel(logging.ERROR)

_error_handler_attached = False
_error_handler_lock = threading.Lock()


def ensure_error_log_handler():
    global _error_handler_attached
    if _error_handler_attached:
        return
    with _error_handler_lock:
        if _error_handler_attached:
            return
        try:
            err_handler = RotatingFileHandler(errorlog_path, maxBytes=10*1024*1024, backupCount=3, encoding="utf-8")
            err_handler.setLevel(logging.ERROR)
            err_handler.setFormatter(formatter)
            logger.addHandler(err_handler)
            error_logger.addHandler(err_handler)
            _error_handler_attached = True
            logger.info("Error log handler attached")
        except Exception as e:
            logger.error(f"Failed to attach error log handler: {e}")


class StreamToLogger:
    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level
        self._buff = ''
    
    def write(self, buf):
        buf = buf.rstrip('\n')
        if buf:
            # Ensure error log file is created only on first error-level write
            try:
                if self.level >= logging.ERROR:
                    ensure_error_log_handler()
            except Exception:
                pass
            self.logger.log(self.level, buf)
    
    def flush(self):
        pass


sys.stdout = StreamToLogger(logger, logging.INFO)
sys.stderr = StreamToLogger(error_logger, logging.ERROR)

NETWORK_ERROR_STRINGS = [
    'timeout', 'timed out', 'connection', 'handshake', 'getaddrinfo', 
    'name resolution', 'host', 'socket', 'client', 'remote', 
    '10065', '10054', '10060', '10061', '11001'
]

def is_user_error(exc_value):
    error_str = str(exc_value).lower()
    
    # Dependencies missing
    if isinstance(exc_value, (ImportError, ModuleNotFoundError)):
        return True
    
    # File not found / missing installation files
    if isinstance(exc_value, FileNotFoundError):
        if 'assets' in error_str or 'logo.ico' in error_str:
            return True
    
    # Port/permission errors
    if isinstance(exc_value, (OSError, PermissionError)):
        if hasattr(exc_value, 'errno') and exc_value.errno in (10013, 10048, 48, 98):
            return True
        if 'port' in error_str and ('already in use' in error_str or 'bind' in error_str or 'address already in use' in error_str):
            return True
    
    # Notion API errors
    if '404' in error_str or 'could not find block' in error_str or 'could not find page' in error_str:
        return True
    if '401' in error_str or 'unauthorized' in error_str or 'invalid token' in error_str:
        return True
    if '403' in error_str or 'forbidden' in error_str or 'not shared' in error_str:
        return True
    if 'api_error' in error_str and ('validation' in error_str or 'invalid' in error_str):
        return True
    
    # Network / Timeout errors
    if any(x in error_str for x in NETWORK_ERROR_STRINGS):
        return True
    
    return False


def exception_handler(exc_type, exc_value, exc_tb):
    global sentry_sdk

    # Treat Ctrl+C / normal interpreter exits as non-errors.
    if exc_type in (KeyboardInterrupt, SystemExit):
        logger.info(f"Application exiting: {exc_type.__name__}")
        return
    
    tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_tb))

    # Ensure the error file handler is attached before writing uncaught exceptions
    try:
        ensure_error_log_handler()
    except Exception:
        pass

    if is_user_error(exc_value):
        msg = f"User configuration/network error (not sent to Sentry): {exc_value}"
        logger.warning(msg)
        return

    error_logger.error(f"Uncaught exception: {exc_value}\nTraceback:\n{tb}")
    
    device_info = {
        'platform': platform.platform(),
        'python_version': platform.python_version(),
        'hostname': socket.gethostname(),
        'user': os.getenv('USERNAME') or os.getenv('USER') or 'unknown',
        'machine_id': str(uuid.getnode()),
    }
    
    sentry_device_info = {
        'platform': device_info['platform'],
        'python_version': device_info['python_version'],
    }
    
    try:
        if sentry_sdk is not None:
            with sentry_sdk.push_scope() as scope:
                scope.set_context("Device Info", sentry_device_info)
                sentry_sdk.capture_exception(exc_value)
            logger.error(f"Bug report sent to Sentry: {exc_value}")
        else:
            logger.error(f"Bug logged locally (Sentry disabled): {exc_value}")
    except Exception as e:
        logger.error(f"Failed to send error to Sentry: {e}")


def init_sentry_if_enabled():
    global sentry_sdk
    
    try:
        if not isinstance(config, dict):
            return
        
        if not config.get('sentry_enabled', True):
            logger.info('Sentry disabled by configuration.')
            sentry_sdk = None  # Ensure it's None
            return
        
        import importlib
        sentry = importlib.import_module('sentry_sdk')
        sentry.init(
            dsn="https://f97cc16cb262264495392aa853c700bb@o4510309097865216.ingest.de.sentry.io/4510309121982544",
            send_default_pii=False,
            traces_sample_rate=1.0,
            release=f"notionlink@{APP_VERSION}",
        )
        sentry_sdk = sentry
        logger.info(f'Sentry initialized for Alpha Build {APP_VERSION}.')
            
    except Exception as e:
        logger.error(f'Sentry init failed: {e}')


sys.excepthook = exception_handler


def migrate_config_if_needed(config_obj):
    from .notion import extract_id_and_title_from_link, get_notion_title
    
    if "folder_mappings" in config_obj:
        print("Old config structure detected. Migrating...")
        old_mappings = config_obj.pop("folder_mappings", [])
        token = config_obj.get("notion_token")
        
        pages = {}
        for mapping in old_mappings:
            link_or_id = mapping.get("notion_page_link_or_id", "")
            id_tuple = extract_id_and_title_from_link(link_or_id)
            if not id_tuple:
                continue
            
            page_id, title_from_url = id_tuple
            folder_path = mapping.get("folder_path")
            if not folder_path:
                continue

            if page_id not in pages:
                real_title = get_notion_title(page_id, token, is_db=False)
                pages[page_id] = {
                    "notion_title": real_title or title_from_url or f"Page ID: ...{page_id[-6:]}",
                    "notion_id": page_id,
                    "folders": [],
                    "ignore_extensions": ["*.tmp", ".*", "desktop.ini"],
                    "ignore_files": []
                }
            pages[page_id]["folders"].append(folder_path)

        config_obj["page_mappings"] = list(pages.values())
        print(f"Migrated {len(old_mappings)} old mappings into {len(pages)} new page mappings.")
        return True
    return False


def load_config():
    global config
    
    config_path = os.path.join(path, config_file_path)
    
    try:
        if not os.path.isfile(config_path):
            with open(config_path, "w") as config_file:
                json.dump(default_config, config_file, indent=4)
            print("Config file created with default settings.")
            config = default_config
        else:
            with open(config_path, "r") as config_file:
                config = json.load(config_file)
                
            config_updated = migrate_config_if_needed(config)

            for key, value in default_config.items():
                if key not in config:
                    config[key] = value
                    config_updated = True
            
            if config_updated:
                with open(config_path, "w") as config_file:
                    json.dump(config, config_file, indent=4)
                print("Config file migrated or updated.")
                
        print("Configuration loaded.")
        try:
            # Initialize Sentry in a background thread to avoid blocking imports/startup.
            # Sentry initialization can import network/IO heavy modules and may delay
            # application startup significantly on some environments.
            threading.Thread(target=init_sentry_if_enabled, daemon=True).start()
        except Exception as e:
            logger.error(f"Error scheduling Sentry initialization: {e}")
    except Exception as e:
        print(f"Error loading config, using defaults. Error: {e}")
        config = default_config
        try:
            threading.Thread(target=init_sentry_if_enabled, daemon=True).start()
        except Exception:
            pass
    
    return config


def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.dirname(os.path.dirname(__file__))  # Go up one level from src/
    return os.path.join(base_path, relative_path)


config = {}
load_config()
