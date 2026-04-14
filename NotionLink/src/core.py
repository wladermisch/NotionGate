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
import hashlib
import urllib.request
from collections import defaultdict

APP_VERSION = "5.1.4"
INSTALL_COUNTER_URL = "https://notionlink-counter.ermisch-wlad.workers.dev/install"

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

# Shared runtime state
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
            # Lazily attach error logging when needed.
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

    if isinstance(exc_value, (ImportError, ModuleNotFoundError)):
        return True

    if isinstance(exc_value, FileNotFoundError):
        if 'assets' in error_str or 'logo.ico' in error_str:
            return True

    if isinstance(exc_value, (OSError, PermissionError)):
        if hasattr(exc_value, 'errno') and exc_value.errno in (10013, 10048, 48, 98):
            return True
        if 'port' in error_str and ('already in use' in error_str or 'bind' in error_str or 'address already in use' in error_str):
            return True

    if '404' in error_str or 'could not find block' in error_str or 'could not find page' in error_str:
        return True
    if '401' in error_str or 'unauthorized' in error_str or 'invalid token' in error_str:
        return True
    if '403' in error_str or 'forbidden' in error_str or 'not shared' in error_str:
        return True
    if 'api_error' in error_str and ('validation' in error_str or 'invalid' in error_str):
        return True

    if any(x in error_str for x in NETWORK_ERROR_STRINGS):
        return True
    
    return False


def exception_handler(exc_type, exc_value, exc_tb):
    global sentry_sdk

    # Ignore normal process termination paths.
    if exc_type in (KeyboardInterrupt, SystemExit):
        logger.info(f"Application exiting: {exc_type.__name__}")
        return
    
    tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_tb))

    # Make sure error output has a backing file.
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
            # Defer Sentry setup so startup stays responsive.
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


def _get_install_id_path():
    appdata_dir = os.getenv("APPDATA")
    if appdata_dir:
        base_dir = os.path.join(appdata_dir, "NotionLink")
    else:
        base_dir = path
    return os.path.join(base_dir, "install_id")


def _load_or_create_install_id():
    install_id_path = _get_install_id_path()
    try:
        if os.path.isfile(install_id_path):
            with open(install_id_path, "r", encoding="utf-8") as install_file:
                install_id = install_file.read().strip()
            if install_id:
                return install_id
    except Exception as e:
        logger.warning(f"Failed to read install id: {e}")

    install_id = str(uuid.uuid4())
    try:
        os.makedirs(os.path.dirname(install_id_path), exist_ok=True)
        with open(install_id_path, "w", encoding="utf-8") as install_file:
            install_file.write(install_id)
    except Exception as e:
        logger.warning(f"Failed to persist install id: {e}")
    return install_id


def send_install_counter_ping_for_first_setup():
    try:
        install_id = _load_or_create_install_id()
        id_hash = hashlib.sha256(install_id.encode("utf-8")).hexdigest()
        payload = {
            "id_hash": id_hash,
            "app_version": APP_VERSION,
            "platform": platform.system(),
        }

        request = urllib.request.Request(
            INSTALL_COUNTER_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        with urllib.request.urlopen(request, timeout=4) as response:
            status_code = getattr(response, "status", 200)

        if 200 <= status_code < 300:
            logger.info("Install counter registration sent after first-time setup.")
        else:
            logger.warning(f"Install counter ping returned status {status_code}")
    except Exception as e:
        logger.warning(f"Install counter ping failed: {e}")


def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.dirname(os.path.dirname(__file__))
    return os.path.join(base_path, relative_path)


config = {}
load_config()
