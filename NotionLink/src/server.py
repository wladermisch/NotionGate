import os
import sys
import socket
import socketserver
import threading
import time
import platform
import fnmatch
from http.server import BaseHTTPRequestHandler
from http import HTTPStatus
from urllib.parse import unquote, quote
from watchdog.events import FileSystemEventHandler
import win32com.client

from .core import config, logger, notified_errors, file_to_page_map, httpd, is_user_error, resource_path
from .notion import (
    sync_file_to_notion, find_notion_page_by_filename,
    archive_notion_page, update_notion_page_filename
)

# Windows shell for autostart management
shell = win32com.client.Dispatch("WScript.Shell")

# =============================================================================
# CONSTANTS
# =============================================================================

ASSETS_DIR = 'assets'
TRAY_ICON_ICO = resource_path(os.path.join(ASSETS_DIR, 'logo.ico'))
_handoff_tray_app = None

# =============================================================================
# HTTP SERVER
# =============================================================================

def open_explorer(Path):
    # Open a file in Windows Explorer.
    full_path = unquote(Path[1:].replace("/", "\\"))
    print(f"Opening full path with os.startfile: {full_path}")
    try:
        os.startfile(full_path)
    except Exception as e:
        print(f"Failed to open file: {e}")


class MyHandler(BaseHTTPRequestHandler):
    # HTTP request handler for opening files via browser links.
    
    def do_GET(self):
        print('Getting path : --------')
        print(self.path)
        if self.path.startswith('/_notionlink/open-dashboard'):
            try:
                if _handoff_tray_app is not None:
                    _handoff_tray_app.open_dashboard_from_handoff("NotionLink is already running in this session.")
                payload = b'{"ok": true}'
                self.send_response(HTTPStatus.OK)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except Exception as e:
                error_payload = f'{{"ok": false, "error": "{str(e)}"}}'.encode('utf-8', errors='ignore')
                self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(error_payload)))
                self.end_headers()
                self.wfile.write(error_payload)
            return

        if not ('GET' in self.path) and not ('favicon' in self.path):
            # Attempt to open the file locally, then return a small HTML page
            # that tries to close the browser tab. Note: modern browsers often
            # block scripts from closing windows they didn't open, so this is
            # a best-effort fallback with a clear instruction for the user.
            open_explorer(self.path)
            html = b"""
            <!doctype html>
            <html><head><meta charset='utf-8'><title>Opening file...</title></head>
            <body>
            <p>Opening file locally. You can close this tab.</p>
            <script>
            (function(){
                try {
                    // Try to close the tab/window (may be blocked by browser).
                    window.open('', '_self');
                    window.close();
                } catch(e) {
                    // ignore
                }
                // As a fallback, navigate to about:blank shortly after.
                setTimeout(function(){ window.location.href = 'about:blank'; }, 300);
            })();
            </script>
            </body></html>
            """
            self.send_response(HTTPStatus.OK)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(html)))
            self.end_headers()
            self.wfile.write(html)
        else:
            self.send_response(HTTPStatus.OK)
            self.end_headers()
            self.wfile.write(b'File Connection Server is running.')
    
    def log_message(self, format, *args):
        logger.info("%s - - [%s] %s" %
                     (self.client_address[0],
                      self.log_date_time_string(),
                      format%args))


class ReusableTCPServer(socketserver.TCPServer):
    # TCP server with address reuse enabled.
    allow_reuse_address = True
    
    def server_bind(self):
        if hasattr(socket, 'SO_REUSEADDR'):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if platform.system() == 'Windows' and hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 0)
        super().server_bind()


def is_port_in_use(port, host=''):
    # Check if a port is already in use.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind((host, port))
            return False
        except (OSError, socket.error):
            return True


def start_server_blocking(tray_app):
    # Start the HTTP server in blocking mode.
    global httpd
    global _handoff_tray_app
    from .core import httpd as httpd_global  # Use global from core
    
    port = config.get('server_port')
    
    try:
        _handoff_tray_app = tray_app
        if is_port_in_use(port):
            raise OSError(f"Port {port} is already in use. Another NotionLink instance or application may be using it.")

        httpd_instance = ReusableTCPServer(("", port), MyHandler)
        httpd = httpd_instance
        
        # Also update the global reference in core
        import src.core
        src.core.httpd = httpd_instance
        
        print(f"Starting server on port {port}...")
        httpd_instance.serve_forever()
        
    except (OSError, PermissionError) as e:
        msg = f"Error: Could not bind to port {port}. {e}"
        print(msg)
        logger.warning(msg)
        
        error_key = f"port:{port}:{type(e).__name__}"
        if error_key not in notified_errors:
            notified_errors.add(error_key)
            tray_app.server_error_signal.emit(str(e))
    except Exception as e:
        msg = f"Critical Server Error: {e}"
        print(msg)
        from .core import error_logger
        error_logger.error(msg)
        
        error_key = f"server:{type(e).__name__}:{str(e)[:50]}"
        if error_key not in notified_errors:
            notified_errors.add(error_key)
            tray_app.server_error_signal.emit(msg)
        raise e
    finally:
        print("Server loop exited.")


# =============================================================================
# FILE SYSTEM MONITORING
# =============================================================================

class NotionFileHandler(FileSystemEventHandler):
    # Watchdog event handler for file system changes.
    
    def __init__(self, config_data, mapping_config, mapping_type, tray_app, watched_root=None):
        self.config_data = config_data
        self.mapping_config = mapping_config
        self.mapping_type = mapping_type
        self.tray_app = tray_app
        self.watched_root = watched_root
        print(f"Watcher Handler init for {mapping_type} ...{mapping_config['notion_id'][-6:]}")

    def _is_excluded_path(self, candidate_path):
        exclude_patterns = self.mapping_config.get("exclude_folders", [])
        if not exclude_patterns:
            return False

        candidate_norm = os.path.normcase(os.path.normpath(candidate_path))
        base_root = os.path.normcase(os.path.normpath(self.watched_root)) if self.watched_root else None
        rel_path = ""
        if base_root:
            try:
                rel_path = os.path.relpath(candidate_norm, start=base_root).replace("\\", "/")
            except ValueError:
                rel_path = candidate_norm.replace("\\", "/")
        else:
            rel_path = candidate_norm.replace("\\", "/")

        for pattern in exclude_patterns:
            p = (pattern or "").strip()
            if not p:
                continue

            expanded = os.path.normcase(os.path.normpath(os.path.expandvars(p)))
            if os.path.isabs(expanded):
                if candidate_norm == expanded or candidate_norm.startswith(expanded + os.sep):
                    return True

            p_rel = p.replace("\\", "/")
            if fnmatch.fnmatch(os.path.basename(candidate_norm), p):
                return True
            if rel_path and (fnmatch.fnmatch(rel_path, p_rel) or rel_path.startswith(p_rel.rstrip("/") + "/")):
                return True

        return False

    def on_created(self, event):
        # handle file creation events
        if self._is_excluded_path(event.src_path):
            print(f"Ignoring (excluded folder): {event.src_path}")
            return

        if event.is_directory:
            if self.mapping_config.get("folder_links", False):
                folder_path = event.src_path
                folder_name = os.path.basename(folder_path)
                print(f"New folder detected by watcher: {folder_path}")
                sync_file_to_notion(
                    folder_path,
                    self.config_data,
                    self.mapping_config,
                    self.mapping_type,
                    self.tray_app,
                    is_batch=True
                )
                print(f"Folder link sync queued: {folder_name}")
            return
        
        filepath = event.src_path
        filename = os.path.basename(filepath)
        
        try:
            ignore_exts = self.mapping_config.get("ignore_extensions", [])
            for pattern in ignore_exts:
                if fnmatch.fnmatch(filename, pattern):
                    print(f"Ignoring (ext filter): {filename}")
                    return
            
            ignore_files = self.mapping_config.get("ignore_files", [])
            for name in ignore_files:
                if fnmatch.fnmatch(filename, name):
                    print(f"Ignoring (file/wildcard filter): {filename}")
                    return
        except Exception as e:
            print(f"Error applying filters: {e}")

        print(f"New file detected by watcher: {filepath}")
        
        def delayed_upload(path, config_data, mapping_config, mapping_type, tray_app):
            try:
                print(f"Waiting 2.5s for file {filename} to stabilize...")
                time.sleep(2.5)
                
                if not os.path.exists(path):
                    print(f"Ignoring (file deleted during delay): {filename}")
                    return
                    
                sync_file_to_notion(path, config_data, mapping_config, mapping_type, tray_app, is_batch=True)
            except Exception as e:
                print(f"Error in delayed upload thread for {filename}: {e}")
                raise e

        upload_thread = threading.Thread(
            target=delayed_upload,
            args=(filepath, self.config_data, self.mapping_config, self.mapping_type, self.tray_app),
            daemon=True
        )
        upload_thread.start()
    
    def on_deleted(self, event):
        # handle file deletion events
        if self._is_excluded_path(event.src_path):
            return

        if event.is_directory:
            return
        
        if not self.mapping_config.get("full_lifecycle_sync", True):
            return
        
        filepath = event.src_path
        filename = os.path.basename(filepath)
        
        print(f"File deleted: {filepath}")
        
        if self.mapping_type == "database":
            global file_to_page_map
            
            if filepath in file_to_page_map:
                page_info = file_to_page_map[filepath]
                page_id = page_info["page_id"]
                
                archive_notion_page(
                    page_id,
                    self.config_data.get("notion_token"),
                    filename,
                    self.tray_app
                )
                
                del file_to_page_map[filepath]
            else:
                database_id = self.mapping_config["notion_id"]
                page_id = find_notion_page_by_filename(
                    database_id,
                    filename,
                    self.config_data.get("notion_token")
                )
                
                if page_id:
                    archive_notion_page(
                        page_id,
                        self.config_data.get("notion_token"),
                        filename,
                        self.tray_app
                    )
        elif self.mapping_type == "page":
            from .notion import remove_file_from_page
            page_id = self.mapping_config["notion_id"]
            
            # Calculate server link for the deleted file to help find it
            server_host = self.config_data.get("server_host")
            port = self.config_data.get("server_port")
            server_address = f"{server_host}:{port}/"
            url_path = filepath.replace("\\", "/")
            if url_path.startswith('/'):
                url_path = url_path[1:]
            url_path = quote(url_path, safe='/')
            server_link = server_address + url_path
            
            remove_file_from_page(
                page_id,
                filename,
                self.config_data.get("notion_token"),
                self.tray_app,
                server_link=server_link
            )
    
    def on_moved(self, event):
        # handle file rename/move events
        if self._is_excluded_path(event.src_path) or self._is_excluded_path(event.dest_path):
            return

        if event.is_directory:
            return
        
        if not self.mapping_config.get("full_lifecycle_sync", True):
            return
        
        src_path = event.src_path
        dest_path = event.dest_path
        old_filename = os.path.basename(src_path)
        new_filename = os.path.basename(dest_path)
        
        if os.path.dirname(src_path) != os.path.dirname(dest_path):
            print(f"File moved to different directory: {src_path} → {dest_path}")
            self.on_deleted(type('Event', (), {'src_path': src_path, 'is_directory': False})())
            return
        
        print(f"File renamed: {old_filename} → {new_filename}")
        
        if self.mapping_type == "database":
            global file_to_page_map
            
            server_host = self.config_data.get("server_host")
            port = self.config_data.get("server_port")
            server_address = f"{server_host}:{port}/"
            url_path = dest_path.replace("\\", "/")
            if url_path.startswith('/'):
                url_path = url_path[1:]
            
            # Encode path to handle spaces and special characters
            url_path = quote(url_path, safe='/')
            new_server_link = server_address + url_path
            
            if src_path in file_to_page_map:
                page_info = file_to_page_map[src_path]
                page_id = page_info["page_id"]
                
                update_notion_page_filename(
                    page_id,
                    new_filename,
                    new_server_link,
                    self.config_data.get("notion_token"),
                    old_filename,
                    self.tray_app
                )
                
                del file_to_page_map[src_path]
                file_to_page_map[dest_path] = {
                    "page_id": page_id,
                    "database_id": page_info["database_id"],
                    "filename": new_filename
                }
            else:
                database_id = self.mapping_config["notion_id"]
                page_id = find_notion_page_by_filename(
                    database_id,
                    old_filename,
                    self.config_data.get("notion_token")
                )
                
                if page_id:
                    update_notion_page_filename(
                        page_id,
                        new_filename,
                        new_server_link,
                        self.config_data.get("notion_token"),
                        old_filename,
                        self.tray_app
                    )
                    
                    file_to_page_map[dest_path] = {
                        "page_id": page_id,
                        "database_id": database_id,
                        "filename": new_filename
                    }
        elif self.mapping_type == "page":
            from .notion import update_file_on_page, remove_file_from_page, sync_file_to_notion
            page_id = self.mapping_config["notion_id"]
            
            # Calculate new link
            server_host = self.config_data.get("server_host")
            port = self.config_data.get("server_port")
            server_address = f"{server_host}:{port}/"
            url_path = dest_path.replace("\\", "/")
            if url_path.startswith('/'):
                url_path = url_path[1:]
            url_path = quote(url_path, safe='/')
            new_server_link = server_address + url_path
            
            # Calculate old link for fallback
            old_url_path = src_path.replace("\\", "/")
            if old_url_path.startswith('/'):
                old_url_path = old_url_path[1:]
            old_url_path = quote(old_url_path, safe='/')
            old_server_link = server_address + old_url_path
            
            success = update_file_on_page(
                page_id,
                old_filename,
                new_filename,
                new_server_link,
                self.config_data.get("notion_token"),
                self.tray_app,
                old_link=old_server_link
            )
            
            if not success:
                print(f"Update failed for {old_filename}, falling back to delete+upload")
                # Try to remove the old file (using link if filename fails)
                remove_file_from_page(
                    page_id,
                    old_filename,
                    self.config_data.get("notion_token"),
                    self.tray_app,
                    server_link=old_server_link
                )
                # Upload the new file
                sync_file_to_notion(
                    dest_path,
                    self.config_data,
                    self.mapping_config,
                    self.mapping_type,
                    self.tray_app,
                    is_batch=True
                )


# =============================================================================
# WINDOWS AUTOSTART MANAGEMENT
# =============================================================================

def get_startup_folder_path():
    # Get Windows Startup folder path.
    return os.path.join(os.environ['APPDATA'], 'Microsoft', 'Windows', 'Start Menu', 'Programs', 'Startup')


def get_shortcut_link_path():
    # Get path to NotionLink startup shortcut.
    return os.path.join(get_startup_folder_path(), "NotionLink.lnk")


def get_executable_path():
    # Get path to Python executable (use pythonw for non-frozen).
    exe_path = sys.executable
    if not getattr(sys, 'frozen', False) and "python.exe" in exe_path.lower():
        exe_path = exe_path.replace("python.exe", "pythonw.exe")
    return exe_path


def get_script_file_path():
    # Get path to main script file (None if frozen).
    if getattr(sys, 'frozen', False):
        return None
    else:
        # Go up from src/ to get main NotionLink.pyw
        return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'NotionLink.pyw'))


def manage_autostart(enable):
    # Enable or disable Windows autostart.
    shortcut_path = get_shortcut_link_path()
    
    if enable:
        if os.path.isfile(shortcut_path):
            print("Autostart shortcut already exists.")
            return
        try:
            target_exe = get_executable_path()
            script_file = get_script_file_path()
            working_dir = os.path.dirname(target_exe if not script_file else script_file)
            
            shortcut = shell.CreateShortCut(shortcut_path)
            shortcut.TargetPath = target_exe
            
            if script_file:
                shortcut.Arguments = f'"{script_file}"'
            
            shortcut.WorkingDirectory = working_dir
            shortcut.IconLocation = TRAY_ICON_ICO
            shortcut.Description = "Starts NotionLink utility for local file access."
            shortcut.Save()
            print("Autostart shortcut created.")
        except Exception as e:
            print(f"Error creating shortcut: {e}")
            raise e
    else:
        if os.path.isfile(shortcut_path):
            try:
                os.remove(shortcut_path)
                print("Autostart shortcut removed.")
            except Exception as e:
                print(f"Error removing shortcut: {e}")
                raise e
        else:
            print("Autostart shortcut already removed.")
