import sys
import os
import json
import threading
import ctypes
import tempfile
import urllib.request
import msvcrt

try:
    from PySide6.QtWidgets import QApplication, QMessageBox, QDialog
    from PySide6.QtGui import QIcon
    from PySide6.QtCore import QTimer
except ImportError as e:
    try:
        ctypes.windll.user32.MessageBoxW(
            0, 
            f"Critical Error: Failed to load PySide6 (GUI Library).\n\n"
            f"Details: {e}\n\n"
            "This usually means the Python environment is missing dependencies.\n"
            "Please ensure you have installed the requirements:\n"
            "pip install -r requirements.txt", 
            "NotionLink - Launch Error", 
            0x10
        )
    except Exception:
        pass
    sys.exit(1)


_instance_lock_handle = None


def _handoff_to_running_instance():
    try:
        port = config.get("server_port", 8000)
        req = urllib.request.Request(f"http://127.0.0.1:{port}/_notionlink/open-dashboard")
        with urllib.request.urlopen(req, timeout=1.5) as response:
            return response.status == 200
    except Exception:
        return False


def _acquire_single_instance_lock():
    lock_file_path = os.path.join(tempfile.gettempdir(), "notionlink.instance.lock")
    lock_handle = open(lock_file_path, "a+")
    try:
        lock_handle.seek(0)
        msvcrt.locking(lock_handle.fileno(), msvcrt.LK_NBLCK, 1)
        lock_handle.seek(0)
        lock_handle.write(str(os.getpid()))
        lock_handle.flush()
        return lock_handle
    except OSError:
        try:
            lock_handle.close()
        except Exception:
            pass
        return None

try:
    from src.core import (APP_VERSION, config, config_file_path, logger, 
                        init_sentry_if_enabled, exception_handler)
    from src.notion import check_notion_status_once, run_startup_sync
    from src.server import start_server_blocking, TRAY_ICON_ICO
    from src.ui_styles import DARK_STYLESHEET
    from src.ui_dialogs import InitialSetupDialog
    from src.ui_main import NotionLinkTrayApp
except ImportError as e:
    app_dummy = QApplication(sys.argv)
    error_box = QMessageBox()
    error_box.setIcon(QMessageBox.Critical)
    error_box.setWindowTitle("NotionLink - Dependency Error")
    error_box.setText("A required Python component is missing.")
    error_box.setInformativeText(
        f"Error Details: {e}\n\n"
        "Please check that all dependencies are installed properly.\n"
        "Try running: pip install -r requirements.txt"
    )
    error_box.setStandardButtons(QMessageBox.Ok)
    error_box.exec()
    sys.exit(1)


def main():
    global _instance_lock_handle
    _instance_lock_handle = _acquire_single_instance_lock()
    if _instance_lock_handle is None:
        if _handoff_to_running_instance():
            print("Another NotionLink instance is already running. Brought existing dashboard to front.")
            return
        print("Another NotionLink instance appears to be running.")
        return
    
    print("Sentry initialization deferred (background).")
    

    sys.excepthook = exception_handler
    
    if sys.platform.startswith('win'):
        try:
            appid = "NotionLink"
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(appid)
            print(f"Set Windows AppUserModelID: {appid}")
        except Exception as e:
            print(f"Failed to set AppUserModelID: {e}")

    QApplication.setStyle("Fusion")
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(TRAY_ICON_ICO))
    app.setQuitOnLastWindowClosed(False)
    
    app.tray_app = NotionLinkTrayApp(app)
    
    if not config.get("tutorial_completed", False):
        print("First run detected. Starting setup wizard...")
        
        wizard = InitialSetupDialog(app.tray_app) 
        if wizard.exec() != QDialog.Accepted:
            print("Setup not completed. Exiting.")
            sys.exit(0)
        
        try:
            with open(config_file_path, "r") as config_file:
                from src import core
                core.config.clear()
                core.config.update(json.load(config_file))
        except Exception as e:
            print(f"Failed to reload config after wizard: {e}")
            sys.exit(1)
    
    def _start_background_services():
        try:
            print("Starting Notion status check...")
            app.tray_app.run_status_check_thread()

            print("Starting HTTP server thread...")
            threading.Thread(target=start_server_blocking, args=(app.tray_app,), daemon=True).start()

            print("Starting file observer (background)...")
            threading.Thread(target=app.tray_app.start_file_observer, daemon=True).start()

            print("Running startup sync...")
            threading.Thread(target=run_startup_sync, args=(app.tray_app,), daemon=True).start()
        except Exception as e:
            print(f"Error starting background services: {e}")

    QTimer.singleShot(0, _start_background_services)
    
    print("Starting main GUI loop (app.exec())...")
    exit_code = app.exec()

    try:
        if _instance_lock_handle:
            _instance_lock_handle.seek(0)
            msvcrt.locking(_instance_lock_handle.fileno(), msvcrt.LK_UNLCK, 1)
            _instance_lock_handle.close()
    except Exception:
        pass

    sys.exit(exit_code)


if __name__ == '__main__':
    main()
