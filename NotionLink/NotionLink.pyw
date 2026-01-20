# NotionLink - wladermisch on GitHub

import sys
import os
import json
import threading
import ctypes
from PySide6.QtWidgets import QApplication, QMessageBox, QDialog
from PySide6.QtGui import QIcon
from PySide6.QtCore import QTimer

from src.core import (APP_VERSION, config, config_file_path, logger, 
                      init_sentry_if_enabled, exception_handler)
from src.notion import check_notion_status_once, run_startup_sync
from src.server import start_server_blocking, TRAY_ICON_ICO
from src.ui_styles import DARK_STYLESHEET
from src.ui_dialogs import InitialSetupDialog
from src.ui_main import NotionLinkTrayApp


def main():
    required_files = [
        TRAY_ICON_ICO,
        os.path.join("src", "__init__.py"),
        os.path.join("src", "core.py"),
        os.path.join("src", "ui_styles.py"),
        os.path.join("src", "notion.py"),
        os.path.join("src", "server.py"),
        os.path.join("src", "ui_dialogs.py"),
        os.path.join("src", "ui_main.py"),
    ]
    
    missing_files = [f for f in required_files if not os.path.exists(f)]
    
    if missing_files:
        # Show error dialog
        temp_app = QApplication(sys.argv)
        error_box = QMessageBox()
        error_box.setIcon(QMessageBox.Critical)
        error_box.setWindowTitle("NotionLink - Installation Error")
        error_box.setText("Required files are missing!")
        
        missing_list = "\n".join(f"  • {f}" for f in missing_files)
        error_box.setInformativeText(
            f"NotionLink cannot start because required installation files are missing.\n\n"
            f"Missing files:\n{missing_list}\n\n"
            f"Please ensure all files from the NotionLink package are present in the installation directory.\n\n"
            f"Installation directory: {os.path.dirname(os.path.abspath(__file__))}"
        )
        error_box.setStandardButtons(QMessageBox.Ok)
        error_box.exec()
        sys.exit(1)
    
    print("Sentry initialization deferred (background).")
    

    sys.excepthook = exception_handler
    
    if sys.platform.startswith('win'):
        try:
            appid = f"com.notionlink.app.{APP_VERSION}"
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(appid)
            print(f"Set Windows AppUserModelID: {appid}")
        except Exception as e:
            print(f"Failed to set AppUserModelID: {e}")

    QApplication.setStyle("Fusion")
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(TRAY_ICON_ICO))
    app.setQuitOnLastWindowClosed(False)
    
    app.tray_app = NotionLinkTrayApp(app)
    
    # Onboarding wizard
    if not config.get("tutorial_completed", False):
        print("First run detected. Starting setup wizard...")
        
        alpha_msg_box = QMessageBox()
        alpha_msg_box.setWindowIcon(QIcon(TRAY_ICON_ICO))
        alpha_msg_box.setStyleSheet(DARK_STYLESHEET.replace("QMenu", "QMessageBox").replace("QDialog", "QMessageBox"))
        alpha_msg_box.setIcon(QMessageBox.Information)
        alpha_msg_box.setWindowTitle(f"NotionLink v{APP_VERSION}")
        alpha_msg_box.setText(f"Welcome to NotionLink v{APP_VERSION}!")
        alpha_msg_box.setInformativeText(
            "This is the latest release.\n\n"
            "You can enable anonymous error reports in the setup wizard to help improve NotionLink. "
            "These reports do NOT include your Notion Token, file paths, or any personal data.\n\n"
            "Thank you for using NotionLink!"
        )
        alpha_msg_box.setStandardButtons(QMessageBox.Ok)
        alpha_msg_box.exec()
        
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
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
