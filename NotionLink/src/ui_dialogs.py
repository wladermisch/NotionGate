# NotionLink - UI Dialogs Module
# Copyright (c) 2025 wladermisch. All Rights Reserved.
#
# All dialog windows and helper widgets

import os
import json
import threading
import webbrowser
import traceback
from PySide6.QtWidgets import (
    QDialog, QLabel, QLineEdit, QPushButton, QVBoxLayout, QHBoxLayout,
    QListWidget, QListWidgetItem, QCheckBox, QTextEdit, QFileDialog,
    QMessageBox, QDialogButtonBox, QScrollArea, QWidget, QFrame
)
from PySide6.QtGui import QIcon, QCursor
from PySide6.QtCore import Signal, Qt, QObject, QTimer, QThread, QSize
import pyperclip as clip
from notion_client import Client

from .core import config, config_file_path, logger, sentry_sdk, link_cache, notionlog_path, resource_path
from .notion import get_notion_title, extract_id_and_title_from_link
from .server import manage_autostart, TRAY_ICON_ICO
from .ui_styles import DARK_STYLESHEET


# =============================================================================
# BASE DIALOG
# =============================================================================

class BaseDialog(QDialog):
    # Base class for all dialog windows with consistent styling.
    
    def __init__(self, title, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setWindowIcon(QIcon(TRAY_ICON_ICO))
        self.setStyleSheet(DARK_STYLESHEET)
        self.setWindowFlags(Qt.Dialog | Qt.WindowCloseButtonHint)
        self.setAttribute(Qt.WA_QuitOnClose, False)


# =============================================================================
# SETUP & TOKEN MANAGEMENT
# =============================================================================

class InitialSetupDialog(BaseDialog):
    # First-time setup wizard for configuring Notion token.
    
    def __init__(self, tray_app_instance):
        super().__init__("Welcome to NotionLink!")
        self.tray_app = tray_app_instance
        self.setFixedSize(600, 480)
        layout = QVBoxLayout(self)
        
        title = QLabel("Welcome to NotionLink!")
        title.setStyleSheet("font-size: 20px; font-weight: bold;")
        layout.addWidget(title, alignment=Qt.AlignCenter)
        layout.addWidget(QLabel("This one-time setup will connect the app to your Notion account.\nIt only takes 3 steps:"), alignment=Qt.AlignLeft)
        layout.addWidget(QLabel("1. Go to the Notion Integrations page by clicking the button below."), alignment=Qt.AlignLeft)
        link_button = QPushButton("Open Notion Integrations Page")
        link_button.clicked.connect(lambda: webbrowser.open_new("https://www.notion.so/my-integrations"))
        layout.addWidget(link_button)
        layout.addWidget(QLabel("2. Click 'New integration', give it a name (e.g., 'NotionLink'),\nand copy the 'Internal Integration Token' (Secret)."), alignment=Qt.AlignLeft)
        runtime_note = QLabel(
            "Important: NotionLink must be running to open files from the generated local links. "
            "If the app is not running, those links will not resolve."
            "If demand is high enough, offline link resolution may be added in a future update."
        )
        runtime_note.setWordWrap(True)
        runtime_note.setStyleSheet("color: #ffcc66; font-size: 9pt; margin-top: 6px;")
        layout.addWidget(runtime_note, alignment=Qt.AlignLeft)
        layout.addWidget(QLabel("3. Paste your 'Internal Integration Token' (Secret) here:"), alignment=Qt.AlignLeft)
        
        self.token_entry = QLineEdit(self)
        self.token_entry.setEchoMode(QLineEdit.Password)
        layout.addWidget(self.token_entry)
        
        layout.addSpacing(15)
        self.startup_checkbox = QCheckBox("Start NotionLink automatically with Windows")
        self.startup_checkbox.setChecked(True)
        layout.addWidget(self.startup_checkbox, alignment=Qt.AlignLeft)
        layout.addSpacing(8)
        
        self.sentry_checkbox = QCheckBox("Anonymous error reporting (helps improve NotionLink)\nNo personal data is shared; performance is unaffected.")
        self.sentry_checkbox.setChecked(True)
        layout.addWidget(self.sentry_checkbox, alignment=Qt.AlignLeft)
        layout.addSpacing(15)
        
        self.error_label = QLabel("")
        layout.addWidget(self.error_label)
        
        self.save_button = QPushButton("Save and Start Application")
        self.save_button.clicked.connect(self.save_and_exit)
        layout.addWidget(self.save_button)
        self.setLayout(layout)
        
    def save_and_exit(self):
        global config
        token = self.token_entry.text()
        if not token or "PLEASE_ENTER" in token or len(token) < 50:
            self.error_label.setText("That doesn't look like a valid token. Please paste the full secret token.")
            return
        
        config["notion_token"] = token
        config["tutorial_completed"] = True
        
        autostart_enabled = self.startup_checkbox.isChecked()
        config["autostart_with_windows"] = autostart_enabled
        
        sentry_enabled = self.sentry_checkbox.isChecked()
        config["sentry_enabled"] = sentry_enabled

        try:
            with open(config_file_path, "w") as f:
                json.dump(config, f, indent=4)
            print("Initial setup complete. Token saved.")
            
            if self.tray_app:
                self.tray_app.sync_autostart_ui(autostart_enabled)
                self.tray_app.sync_sentry_ui(sentry_enabled)
            
            manage_autostart(autostart_enabled)
            
            self.accept()
        except Exception as e:
            print(f"Error saving config or creating shortcut: {e}")
            self.error_label.setText(f"Error saving config: {e}")


class ManageTokenWindow(BaseDialog):
    # Dialog for updating Notion token.
    
    def __init__(self, tray_app_instance):
        super().__init__("Manage Notion Token")
        self.tray_app = tray_app_instance
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Notion Token:"))
        self.token_entry = QLineEdit(self)
        self.token_entry.setEchoMode(QLineEdit.Password)
        self.token_entry.setText(config.get("notion_token", ""))
        layout.addWidget(self.token_entry)
        save_button = QPushButton("Save")
        save_button.clicked.connect(self.save_and_close)
        layout.addWidget(save_button)
        self.setLayout(layout)
    
    def save_and_close(self):
        global config
        config["notion_token"] = self.token_entry.text()
        with open(config_file_path, "w") as config_file:
            json.dump(config, config_file, indent=4)
        print("Token updated.")
        self.accept()
        if self.tray_app:
            self.tray_app.run_status_check_thread()


# =============================================================================
# HELPER WIDGETS
# =============================================================================

class TitleFetcher(QThread):
    # Background thread for fetching Notion page/database titles.
    title_fetched = Signal(str)
    
    def __init__(self, notion_id, token, is_db):
        super().__init__()
        self.notion_id = notion_id
        self.token = token
        self.is_db = is_db
        
    def run(self):
        title = get_notion_title(self.notion_id, self.token, self.is_db)
        if title:
            self.title_fetched.emit(title)


class LogWatcher(QObject):
    # Real-time log file monitor for dashboard display.
    new_log_line = Signal(str)
    
    def __init__(self, log_path):
        super().__init__()
        self.log_path = log_path
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.check_log)
        self.timer.start(1000)
        
        try:
            self.file = open(self.log_path, 'r', encoding='utf-8')
            self.file.seek(0, os.SEEK_END)
            print("Log watcher initialized.")
        except Exception as e:
            print(f"Error opening log file for watcher: {e}")
            self.file = None

    def check_log(self):
        if not self.file:
            try:
                self.file = open(self.log_path, 'r', encoding='utf-8')
                self.file.seek(0, os.SEEK_END)
            except Exception:
                return
                
        line = self.file.readline()
        while line:
            self.new_log_line.emit(line.strip())
            line = self.file.readline()


# =============================================================================
# MAPPING MANAGEMENT
# =============================================================================

class EditMappingDialog(BaseDialog):
    # Dialog for creating/editing page or database mappings.
    
    def __init__(self, tray_app_instance, existing_mapping=None, mapping_type="page"):
        self.mapping_type_name = "Page" if mapping_type == "page" else "Database"
        title = f"Edit {self.mapping_type_name} Mapping" if existing_mapping else f"Add New {self.mapping_type_name} Mapping"
        super().__init__(title)
        
        self.tray_app = tray_app_instance
        self.mapping = existing_mapping if existing_mapping else {}
        self.mapping_type = mapping_type
        self.setMinimumWidth(600)
        self.title_fetcher = None

        layout = QVBoxLayout(self)
        
        layout.addWidget(QLabel(f"Notion {self.mapping_type_name} Link or ID:"))
        self.notion_id_entry = QLineEdit()
        self.notion_id_entry.setText(self.mapping.get("notion_id", ""))
        self.notion_id_entry.textChanged.connect(self.parse_link_entry)
        layout.addWidget(self.notion_id_entry)

        layout.addWidget(QLabel("Mapping Title (Custom Name):"))
        self.notion_title_entry = QLineEdit()
        self.notion_title_entry.setText(self.mapping.get("notion_title", ""))
        self.notion_title_entry.setPlaceholderText("Fetching title from Notion...")
        layout.addWidget(self.notion_title_entry)

        if mapping_type == "database":
            info_label = QLabel(
                "<b>Database Properties:</b> NotionLink will automatically create required properties if missing:\n"
                "  - <b>Name</b> (Title) - For file names\n"
                "  - <b>Link</b> (URL) - For file links\n"
                "  - <b>Created</b> (Date) - Creation timestamp\n"
                "  - <b>Modified</b> (Date) - Last modified timestamp\n"
                "  - <b>Size (Bytes)</b> (Number) - File size"
            )
            info_label.setWordWrap(True)
            info_label.setStyleSheet("color: #17a2b8; border: 1px solid #17a2b8; padding: 5px; border-radius: 4px;")
            layout.addWidget(info_label)

        layout.addWidget(QLabel("Synced Folders:"))
        self.folders_list = QListWidget()
        for folder in self.mapping.get("folders", []):
            self.folders_list.addItem(folder)
        layout.addWidget(self.folders_list)
        
        folder_btn_layout = QHBoxLayout()
        add_folder_btn = QPushButton("Add Folder")
        add_folder_btn.clicked.connect(self.add_folder)
        remove_folder_btn = QPushButton("Remove Selected Folder")
        remove_folder_btn.setObjectName("secondaryButton")
        remove_folder_btn.clicked.connect(self.remove_folder)
        folder_btn_layout.addWidget(add_folder_btn)
        folder_btn_layout.addWidget(remove_folder_btn)
        layout.addLayout(folder_btn_layout)

        layout.addWidget(QLabel("Ignore files with extensions (comma-separated, e.g. *.tmp, *.log):"))
        self.ignore_ext_entry = QLineEdit()
        self.ignore_ext_entry.setText(", ".join(self.mapping.get("ignore_extensions", ["*.tmp", ".*", "desktop.ini"])))
        layout.addWidget(self.ignore_ext_entry)

        layout.addWidget(QLabel("Ignore specific filenames (comma-separated, wildcards like * ok):"))
        self.ignore_files_entry = QLineEdit()
        self.ignore_files_entry.setText(", ".join(self.mapping.get("ignore_files", [])))
        
        files_btn_layout = QHBoxLayout()
        files_btn_layout.addWidget(self.ignore_files_entry)
        add_files_btn = QPushButton("Add Files...")
        add_files_btn.setObjectName("secondaryButton")
        add_files_btn.clicked.connect(self.add_files_to_ignore)
        files_btn_layout.addWidget(add_files_btn)
        layout.addLayout(files_btn_layout)

        self.full_lifecycle_checkbox = QCheckBox("Full Lifecycle Sync (sync file deletions and renames to Notion)")
        self.full_lifecycle_checkbox.setChecked(self.mapping.get("full_lifecycle_sync", True))
        self.full_lifecycle_checkbox.setToolTip(
            "When enabled, files deleted or renamed in your folders will be automatically updated in Notion.\n"
            "When disabled, only new files and modifications are synced."
        )
        layout.addWidget(self.full_lifecycle_checkbox)

        self.error_label = QLabel("")
        layout.addWidget(self.error_label)

        button_box = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        button_box.accepted.connect(self.save_and_accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)

        self.parse_link_entry(self.notion_id_entry.text())

    def parse_link_entry(self, text):
        id_tuple = extract_id_and_title_from_link(text)
        if id_tuple:
            notion_id, title_from_url = id_tuple
            if self.notion_title_entry.text() == "" or "..." in self.notion_title_entry.text():
                self.notion_title_entry.setText(title_from_url or "Fetching title...")
            
            if self.title_fetcher and self.title_fetcher.isRunning():
                self.title_fetcher.terminate()
                
            self.title_fetcher = TitleFetcher(notion_id, config.get("notion_token"), self.mapping_type == "database")
            self.title_fetcher.title_fetched.connect(self.notion_title_entry.setText)
            self.title_fetcher.start()
        else:
            self.notion_title_entry.setPlaceholderText("Enter valid Notion link or ID above")

    def add_folder(self):
        folder_path = QFileDialog.getExistingDirectory(self, "Select a folder to sync")
        if folder_path:
            self.folders_list.addItem(folder_path)

    def remove_folder(self):
        selected_items = self.folders_list.selectedItems()
        if not selected_items:
            return
        for item in selected_items:
            self.folders_list.takeItem(self.folders_list.row(item))
            
    def add_files_to_ignore(self):
        files, _ = QFileDialog.getOpenFileNames(self, "Select files to ignore")
        if files:
            current_ignored = [p.strip() for p in self.ignore_files_entry.text().split(",") if p.strip()]
            new_files_added = 0
            for f in files:
                filename = os.path.basename(f)
                if filename not in current_ignored:
                    current_ignored.append(filename)
                    new_files_added += 1
            
            self.ignore_files_entry.setText(", ".join(current_ignored))
            print(f"Added {new_files_added} new filenames to ignore list.")

    def get_mapping_data(self):
        notion_id_or_link = self.notion_id_entry.text().strip()
        id_tuple = extract_id_and_title_from_link(notion_id_or_link)
        
        if not id_tuple:
            self.error_label.setText(f"Invalid Notion {self.mapping_type_name} Link or ID.")
            return None
        
        notion_id, title_from_url = id_tuple
        final_title = self.notion_title_entry.text().strip()
        if not final_title or final_title == "Fetching title...":
            final_title = title_from_url or f"Untitled (...{notion_id[-6:]})"

        folders = []
        for i in range(self.folders_list.count()):
            folders.append(self.folders_list.item(i).text())
        
        if not folders:
            self.error_label.setText("You must add at least one folder to sync.")
            return None
            
        ignore_exts = [p.strip() for p in self.ignore_ext_entry.text().split(",") if p.strip()]
        ignore_files = [p.strip() for p in self.ignore_files_entry.text().split(",") if p.strip()]
            
        return {
            "notion_title": final_title,
            "notion_id": notion_id,
            "folders": folders,
            "ignore_extensions": ignore_exts,
            "ignore_files": ignore_files,
            "full_lifecycle_sync": self.full_lifecycle_checkbox.isChecked()
        }

    def save_and_accept(self):
        self.mapping = self.get_mapping_data()
        if self.mapping:
            if self.mapping_type == "database":
                if not self.ensure_database_properties(self.mapping["notion_id"]):
                    return
            self.accept()
    
    def ensure_database_properties(self, database_id):
        # check and auto-create required database properties
        try:
            notion = Client(auth=config.get("notion_token"))
            db_response = notion.databases.retrieve(database_id=database_id)
            properties = db_response.get("properties", {})
            
            required_props = {
                "Name": "title",
                "Link": "url",
                "Created": "date",
                "Modified": "date",
                "Size (Bytes)": "number"
            }
            
            missing_props = {}
            
            for prop_name, prop_type in required_props.items():
                if prop_name not in properties:
                    missing_props[prop_name] = prop_type
                elif prop_name == "Name" and properties["Name"].get("type") != "title":
                    missing_props[prop_name] = prop_type
            
            if not missing_props:
                return True
            
            missing_list = "\n".join([f"  - <b>{name}</b> ({ptype.capitalize()})" for name, ptype in missing_props.items()])
            
            msg_box = QMessageBox(self)
            msg_box.setIcon(QMessageBox.Information)
            msg_box.setWindowTitle("Auto-Configure Database")
            msg_box.setText("This database is missing required properties for NotionLink.")
            msg_box.setInformativeText(
                f"The following properties will be automatically created:\n{missing_list}\n\n"
                "Would you like to continue?"
            )
            msg_box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            msg_box.setDefaultButton(QMessageBox.Yes)
            
            if msg_box.exec() != QMessageBox.Yes:
                self.error_label.setText("Database setup cancelled. Please add required properties manually.")
                return False
            
            new_properties = {}
            for prop_name, prop_type in missing_props.items():
                if prop_type == "title":
                    new_properties[prop_name] = {"title": {}}
                elif prop_type == "url":
                    new_properties[prop_name] = {"url": {}}
                elif prop_type == "date":
                    new_properties[prop_name] = {"date": {}}
                elif prop_type == "number":
                    new_properties[prop_name] = {"number": {"format": "number"}}
            
            notion.databases.update(
                database_id=database_id,
                properties=new_properties
            )
            
            success_msg = QMessageBox(self)
            success_msg.setIcon(QMessageBox.Information)
            success_msg.setWindowTitle("Success")
            success_msg.setText("Database properties created successfully!")
            success_msg.setInformativeText(
                f"Added {len(missing_props)} properties to your database:\n{missing_list}"
            )
            success_msg.exec()
            
            return True
            
        except Exception as e:
            error_msg = QMessageBox(self)
            error_msg.setIcon(QMessageBox.Critical)
            error_msg.setWindowTitle("Error")
            error_msg.setText("Failed to configure database properties.")
            error_msg.setInformativeText(f"Error: {str(e)}\n\nPlease add the required properties manually.")
            error_msg.exec()
            self.error_label.setText(f"Error: {str(e)}")
            return False


class ManageMappingsListDialog(BaseDialog):
    # Dialog for managing list of page/database mappings.
    
    def __init__(self, tray_app_instance, mapping_type="page"):
        self.mapping_type = mapping_type
        self.mapping_key = f"{mapping_type}_mappings"
        self.mapping_type_name = "Page" if mapping_type == "page" else "Database"
        super().__init__(f"Manage {self.mapping_type_name} Mappings")

        self.tray_app = tray_app_instance
        self.setMinimumSize(600, 400)
        
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"Sync configurations for {self.mapping_type_name}s:"))
        
        self.list_widget = QListWidget()
        layout.addWidget(self.list_widget)
        
        btn_layout = QHBoxLayout()
        add_btn = QPushButton(f"Add New {self.mapping_type_name} Mapping")
        add_btn.clicked.connect(self.add_mapping)
        edit_btn = QPushButton("Edit Selected")
        edit_btn.setObjectName("secondaryButton")
        edit_btn.clicked.connect(self.edit_mapping)
        remove_btn = QPushButton("Remove Selected")
        remove_btn.setObjectName("dangerButton")
        remove_btn.clicked.connect(self.remove_mapping)
        
        btn_layout.addWidget(add_btn)
        btn_layout.addWidget(edit_btn)
        btn_layout.addWidget(remove_btn)
        layout.addLayout(btn_layout)
        
        self.status_label = QLabel("Mappings saved. Watchers restarted.")
        self.status_label.setStyleSheet("color: green;")
        self.status_label.setVisible(False)
        layout.addWidget(self.status_label)
        
        close_btn_box = QDialogButtonBox(QDialogButtonBox.Close)
        close_btn_box.rejected.connect(self.reject)
        layout.addWidget(close_btn_box)
        
        self.load_mappings_into_list()
        
    def load_mappings_into_list(self):
        self.list_widget.clear()
        mappings = config.get(self.mapping_key, [])
        for i, mapping in enumerate(mappings):
            title = mapping.get("notion_title", f"Untitled Mapping {i}")
            folders_count = len(mapping.get("folders", []))
            item = QListWidgetItem(f"{title} ({folders_count} folder{'s' if folders_count != 1 else ''})")
            item.setData(Qt.UserRole, i)
            self.list_widget.addItem(item)
            
    def add_mapping(self):
        dialog = EditMappingDialog(self.tray_app, mapping_type=self.mapping_type)
        if dialog.exec() == QDialog.Accepted:
            new_mapping_data = dialog.get_mapping_data()
            if new_mapping_data:
                config[self.mapping_key].append(new_mapping_data)
                self.save_and_restart_watchers(new_mapping_data)
                
    def edit_mapping(self):
        selected_items = self.list_widget.selectedItems()
        if not selected_items:
            return
            
        item = selected_items[0]
        index = item.data(Qt.UserRole)
        existing_mapping = config[self.mapping_key][index]
        
        dialog = EditMappingDialog(self.tray_app, existing_mapping=existing_mapping, mapping_type=self.mapping_type)
        if dialog.exec() == QDialog.Accepted:
            updated_mapping_data = dialog.get_mapping_data()
            if updated_mapping_data:
                config[self.mapping_key][index] = updated_mapping_data
                self.save_and_restart_watchers(updated_mapping_data)

    def remove_mapping(self):
        selected_items = self.list_widget.selectedItems()
        if not selected_items:
            return
            
        reply = QMessageBox.warning(self, "Confirm Delete",
                                    "Are you sure you want to remove this mapping?",
                                    QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
        if reply == QMessageBox.No:
            return
            
        item = selected_items[0]
        index = item.data(Qt.UserRole)
        del config[self.mapping_key][index]
        self.save_and_restart_watchers()
        
    def save_and_restart_watchers(self, new_mapping_data=None):
        global config, link_cache
        with open(config_file_path, "w") as config_file:
            json.dump(config, config_file, indent=4)
        print(f"{self.mapping_key} updated.")
        
        link_cache.clear()
        
        self.load_mappings_into_list()
        self.tray_app.restart_file_observer()
        self.status_label.setVisible(True)

        if new_mapping_data:
            for folder_path in new_mapping_data.get("folders", []):
                print(f"Starting initial backfill for new folder: {folder_path}")
                threading.Thread(target=self.tray_app.upload_folder_to_notion,
                                 args=(folder_path, new_mapping_data, self.mapping_type),
                                 daemon=True).start()


# =============================================================================
# UTILITY DIALOGS
# =============================================================================

class ManualUploadWindow(BaseDialog):
    # Dialog for manually triggering folder uploads.
    
    def __init__(self, tray_app_instance):
        super().__init__("Start Manual Upload")
        self.tray_app = tray_app_instance
        self.setGeometry(300, 300, 600, 300)
        self.selected_task = None
        
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Select a mapping to backfill (upload all existing files):"))
        
        all_mappings = [("page", pm) for pm in config.get("page_mappings", [])] + \
                       [("database", dbm) for dbm in config.get("database_mappings", [])]
                       
        if not all_mappings:
            layout.addWidget(QLabel("No mappings have been configured.", alignment=Qt.AlignCenter))
            return
        
        scroll_area = QScrollArea(self)
        scroll_area.setWidgetResizable(True)
        layout.addWidget(scroll_area)
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)
        scroll_layout.setAlignment(Qt.AlignTop)
        scroll_area.setWidget(scroll_content)
        
        for mapping_type, mapping in all_mappings:
            title = mapping["notion_title"]
            type_label = "Page" if mapping_type == "page" else "Database"
            for folder_path in mapping.get("folders", []):
                folder_name = os.path.basename(folder_path)
                button_text = f"[{type_label}] {title} - {folder_name}"
                button = QPushButton(button_text)
                button.clicked.connect(lambda checked=False, p=folder_path, m=mapping, t=mapping_type: self.start_upload(p, m, t))
                scroll_layout.addWidget(button)
                
    def start_upload(self, folder_path, mapping_config, mapping_type):
        print(f"User selected folder for upload: {folder_path}")
        self.selected_task = (folder_path, mapping_config, mapping_type)
        self.accept()


class ConvertPathWindow(BaseDialog):
    # Dialog for converting file paths to server links.
    
    def __init__(self, tray_app_instance):
        super().__init__("Convert Path")
        self.tray_app = tray_app_instance
        
        layout = QVBoxLayout(self)
        
        layout.addWidget(QLabel("Paste path or select file:"))
        
        input_layout = QHBoxLayout()
        self.entry = QLineEdit(self)
        self.entry.setPlaceholderText(clip.paste().replace("\"", ""))
        self.entry.returnPressed.connect(self.convert_path)
        input_layout.addWidget(self.entry)
        
        browse_button = QPushButton("Browse File...")
        browse_button.setObjectName("secondaryButton")
        browse_button.clicked.connect(self.browse_file)
        input_layout.addWidget(browse_button)
        layout.addLayout(input_layout)
        
        save_button = QPushButton("Convert (Copies to Clipboard)")
        save_button.clicked.connect(self.convert_path)
        layout.addWidget(save_button)
        
        layout.addWidget(QLabel("Generated Link:"))
        self.output_label = QLineEdit(self)
        self.output_label.setReadOnly(True)
        layout.addWidget(self.output_label)

    def browse_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Select a file to convert")
        if file_path:
            self.entry.setText(file_path)
            self.convert_path()

    def convert_path(self):
        path_to_convert = self.entry.text().strip().replace("\"", "")
        
        if not path_to_convert:
            path_to_convert = self.entry.placeholderText()

        if not path_to_convert:
            self.output_label.setText("Error: No path entered or found in clipboard.")
            return

        port = config.get("server_port")
        server_host = config.get("server_host")
        server_address = f"{server_host}:{port}/"
        
        url_path = path_to_convert.replace("\\", "/")
        if url_path.startswith('/'):
            url_path = url_path[1:]
            
        resulting_path = server_address + url_path
        
        self.output_label.setText(resulting_path)
        clip.copy(resulting_path)
        self.tray_app.show_notification("Path Converted", "Link copied to clipboard.")


class FeedbackDialog(BaseDialog):
    # Dialog for sending feedback via Sentry.
    
    def __init__(self, tray_app_instance):
        super().__init__("Send Feedback")
        self.tray_app = tray_app_instance
        self.setMinimumWidth(400)

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Found a bug or have a suggestion? Let us know!"))

        self.feedback_text = QTextEdit(self)
        self.feedback_text.setPlaceholderText("Please describe the issue or feedback...")
        self.feedback_text.setMinimumHeight(100)
        layout.addWidget(self.feedback_text)

        layout.addWidget(QLabel("Your Discord Name (Optional):"))
        self.discord_name_entry = QLineEdit(self)
        self.discord_name_entry.setPlaceholderText("e.g., username#1234 (for updates on your report)")
        layout.addWidget(self.discord_name_entry)

        self.status_label = QLabel("")
        layout.addWidget(self.status_label)

        button_box = QDialogButtonBox()
        self.send_button = button_box.addButton("Send Feedback", QDialogButtonBox.AcceptRole)
        self.send_button.clicked.connect(self.send_feedback)
        button_box.addButton(QDialogButtonBox.Cancel).clicked.connect(self.reject)
        layout.addWidget(button_box)

    def send_feedback(self):
        feedback = self.feedback_text.toPlainText().strip()
        discord_name = self.discord_name_entry.text().strip()

        if not feedback:
            self.status_label.setText("Please enter some feedback before sending.")
            return

        try:
            if sentry_sdk is not None:
                with sentry_sdk.isolation_scope() as scope:
                    if discord_name:
                        scope.set_user({"username": discord_name})
                    
                    sentry_sdk.capture_message(feedback, level="info")
                
                print(f"Feedback sent to Sentry. Discord: {discord_name}")
                self.status_label.setText("Feedback successfully sent. Thank you!")
                self.status_label.setStyleSheet("color: green;")
                self.send_button.setEnabled(False)
                self.feedback_text.setEnabled(False)
                self.discord_name_entry.setEnabled(False)
                QTimer.singleShot(2000, self.accept)
            else:
                raise Exception("Sentry SDK not initialized.")
                
        except Exception as e:
            print(f"Error sending Sentry feedback: {e}")
            traceback.print_exc()
            self.status_label.setText("Could not send feedback. Check logs.")
