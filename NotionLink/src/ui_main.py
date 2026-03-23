# Main UI Components - Dashboard Window and System Tray Application
import sys
import os
import json
import time
import fnmatch
import threading
import re
from PySide6.QtWidgets import (QApplication, QMainWindow, QSystemTrayIcon, QMenu,
                                QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
                                QTextEdit, QCheckBox, QWidget, QDialog, QMessageBox,
                                QLineEdit, QFrame, QScrollArea, QSizePolicy, QToolButton,
                                QFormLayout, QGraphicsOpacityEffect, QListWidget,
                                QFileDialog)
from PySide6.QtCore import Qt, QTimer, Signal, QObject, QPropertyAnimation, QEasingCurve
from PySide6.QtGui import QIcon, QAction, QPainter, QColor, QPixmap
import webbrowser
from notion_client import Client
from watchdog.observers import Observer

from .core import (APP_VERSION, config, config_file_path, logger, 
                   observer, httpd, notification_batch, notified_errors, 
                   is_user_error, sentry_sdk)
from .notion import (get_existing_links, link_cache, sync_file_to_notion, 
                     check_notion_status_once, run_startup_sync, process_pending_uploads,
                     extract_id_and_title_from_link)
from .server import (NotionFileHandler, start_server_blocking, 
                     manage_autostart, TRAY_ICON_ICO)
from .ui_styles import DARK_STYLESHEET
from .ui_dialogs import (InitialSetupDialog, ManageTokenWindow, 
                         EditMappingDialog, ManageMappingsListDialog, 
                         ManualUploadWindow, ConvertPathWindow, FeedbackDialog, 
                         LogWatcher, UpdateAvailableDialog, UpdateCheckThread,
                         TitleFetcher)


class MainDashboardWindow(QMainWindow):
    # Main application dashboard window
    
    def __init__(self, tray_app):
        super().__init__()
        self.tray_app = tray_app
        self.setWindowTitle(f"NotionLink {APP_VERSION} - Dashboard")
        self.setWindowIcon(QIcon(TRAY_ICON_ICO))
        self.setMinimumSize(900, 600)
        self.setStyleSheet(DARK_STYLESHEET)
        self.log_watcher = None
        self.page_history = []
        self.current_page = "dashboard"
        self.transient_status_active = False
        self._status_busy_timer = None
        self._status_busy_frames = ["⟳", "⟲"]
        self._status_busy_index = 0
        self._active_mapping_buttons = {}
        self._last_animation = None
        self._current_opacity_effect = None
        self.mapping_editor_state = None
        
        # Initialize UI
        self.init_ui()
        
        # Start log watcher in a separate thread/timer to avoid blocking UI init
        QTimer.singleShot(100, self.start_log_watcher)
        
        # Connect to tray app signals for status updates
        if self.tray_app:
            self.tray_app.status_updated.connect(self.update_token_status)
    
    def start_log_watcher(self):
        try:
            # Start log watcher (auto-starts when initialized)
            self.log_watcher = LogWatcher(logger.handlers[0].baseFilename)
            self.log_watcher.new_log_line.connect(self.append_log_line)
        except Exception as e:
            print(f"Failed to start log watcher: {e}")
            self.tray_app.server_error_signal.connect(self.update_status_panel_error)
            self.tray_app.user_error_signal.connect(self.update_status_panel_warning)
            self.tray_app.op_success_signal.connect(self.reset_status_panel)

    def init_ui(self):
        # Main container
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(15, 15, 15, 15)
        main_layout.setSpacing(15)
        
        # Left column - Actions & Settings (narrower)
        left_column = self._init_left_column()
        
        # Right column - Status & Pages (wider)
        right_column = self._init_right_column()
        
        # Add columns to main layout (30% left, 70% right)
        main_layout.addLayout(left_column, stretch=30)
        main_layout.addLayout(right_column, stretch=70)

    def _init_left_column(self):
        left_column = QVBoxLayout()
        left_column.setSpacing(10)
        
        # Actions Section
        actions_label = QLabel("Quick Actions")
        actions_label.setStyleSheet("font-size: 14pt; font-weight: bold; margin-bottom: 5px;")
        left_column.addWidget(actions_label)
        
        convert_btn = QPushButton("Convert Path to Link")
        convert_btn.clicked.connect(self.tray_app.show_convert_path)
        left_column.addWidget(convert_btn)
        
        left_column.addSpacing(20)
        
        # Management Section
        mgmt_label = QLabel("Management")
        mgmt_label.setStyleSheet("font-size: 14pt; font-weight: bold; margin-bottom: 5px;")
        left_column.addWidget(mgmt_label)
        
        page_btn = QPushButton("Page Mappings")
        page_btn.clicked.connect(self.tray_app.show_page_mappings)
        left_column.addWidget(page_btn)
        
        db_btn = QPushButton("Database Mappings")
        db_btn.clicked.connect(self.tray_app.show_database_mappings)
        left_column.addWidget(db_btn)
        
        token_btn = QPushButton("Notion Token")
        token_btn.clicked.connect(self.tray_app.show_token)
        left_column.addWidget(token_btn)
        
        left_column.addSpacing(20)
        
        # Settings Section
        settings_label = QLabel("Settings")
        settings_label.setStyleSheet("font-size: 14pt; font-weight: bold; margin-bottom: 5px;")
        left_column.addWidget(settings_label)
        
        self.autostart_checkbox = QCheckBox("Start with Windows")
        self.autostart_checkbox.setChecked(config.get("autostart_with_windows", False))
        self.autostart_checkbox.toggled.connect(self.tray_app.toggle_autostart)
        left_column.addWidget(self.autostart_checkbox)
        
        self.sentry_checkbox = QCheckBox("Enable Error Reports")
        self.sentry_checkbox.setChecked(config.get("sentry_enabled", False))
        self.sentry_checkbox.toggled.connect(self.tray_app.toggle_sentry)
        left_column.addWidget(self.sentry_checkbox)
        
        feedback_btn = QPushButton("Send Feedback")
        feedback_btn.clicked.connect(self.tray_app.show_feedback_dialog)
        left_column.addWidget(feedback_btn)

        # Small Help button for quick access to runtime instructions and wiki
        help_btn = QPushButton("Help")
        help_btn.setToolTip("Help & documentation")
        help_btn.clicked.connect(self.show_help)
        left_column.addWidget(help_btn)

        left_column.addSpacing(12)

        minimize_btn = QPushButton("Minimize to Tray")
        minimize_btn.setObjectName("secondaryButton")
        minimize_btn.clicked.connect(self.tray_app.minimize_to_tray)
        left_column.addWidget(minimize_btn)
        
        left_column.addStretch()
        
        quit_btn = QPushButton("Close NotionLink")
        quit_btn.setStyleSheet("background-color: #8B0000; font-weight: bold;")
        quit_btn.clicked.connect(self.tray_app.quit_app)
        left_column.addWidget(quit_btn)
        
        return left_column

    def _init_right_column(self):
        right_column = QVBoxLayout()
        right_column.setSpacing(8)

        nav_row = QHBoxLayout()
        self.back_btn = QToolButton()
        self.back_btn.setText("←")
        self.back_btn.setToolTip("Go back")
        self.back_btn.setVisible(False)
        self.back_btn.clicked.connect(self.go_back)
        nav_row.addWidget(self.back_btn)

        self.breadcrumb_label = QLabel("Dashboard")
        self.breadcrumb_label.setTextFormat(Qt.RichText)
        self.breadcrumb_label.setTextInteractionFlags(Qt.TextBrowserInteraction)
        self.breadcrumb_label.setOpenExternalLinks(False)
        self.breadcrumb_label.linkActivated.connect(self._on_breadcrumb_clicked)
        self.breadcrumb_label.setStyleSheet("font-size: 14pt; font-weight: bold;")
        nav_row.addWidget(self.breadcrumb_label, stretch=1)
        right_column.addLayout(nav_row)

        status_header = QLabel("System Status")
        status_header.setStyleSheet("font-size: 12pt; font-weight: bold;")
        right_column.addWidget(status_header)

        status_row = QHBoxLayout()
        status_row.setSpacing(8)
        self.status_panel = QLabel("NotionLink is running...")
        self.status_panel.setStyleSheet(self._get_status_style("#1e3a1e", "#66ff66", "#2e5a2e"))
        self.status_panel.setWordWrap(True)
        self.status_panel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Minimum)
        self.status_panel.setMinimumHeight(72)
        status_row.addWidget(self.status_panel, stretch=1)

        self.ack_btn = QToolButton()
        self.ack_btn.setText("✔")
        self.ack_btn.setToolTip("Acknowledge current status message")
        self.ack_btn.setVisible(False)
        self.ack_btn.clicked.connect(self.acknowledge_status)
        self.ack_btn.setFixedWidth(32)
        status_row.addWidget(self.ack_btn, alignment=Qt.AlignTop)
        right_column.addLayout(status_row)

        token_status_layout = QHBoxLayout()
        self.token_status_icon = QLabel()
        self.token_status_icon.setFixedSize(16, 16)

        self.token_status_label = QLabel(self.tray_app.current_token_status if self.tray_app else "Notion: No Token")
        self.token_status_label.setVisible(False)

        self.reconnect_btn = QPushButton("Retry Connection")
        self.reconnect_btn.setVisible(False)
        self.reconnect_btn.clicked.connect(self.tray_app.start_auto_retry_loop)
        token_status_layout.addWidget(self.reconnect_btn)

        self.offline_btn = QPushButton("Go Offline")
        self.offline_btn.setVisible(False)
        self.offline_btn.clicked.connect(self.tray_app.activate_offline_mode_manually)
        token_status_layout.addWidget(self.offline_btn)
        token_status_layout.addStretch()
        right_column.addLayout(token_status_layout)

        self.page_container = QScrollArea()
        self.page_container.setWidgetResizable(True)
        self.page_content = QWidget()
        self.page_layout = QVBoxLayout(self.page_content)
        self.page_layout.setContentsMargins(0, 0, 0, 0)
        self.page_layout.setSpacing(8)
        self.page_container.setWidget(self.page_content)
        right_column.addWidget(self.page_container, stretch=1)

        self.footer_status_icon = QLabel()
        self.footer_status_icon.setFixedSize(12, 12)

        self.footer_status_label = QLabel(self.tray_app.current_token_status if self.tray_app else "Notion: No Token")
        self.footer_status_label.setStyleSheet("font-size: 9pt; color: #bbbbbb;")

        self.version_label = QLabel(f"NotionLink - wladermisch | Version {APP_VERSION}")
        self.version_label.setStyleSheet("font-size: 8pt; color: #888888; margin-top: 6px;")
        self.version_label.setAlignment(Qt.AlignRight)

        footer_row = QHBoxLayout()
        footer_row.addWidget(self.footer_status_icon)
        footer_row.addWidget(self.footer_status_label)
        footer_row.addStretch()
        footer_row.addWidget(self.version_label)
        right_column.addLayout(footer_row)

        # Initialize status icons after both top and footer icon widgets exist.
        self.update_token_status_icon(self.tray_app.current_token_status if self.tray_app else "Notion: No Token")

        self.page_definitions = {
            "dashboard": {"title": "Dashboard", "crumbs": ["Dashboard"]},
            "page_mappings": {"title": "Page Mappings", "crumbs": ["Dashboard", "Page Mappings"]},
            "database_mappings": {"title": "Database Mappings", "crumbs": ["Dashboard", "Database Mappings"]},
            "mapping_editor": {"title": "Mapping Editor", "crumbs": ["Dashboard", "Page Mappings", "Mapping Editor"]},
            "token": {"title": "Notion Token", "crumbs": ["Dashboard", "Notion Token"]},
            "convert": {"title": "Convert Path", "crumbs": ["Dashboard", "Convert Path"]},
            "feedback": {"title": "Feedback", "crumbs": ["Dashboard", "Feedback"]},
            "help": {"title": "Help", "crumbs": ["Dashboard", "Help"]},
        }
        self.render_page("dashboard")
        return right_column

    def _clear_layout_recursive(self, layout):
        while layout.count():
            item = layout.takeAt(0)
            child_widget = item.widget()
            child_layout = item.layout()

            if child_widget is not None:
                child_widget.setParent(None)
                child_widget.deleteLater()
            elif child_layout is not None:
                self._clear_layout_recursive(child_layout)
                child_layout.deleteLater()
            # Spacer items are automatically removed by takeAt.

    def _clear_page_layout(self):
        self._clear_layout_recursive(self.page_layout)

    def render_page(self, page_key):
        self.current_page = page_key
        self._set_breadcrumb(page_key)
        self.back_btn.setVisible(page_key != "dashboard")
        self._clear_page_layout()

        if page_key == "dashboard":
            self._build_dashboard_page()
        elif page_key == "page_mappings":
            self._build_mapping_page("page")
        elif page_key == "database_mappings":
            self._build_mapping_page("database")
        elif page_key == "mapping_editor":
            self._build_mapping_editor_page()
        elif page_key == "token":
            self._build_token_page()
        elif page_key == "convert":
            self._build_convert_page()
        elif page_key == "feedback":
            self._build_feedback_page()
        elif page_key == "help":
            self._build_help_page()

        self._animate_page_content()

    def navigate_to(self, page_key, push_history=True):
        if page_key == self.current_page:
            return
        if page_key not in self.page_definitions:
            return
        if push_history:
            self.page_history.append(self.current_page)
        self.render_page(page_key)

    def go_back(self):
        if not self.page_history:
            return
        previous = self.page_history.pop()
        self.render_page(previous)

    def _on_breadcrumb_clicked(self, page_key):
        if page_key in self.page_definitions and page_key != self.current_page:
            self.navigate_to(page_key, push_history=False)

    def _set_breadcrumb(self, page_key):
        definition = self.page_definitions.get(page_key, self.page_definitions["dashboard"])
        crumbs = definition["crumbs"]
        key_map = {
            "Dashboard": "dashboard",
            "Page Mappings": "page_mappings",
            "Database Mappings": "database_mappings",
            "Mapping Editor": "mapping_editor",
            "Notion Token": "token",
            "Convert Path": "convert",
            "Feedback": "feedback",
            "Help": "help",
        }
        parts = []
        for idx, crumb in enumerate(crumbs):
            target = key_map.get(crumb)
            if idx < len(crumbs) - 1 and target:
                parts.append(f'<a href="{target}" style="color:#88c0ff;text-decoration:none;">{crumb}</a>')
            else:
                parts.append(f"<span>{crumb}</span>")
        self.breadcrumb_label.setText("  ›  ".join(parts))

    def _animate_page_content(self):
        if self._last_animation is not None:
            self._last_animation.stop()

        # Remove any previous effect to avoid stale rendered frames during repeated nav.
        self.page_content.setGraphicsEffect(None)

        effect = QGraphicsOpacityEffect(self.page_content)
        self._current_opacity_effect = effect
        self.page_content.setGraphicsEffect(effect)

        animation = QPropertyAnimation(effect, b"opacity", self.page_content)
        animation.setDuration(140)
        animation.setStartValue(0.55)
        animation.setEndValue(1.0)
        animation.setEasingCurve(QEasingCurve.OutCubic)
        animation.finished.connect(lambda: self.page_content.setGraphicsEffect(None))
        animation.start()
        self._last_animation = animation

    def _build_dashboard_page(self):
        title = QLabel("Mapping Overview")
        title.setStyleSheet("font-size: 14pt; font-weight: bold;")
        self.page_layout.addWidget(title)

        hint = QLabel("Manual refresh uploads all existing files for a mapping folder.")
        hint.setStyleSheet("color: #bbbbbb;")
        self.page_layout.addWidget(hint)

        self.mappings_list_container = QWidget()
        self.mappings_list_layout = QVBoxLayout(self.mappings_list_container)
        self.mappings_list_layout.setContentsMargins(0, 0, 0, 0)
        self.mappings_list_layout.setSpacing(6)
        self.page_layout.addWidget(self.mappings_list_container)
        self.page_layout.addStretch()

        self.refresh_mapping_overview()

    def _build_mapping_page(self, mapping_type):
        mapping_key = f"{mapping_type}_mappings"
        mappings = config.get(mapping_key, [])
        title = "Page Mappings" if mapping_type == "page" else "Database Mappings"
        title_label = QLabel(title)
        title_label.setStyleSheet("font-size: 13pt; font-weight: bold;")
        self.page_layout.addWidget(title_label)

        list_frame = QWidget()
        list_layout = QVBoxLayout(list_frame)
        list_layout.setContentsMargins(0, 0, 0, 0)
        list_layout.setSpacing(8)

        if not mappings:
            empty = QLabel("No mappings configured yet.")
            empty.setStyleSheet("color: #bbbbbb;")
            list_layout.addWidget(empty)

        for idx, mapping in enumerate(mappings):
            row_frame = QFrame()
            row_frame.setStyleSheet("QFrame { border: 1px solid #444444; border-radius: 4px; }")
            row_layout = QHBoxLayout(row_frame)
            row_layout.setContentsMargins(8, 6, 8, 6)
            row_layout.setSpacing(8)

            title_text = mapping.get("notion_title", f"Untitled {idx + 1}")
            folders_count = len(mapping.get("folders", []))
            info = QLabel(f"{title_text} ({folders_count} folder{'s' if folders_count != 1 else ''})")
            info.setWordWrap(True)
            row_layout.addWidget(info, stretch=1)

            edit_btn = QPushButton("Edit")
            edit_btn.setObjectName("secondaryButton")
            edit_btn.setFixedWidth(90)
            edit_btn.clicked.connect(lambda checked=False, i=idx, mt=mapping_type: self._edit_mapping(i, mt))
            row_layout.addWidget(edit_btn)

            remove_btn = QPushButton("Remove")
            remove_btn.setObjectName("dangerButton")
            remove_btn.setFixedWidth(90)
            remove_btn.clicked.connect(lambda checked=False, i=idx, mt=mapping_type: self._remove_mapping(i, mt))
            row_layout.addWidget(remove_btn)
            list_layout.addWidget(row_frame)

        list_scroll = QScrollArea()
        list_scroll.setWidgetResizable(True)
        list_scroll.setWidget(list_frame)
        self.page_layout.addWidget(list_scroll, stretch=1)

        btn_row = QHBoxLayout()
        add_label = "Add Page Mapping" if mapping_type == "page" else "Add Database Mapping"
        add_btn = QPushButton(add_label)
        if mapping_type == "page":
            add_btn.clicked.connect(lambda: self._open_mapping_editor("page"))
        else:
            add_btn.clicked.connect(lambda: self._add_mapping(mapping_type))
        btn_row.addWidget(add_btn)
        btn_row.addStretch()
        self.page_layout.addLayout(btn_row)

    def _open_mapping_editor(self, mapping_type, index=None):
        mapping_key = f"{mapping_type}_mappings"
        existing = None
        if index is not None:
            mappings = config.get(mapping_key, [])
            if 0 <= index < len(mappings):
                existing = mappings[index]

        self.mapping_editor_state = {
            "mapping_type": mapping_type,
            "index": index,
            "existing": existing,
        }
        self.navigate_to("mapping_editor")

    def _build_mapping_editor_page(self):
        state = self.mapping_editor_state or {"mapping_type": "page", "index": None, "existing": None}
        mapping_type = state.get("mapping_type", "page")
        existing = state.get("existing") or {}

        title = QLabel("Add Page Mapping" if state.get("index") is None else "Edit Page Mapping")
        title.setStyleSheet("font-size: 13pt; font-weight: bold;")
        self.page_layout.addWidget(title)

        self.mapping_editor_error = QLabel("")
        self.mapping_editor_error.setStyleSheet("color: #ff9999;")
        self.mapping_editor_current_type = mapping_type
        self.mapping_editor_manual_title = bool(existing.get("notion_title", "").strip())
        self.mapping_editor_title_fetcher = None

        form = QFormLayout()
        self.mapping_editor_notion_id = QLineEdit(existing.get("notion_id", ""))
        self.mapping_editor_notion_title = QLineEdit(existing.get("notion_title", ""))
        self.mapping_editor_ignore_ext = QLineEdit(", ".join(existing.get("ignore_extensions", ["*.tmp", ".*", "desktop.ini"])))
        self.mapping_editor_ignore_files = QLineEdit(", ".join(existing.get("ignore_files", [])))
        self.mapping_editor_folder_discovery = QCheckBox("Include subfolder files")
        self.mapping_editor_folder_discovery.setChecked(existing.get("folder_discovery", False))
        self.mapping_editor_folder_links = QCheckBox("Add subfolder links")
        self.mapping_editor_folder_links.setChecked(existing.get("folder_links", False))
        self.mapping_editor_lifecycle = QCheckBox("Full lifecycle sync (rename/delete)")
        self.mapping_editor_lifecycle.setChecked(existing.get("full_lifecycle_sync", True))

        form.addRow("Notion Link or ID:", self.mapping_editor_notion_id)
        form.addRow("Mapping Title:", self.mapping_editor_notion_title)
        form.addRow("Ignore extensions:", self.mapping_editor_ignore_ext)
        form.addRow("Ignore files:", self.mapping_editor_ignore_files)
        self.page_layout.addLayout(form)

        self.mapping_editor_notion_id.textChanged.connect(self._mapping_editor_parse_notion_input)
        self.mapping_editor_notion_title.textEdited.connect(self._mapping_editor_on_title_edited)

        # Prime title autofill immediately if a valid Notion link/ID already exists.
        self._mapping_editor_parse_notion_input(self.mapping_editor_notion_id.text())

        self.page_layout.addWidget(self.mapping_editor_folder_discovery)
        self.page_layout.addWidget(self.mapping_editor_folder_links)
        self.page_layout.addWidget(self.mapping_editor_lifecycle)

        folders_label = QLabel("Synced folders")
        folders_label.setStyleSheet("font-size: 11pt; font-weight: bold;")
        self.page_layout.addWidget(folders_label)

        self.mapping_editor_folders = QListWidget()
        for folder in existing.get("folders", []):
            self.mapping_editor_folders.addItem(folder)
        self.mapping_editor_folders.setMinimumHeight(160)
        self.page_layout.addWidget(self.mapping_editor_folders)

        folder_btn_row = QHBoxLayout()
        add_folder_btn = QPushButton("Add Folder")
        add_folder_btn.clicked.connect(self._mapping_editor_add_folder)
        remove_folder_btn = QPushButton("Remove Selected")
        remove_folder_btn.setObjectName("secondaryButton")
        remove_folder_btn.clicked.connect(self._mapping_editor_remove_selected_folder)
        folder_btn_row.addWidget(add_folder_btn)
        folder_btn_row.addWidget(remove_folder_btn)
        folder_btn_row.addStretch()
        self.page_layout.addLayout(folder_btn_row)

        self.page_layout.addWidget(self.mapping_editor_error)

        action_row = QHBoxLayout()
        save_btn = QPushButton("Save Mapping")
        save_btn.clicked.connect(self._mapping_editor_save)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.setObjectName("secondaryButton")
        cancel_btn.clicked.connect(lambda: self.navigate_to("page_mappings", push_history=False))
        action_row.addWidget(save_btn)
        action_row.addWidget(cancel_btn)
        action_row.addStretch()
        self.page_layout.addLayout(action_row)
        self.page_layout.addStretch()

    def _mapping_editor_add_folder(self):
        folder_path = QFileDialog.getExistingDirectory(self, "Select a folder to sync")
        if folder_path:
            self.mapping_editor_folders.addItem(folder_path)

    def _mapping_editor_remove_selected_folder(self):
        for item in self.mapping_editor_folders.selectedItems():
            self.mapping_editor_folders.takeItem(self.mapping_editor_folders.row(item))

    def _mapping_editor_save(self):
        notion_input = self.mapping_editor_notion_id.text().strip()
        id_tuple = extract_id_and_title_from_link(notion_input)
        if not id_tuple:
            self.mapping_editor_error.setText("Invalid Notion link or ID.")
            return

        notion_id, title_from_url = id_tuple
        notion_title_raw = self.mapping_editor_notion_title.text().strip()
        if notion_title_raw.lower().startswith("fetching title"):
            notion_title_raw = ""
        notion_title = notion_title_raw or title_from_url or f"Untitled (...{notion_id[-6:]})"
        folders = [self.mapping_editor_folders.item(i).text() for i in range(self.mapping_editor_folders.count())]
        if not folders:
            self.mapping_editor_error.setText("Add at least one folder.")
            return

        mapping_data = {
            "notion_title": notion_title,
            "notion_id": notion_id,
            "folders": folders,
            "ignore_extensions": [p.strip() for p in self.mapping_editor_ignore_ext.text().split(",") if p.strip()],
            "ignore_files": [p.strip() for p in self.mapping_editor_ignore_files.text().split(",") if p.strip()],
            "full_lifecycle_sync": self.mapping_editor_lifecycle.isChecked(),
            "folder_discovery": self.mapping_editor_folder_discovery.isChecked(),
            "folder_links": self.mapping_editor_folder_links.isChecked(),
            "enabled": True,
        }

        state = self.mapping_editor_state or {}
        mapping_type = state.get("mapping_type", "page")
        key = f"{mapping_type}_mappings"
        idx = state.get("index")

        if idx is None:
            config[key].append(mapping_data)
            self.tray_app.show_notification("Mapping Added", "New mapping saved.")
            # Match manual-upload behavior: immediately backfill new mapping folders.
            for folder_path in mapping_data.get("folders", []):
                threading.Thread(
                    target=self.tray_app.upload_folder_to_notion,
                    args=(folder_path, mapping_data, mapping_type),
                    daemon=True,
                ).start()
        else:
            if idx < 0 or idx >= len(config.get(key, [])):
                self.mapping_editor_error.setText("Failed to update mapping. Please reopen editor.")
                return
            config[key][idx] = mapping_data
            self.tray_app.show_notification("Mapping Updated", "Mapping saved.")

        with open(config_file_path, "w") as config_file:
            json.dump(config, config_file, indent=4)

        self.tray_app.restart_file_observer()
        self.mapping_editor_state = None
        self.navigate_to("page_mappings", push_history=False)

    def _mapping_editor_on_title_edited(self, _text):
        self.mapping_editor_manual_title = True

    def _mapping_editor_parse_notion_input(self, text):
        id_tuple = extract_id_and_title_from_link(text)
        if not id_tuple:
            return

        notion_id, title_from_url = id_tuple

        current_title = self.mapping_editor_notion_title.text().strip()
        if not self.mapping_editor_manual_title and (not current_title or current_title.lower().startswith("fetching title")):
            self.mapping_editor_notion_title.setText(title_from_url or "Fetching title...")

        if self.mapping_editor_title_fetcher and self.mapping_editor_title_fetcher.isRunning():
            self.mapping_editor_title_fetcher.terminate()

        self.mapping_editor_title_fetcher = TitleFetcher(
            notion_id,
            config.get("notion_token"),
            self.mapping_editor_current_type == "database",
        )
        self.mapping_editor_title_fetcher.title_fetched.connect(self._mapping_editor_apply_fetched_title)
        self.mapping_editor_title_fetcher.start()

    def _mapping_editor_apply_fetched_title(self, title):
        if not title:
            return
        if self.mapping_editor_manual_title:
            return
        self.mapping_editor_notion_title.setText(title)

    def _build_token_page(self):
        title = QLabel("Notion Token")
        title.setStyleSheet("font-size: 12pt; font-weight: bold;")
        self.page_layout.addWidget(title)

        form = QFormLayout()
        self.token_edit = QLineEdit(config.get("notion_token", ""))
        self.token_edit.setEchoMode(QLineEdit.Password)
        form.addRow("Token:", self.token_edit)
        self.page_layout.addLayout(form)

        btn_row = QHBoxLayout()
        save_btn = QPushButton("Save Token")
        save_btn.clicked.connect(self._save_token_inline)
        btn_row.addWidget(save_btn)
        btn_row.addStretch()
        self.page_layout.addLayout(btn_row)

        self.token_page_status_label = QLabel("")
        self.token_page_status_label.setStyleSheet("font-size: 10pt; font-weight: bold;")
        self.page_layout.addWidget(self.token_page_status_label)

        self.token_help_label = QLabel(
            "How to get your token:\n"
            "1. Open https://www.notion.so/my-integrations\n"
            "2. Create/open an integration\n"
            "3. Copy the Internal Integration Token and save it above"
        )
        self.token_help_label.setWordWrap(True)
        self.token_help_label.setStyleSheet("color: #ffcc66;")
        self.page_layout.addWidget(self.token_help_label)

        self.token_perm_note_label = QLabel(
            "Important: each Notion page/database mapping must be shared with your integration "
            "(Share -> Add connections), otherwise syncing will not work."
        )
        self.token_perm_note_label.setWordWrap(True)
        self.token_perm_note_label.setStyleSheet("color: #bbbbbb;")
        self.page_layout.addWidget(self.token_perm_note_label)

        self._update_token_page_status_content(self.tray_app.current_token_status if self.tray_app else "Notion: No Token")
        self.page_layout.addStretch()

    def _build_convert_page(self):
        title = QLabel("Convert Path to Link")
        title.setStyleSheet("font-size: 12pt; font-weight: bold;")
        self.page_layout.addWidget(title)

        self.convert_input = QLineEdit()
        self.convert_input.setPlaceholderText("Paste file path...")
        self.page_layout.addWidget(self.convert_input)

        output_label = QLabel("Generated Link")
        output_label.setStyleSheet("color: #bbbbbb;")
        self.page_layout.addWidget(output_label)

        self.convert_output = QLineEdit()
        self.convert_output.setReadOnly(True)
        self.page_layout.addWidget(self.convert_output)

        btn_row = QHBoxLayout()
        convert_btn = QPushButton("Convert and Copy")
        convert_btn.clicked.connect(self._convert_path_inline)
        btn_row.addWidget(convert_btn)
        btn_row.addStretch()
        self.page_layout.addLayout(btn_row)
        self.page_layout.addStretch()

    def _build_feedback_page(self):
        title = QLabel("Send Feedback")
        title.setStyleSheet("font-size: 12pt; font-weight: bold;")
        self.page_layout.addWidget(title)
        hint = QLabel("Use this for bug reports or suggestions.")
        hint.setStyleSheet("color: #bbbbbb;")
        self.page_layout.addWidget(hint)

        self.feedback_input = QTextEdit()
        self.feedback_input.setPlaceholderText("Please describe the issue or idea...")
        self.feedback_input.setMinimumHeight(140)
        self.page_layout.addWidget(self.feedback_input)

        self.feedback_contact_input = QLineEdit()
        self.feedback_contact_input.setPlaceholderText("Discord name (optional)")
        self.page_layout.addWidget(self.feedback_contact_input)

        btn_row = QHBoxLayout()
        send_btn = QPushButton("Send Feedback")
        send_btn.clicked.connect(self._send_feedback_inline)
        btn_row.addWidget(send_btn)
        btn_row.addStretch()
        self.page_layout.addLayout(btn_row)
        self.page_layout.addStretch()

    def _send_feedback_inline(self):
        feedback = self.feedback_input.toPlainText().strip()
        if not feedback:
            self._set_status_message("Please enter feedback before sending.", "warning", transient=True)
            return

        try:
            if sentry_sdk is not None:
                with sentry_sdk.isolation_scope() as scope:
                    contact = self.feedback_contact_input.text().strip()
                    if contact:
                        scope.set_user({"username": contact})
                    sentry_sdk.capture_message(feedback, level="info")
                self.feedback_input.clear()
                self._set_status_message("Feedback sent. Thank you!", "ok", transient=True)
                return
            self._set_status_message("Feedback service unavailable in this build.", "warning", transient=True)
        except Exception as e:
            print(f"Error sending inline feedback: {e}")
            self._set_status_message("Could not send feedback right now. Check logs.", "error", transient=True)

    def _build_help_page(self):
        title = QLabel("Help & Documentation")
        title.setStyleSheet("font-size: 12pt; font-weight: bold;")
        self.page_layout.addWidget(title)
        help_text = QLabel(
            "NotionLink needs to stay running for local links to open. "
            "Share mapped Notion pages/databases with your integration."
        )
        help_text.setWordWrap(True)
        self.page_layout.addWidget(help_text)
        btn_row = QHBoxLayout()
        open_btn = QPushButton("Open Wiki")
        open_btn.clicked.connect(lambda: webbrowser.open_new("https://github.com/wladermisch/NotionLink/wiki"))
        btn_row.addWidget(open_btn)
        btn_row.addStretch()
        self.page_layout.addLayout(btn_row)
        self.page_layout.addStretch()

    def _add_mapping(self, mapping_type):
        dialog = EditMappingDialog(self.tray_app, mapping_type=mapping_type)
        if dialog.exec() == QDialog.Accepted:
            data = dialog.get_mapping_data()
            if not data:
                return
            key = f"{mapping_type}_mappings"
            config[key].append(data)
            with open(config_file_path, "w") as config_file:
                json.dump(config, config_file, indent=4)
            self.tray_app.restart_file_observer()
            self.tray_app.show_notification("Mapping Added", "New mapping saved.")
            self.render_page(self.current_page)

    def _edit_mapping(self, index, mapping_type):
        key = f"{mapping_type}_mappings"
        mappings = config.get(key, [])
        if index < 0 or index >= len(mappings):
            return
        dialog = EditMappingDialog(self.tray_app, existing_mapping=mappings[index], mapping_type=mapping_type)
        if dialog.exec() == QDialog.Accepted:
            data = dialog.get_mapping_data()
            if not data:
                return
            config[key][index] = data
            with open(config_file_path, "w") as config_file:
                json.dump(config, config_file, indent=4)
            self.tray_app.restart_file_observer()
            self.tray_app.show_notification("Mapping Updated", "Mapping saved.")
            self.render_page(self.current_page)

    def _remove_mapping(self, index, mapping_type):
        key = f"{mapping_type}_mappings"
        mappings = config.get(key, [])
        if index < 0 or index >= len(mappings):
            return
        confirm = QMessageBox.warning(
            self,
            "Confirm Delete",
            "Are you sure you want to remove this mapping?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No,
        )
        if confirm != QMessageBox.Yes:
            return
        del config[key][index]
        with open(config_file_path, "w") as config_file:
            json.dump(config, config_file, indent=4)
        self.tray_app.restart_file_observer()
        self.render_page(self.current_page)

    def _save_token_inline(self):
        config["notion_token"] = self.token_edit.text().strip()
        with open(config_file_path, "w") as config_file:
            json.dump(config, config_file, indent=4)
        self._update_token_page_status_content("Notion: Checking...")
        self.tray_app.run_status_check_thread()
        self._set_status_message("Token saved.", "ok", transient=True)

    def _update_token_page_status_content(self, status):
        if not hasattr(self, "token_page_status_label"):
            return

        self.token_page_status_label.setText(f"Status: {status}")
        lowered = (status or "").lower()
        connection_ok = status == "Notion: Connected"

        if connection_ok:
            self.token_page_status_label.setStyleSheet("font-size: 10pt; font-weight: bold; color: #66ff66;")
        elif "checking" in lowered:
            self.token_page_status_label.setStyleSheet("font-size: 10pt; font-weight: bold; color: #ffd166;")
        else:
            self.token_page_status_label.setStyleSheet("font-size: 10pt; font-weight: bold; color: #ff9999;")

        if hasattr(self, "token_help_label"):
            self.token_help_label.setVisible(not connection_ok)

    def _convert_path_inline(self):
        path_to_convert = self.convert_input.text().strip().replace('"', "")
        if not path_to_convert:
            self._set_status_message("Please enter a file path to convert.", "warning", transient=True)
            return
        port = config.get("server_port")
        server_host = config.get("server_host")
        url_path = path_to_convert.replace("\\", "/")
        if url_path.startswith('/'):
            url_path = url_path[1:]
        result = f"{server_host}:{port}/{url_path}"
        self.convert_output.setText(result)
        try:
            import pyperclip as clip
            clip.copy(result)
        except Exception:
            pass
        self._set_status_message("Link converted and copied to clipboard.", "ok", transient=True)

    def refresh_mapping_overview(self):
        if not hasattr(self, "mappings_list_layout"):
            return
        try:
            while self.mappings_list_layout.count():
                item = self.mappings_list_layout.takeAt(0)
                widget = item.widget()
                if widget:
                    widget.deleteLater()
        except RuntimeError:
            # Layout was deleted during page switch; skip this refresh cycle.
            return

        self._active_mapping_buttons = {}
        all_mappings = [
            ("page", idx, m) for idx, m in enumerate(config.get("page_mappings", []))
        ] + [
            ("database", idx, m) for idx, m in enumerate(config.get("database_mappings", []))
        ]

        if not all_mappings:
            empty = QLabel("No mappings configured yet.")
            empty.setStyleSheet("color: #bbbbbb;")
            self.mappings_list_layout.addWidget(empty)
            return

        for mapping_type, idx, mapping in all_mappings:
            row = QFrame()
            row.setStyleSheet("QFrame { border: 1px solid #444444; border-radius: 4px; }")
            row_layout = QHBoxLayout(row)
            row_layout.setContentsMargins(8, 6, 8, 6)
            row_layout.setSpacing(8)

            name = mapping.get("notion_title", "Untitled")
            state = "healthy"
            state_tip = "All mapped folders exist and contain files"
            state_color = "#66ff66"
            for folder in mapping.get("folders", []):
                if not folder or not os.path.exists(folder):
                    state = "missing"
                    state_tip = f"Missing or inaccessible: {folder}"
                    state_color = "#ff6666"
                    break
                if os.path.isdir(folder) and len(os.listdir(folder)) == 0 and state != "missing":
                    state = "empty"
                    state_tip = f"Folder exists but is empty: {folder}"
                    state_color = "#ffd166"

            indicator = f"<span style='color:{state_color};font-weight:bold;'>●</span>"
            info = QLabel(f"{name} [{mapping_type}]  {indicator}")
            info.setTextFormat(Qt.RichText)
            info.setWordWrap(True)
            info.setToolTip(state_tip)
            row_layout.addWidget(info, stretch=1)

            enabled_box = QCheckBox("Enabled")
            enabled_box.setChecked(mapping.get("enabled", True))
            enabled_box.toggled.connect(lambda checked=False, mt=mapping_type, i=idx: self._toggle_mapping_enabled(mt, i, checked))
            row_layout.addWidget(enabled_box)

            refresh_btn = QPushButton("↻")
            refresh_btn.setToolTip("Manual refresh mapping")
            refresh_btn.setFixedWidth(34)
            refresh_btn.clicked.connect(lambda checked=False, mt=mapping_type, i=idx, b=refresh_btn: self._refresh_mapping_manual(mt, i, b))
            row_layout.addWidget(refresh_btn)

            self._active_mapping_buttons[(mapping_type, idx)] = refresh_btn
            self.mappings_list_layout.addWidget(row)

    def _toggle_mapping_enabled(self, mapping_type, index, checked):
        key = f"{mapping_type}_mappings"
        mappings = config.get(key, [])
        if index < 0 or index >= len(mappings):
            return
        mappings[index]["enabled"] = bool(checked)
        with open(config_file_path, "w") as config_file:
            json.dump(config, config_file, indent=4)
        self.tray_app.restart_file_observer()

    def _refresh_mapping_manual(self, mapping_type, index, button):
        key = f"{mapping_type}_mappings"
        mappings = config.get(key, [])
        if index < 0 or index >= len(mappings):
            return
        mapping = mappings[index]
        if button.property("busy"):
            return

        button.setProperty("busy", True)
        button.setEnabled(False)
        button.setText("⟳")

        spin_timer = QTimer(button)
        spin_frames = ["⟳", "⟲"]
        spin_state = {"idx": 0}

        def _spin():
            spin_state["idx"] = (spin_state["idx"] + 1) % len(spin_frames)
            button.setText(spin_frames[spin_state["idx"]])

        spin_timer.timeout.connect(_spin)
        spin_timer.start(220)

        run_result = {"had_error": False, "had_valid_folder": False}

        def _run_upload():
            for folder in mapping.get("folders", []):
                if not folder or not os.path.isdir(folder):
                    msg = f"Mapped folder path is missing or inaccessible: {folder}"
                    self.tray_app.user_error_signal.emit(msg)
                    run_result["had_error"] = True
                    continue
                run_result["had_valid_folder"] = True
                self.tray_app.upload_folder_to_notion(folder, mapping, mapping_type)

        worker = threading.Thread(target=_run_upload, daemon=True)
        worker.start()

        wait_timer = QTimer(button)

        def _check_done():
            if worker.is_alive():
                return
            wait_timer.stop()
            spin_timer.stop()
            button.setProperty("busy", False)
            button.setEnabled(True)
            button.setText("↻")
            if run_result["had_error"] and not run_result["had_valid_folder"]:
                self._set_status_message("Manual refresh failed: all mapped folders are missing or inaccessible.", "error", transient=True)
            elif run_result["had_error"]:
                self._set_status_message("Manual refresh completed with warnings for invalid folders.", "warning", transient=True)
            elif not run_result["had_valid_folder"]:
                self._set_status_message("Manual refresh skipped: no mapped folders configured.", "warning", transient=True)
            else:
                self._set_status_message("Manual refresh completed.", "ok", transient=True)

        wait_timer.timeout.connect(_check_done)
        wait_timer.start(200)

    def _set_status_message(self, message, level="ok", transient=False):
        if not message:
            return
        if level == "error":
            style = self._get_status_style("#4a1a1a", "#ff6666", "#6a2a2a")
        elif level == "warning":
            style = self._get_status_style("#4a4a1a", "#ffff66", "#6a6a2a")
        else:
            style = self._get_status_style("#1e3a1e", "#66ff66", "#2e5a2e")

        self.status_panel.setText(message)
        self.status_panel.setStyleSheet(style)
        self.transient_status_active = transient
        self.ack_btn.setVisible(transient)

    def acknowledge_status(self):
        if not self.transient_status_active:
            return
        self.transient_status_active = False
        self.ack_btn.setVisible(False)
        self.update_token_status(self.token_status_label.text())

    def _get_status_style(self, bg_color, text_color, border_color):
        return f"""
            QLabel {{
                background-color: {bg_color};
                color: {text_color};
                padding: 15px;
                border-radius: 5px;
                font-size: 11pt;
                font-weight: bold;
                border: 2px solid {border_color};
            }}
        """

    def update_token_status(self, status):
        # Update token status display and dashboard status panel
        self.token_status_label.setText(status)
        self.update_token_status_icon(status)
        self._update_token_page_status_content(status)
        
        self.footer_status_label.setText(status)

        # Show/hide buttons
        self.reconnect_btn.setVisible(False)
        self.offline_btn.setVisible(False)
        
        if status == "Notion: Connection Error":
            self.reconnect_btn.setVisible(True)
            self.reconnect_btn.setText("Retry Connection")
            self.offline_btn.setVisible(True)
        elif status == "Notion: Offline Mode":
            self.reconnect_btn.setVisible(True)
            self.reconnect_btn.setText("Reconnect Now")
        elif status == "Notion: Disconnected":
            self.reconnect_btn.setVisible(True)
            self.reconnect_btn.setText("Retry Connection")
        
        # Persistent states should remain visible until resolved.
        persistent = True
        if status == "Notion: Connected":
            self._set_status_message("NotionLink is running...", "ok", transient=False)
        elif status == "Notion: Connection Error":
            self._set_status_message("Connection failed. Please check your internet connection or Notion token.", "warning", transient=not persistent)
        elif status in ["Notion: Disconnected", "Notion: Invalid Token", "Notion: Access Denied"]:
            self._set_status_message(f"{status}", "error", transient=not persistent)
        elif status == "Notion: Offline Mode":
            self._set_status_message("Offline mode active. Sync is paused.", "warning", transient=not persistent)
        elif status == "Notion: No Token":
            self._set_status_message(f"{status} - Please configure your Notion token", "warning", transient=not persistent)
        
    def update_token_status_icon(self, status):
        # Update the colored circle indicator
        if status == "Notion: Connected":
            color = "#00ff00"  # Green
        elif status == "Notion: Disconnected" or status == "Notion: Connection Error":
            color = "#ff0000"  # Red
        elif status == "Notion: Offline Mode":
            color = "#808080"  # Gray
        elif status == "Notion: No Token":
            color = "#808080"  # Gray
        else:
            color = "#ffff00"  # Yellow
        
        pixmap = QPixmap(16, 16)
        pixmap.fill(Qt.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setBrush(QColor(color))
        painter.setPen(Qt.NoPen)
        painter.drawEllipse(2, 2, 12, 12)
        painter.end()
        self.token_status_icon.setPixmap(pixmap)
        if hasattr(self, "footer_status_icon") and self.footer_status_icon is not None:
            self.footer_status_icon.setPixmap(pixmap.scaled(12, 12, Qt.KeepAspectRatio, Qt.SmoothTransformation))
        
    def update_status_panel_error(self, message):
        # Transient warning/error messages are user-acknowledgeable.
        self._set_status_message(f"{message}", "error", transient=True)
    
    def update_status_panel_warning(self, message):
        self._set_status_message(f"{message}", "warning", transient=True)

    def reset_status_panel(self):
        self.acknowledge_status()
        
    def append_log_line(self, line):
        # Surface only real warning/error log entries with specific message text.
        if self.transient_status_active:
            return

        parsed = self._parse_issue_log_line(line)
        if not parsed:
            return

        level, message = parsed
        if level == "WARNING":
            self._set_status_message(f"Warning: {message}", "warning", transient=True)
        else:
            self._set_status_message(f"Error: {message}", "error", transient=True)

    def _parse_issue_log_line(self, line):
        if not line:
            return None

        # Expected format: YYYY-MM-DD ... LEVEL logger.name: message
        match = re.match(
            r"^\d{4}-\d{2}-\d{2} .*?\s(?P<level>WARNING|ERROR|CRITICAL)\s+[\w\.]+:\s*(?P<message>.*)$",
            line,
        )
        if not match:
            return None

        level = match.group("level")
        message = (match.group("message") or "").strip()

        # Ignore stack-trace framing lines and empty payloads.
        if not message or message.lower().startswith("traceback"):
            return None

        if len(message) > 220:
            message = message[:217] + "..."

        return level, message

    def show_help(self):
        self.navigate_to("help")
        
    def closeEvent(self, event):
        # Handle window close event
        # Stop log watcher timer
        if self.log_watcher and self.log_watcher.timer:
            self.log_watcher.timer.stop()
        if self.tray_app:
            self.tray_app.on_dashboard_closed()
        event.accept()


class NotionLinkTrayApp(QObject):
    
    status_updated = Signal(str)
    server_error_signal = Signal(str)
    user_error_signal = Signal(str)
    op_success_signal = Signal()
    offline_mode_signal = Signal()
    notification_timer_signal = Signal()
    dashboard_open_signal = Signal(str)
    
    def __init__(self, app):
        super().__init__()
        self.app = app
        self.dashboard_window = None
        self.current_token_status = "Notion: No Token"
        self.status_check_timer = None
        self.notification_timer = None
        self.auto_retry_timer = None
        self.is_auto_retrying = False
        
        # Cached errors for dashboard display
        self.last_server_error = None
        self.last_user_error = None
        
        # Create colored icons for status
        self.green_icon = self.create_color_icon("#00ff00")
        self.yellow_icon = self.create_color_icon("#ffff00")
        self.red_icon = self.create_color_icon("#ff0000")
        self.gray_icon = self.create_color_icon("#808080")
        
        # Create system tray icon
        self.tray_icon = QSystemTrayIcon(QIcon(TRAY_ICON_ICO), parent=app)
        
        # Create context menu
        self.menu = QMenu()
        self.menu.setStyleSheet(DARK_STYLESHEET)
        
        # Status indicator (clickable) - shows current Notion token status
        self.status_action = QAction("Notion: No Token", self)
        self.status_action.setIcon(self.gray_icon)
        # make it clickable so user can force a manual status check
        self.status_action.setEnabled(True)
        self.status_action.setToolTip("Click to check Notion token status")
        self.status_action.triggered.connect(self.manual_status_check)
        self.menu.addAction(self.status_action)
        self.menu.addSeparator()
        
        # Main actions
        self.add_menu_action("Dashboard", self.show_dashboard, bold=True)
        self.menu.addSeparator()
        self.add_menu_action("Convert Path to Link", self.show_convert_path)
        self.menu.addSeparator()
        
        self.add_menu_action("Quit", self.quit_app)
        
        self.tray_icon.setContextMenu(self.menu)
        self.tray_icon.activated.connect(self.on_tray_icon_activated)
        self.tray_icon.show()
        
        # Start notification batch timer (SingleShot for debouncing)
        self.notification_timer = QTimer(self)
        self.notification_timer.setSingleShot(True)
        self.notification_timer.timeout.connect(self.process_notification_batch)
        self.notification_timer_signal.connect(lambda: self.notification_timer.start(5000))
        
        # Connect status signal
        self.status_updated.connect(self.update_status_ui)
        self.offline_mode_signal.connect(self.on_offline_mode_activated)
        self.dashboard_open_signal.connect(self._open_dashboard_from_signal)
        
        # Check for updates
        self.update_thread = UpdateCheckThread()
        self.update_thread.update_available.connect(self.show_update_dialog)
        QTimer.singleShot(3000, self.update_thread.start)
        
    def show_update_dialog(self, latest_version, url):
        dialog = UpdateAvailableDialog(APP_VERSION, latest_version, url)
        dialog.exec()

    def show_notification(self, title, message, icon=QSystemTrayIcon.Information, timeout=3000):
        try:
            self.tray_icon.showMessage(title, message, icon, timeout)
        except Exception:
            pass
        
    def reset_notification_timer(self):
        self.notification_timer_signal.emit()
        
    def process_notification_batch(self):
        # Process batched sync notifications
        global notification_batch
        if not notification_batch:
            return
        
        # Create a snapshot to avoid "dictionary changed size during iteration"
        batch_snapshot = dict(notification_batch)
        notification_batch.clear()
        
        print(f"Processing notification batch with {len(batch_snapshot)} entries...")
        for notion_title, filenames in batch_snapshot.items():
            count = len(filenames)
            if count == 1:
                message = f"'{filenames[0]}' was added to {notion_title}."
            else:
                message = f"Synced {count} new files to {notion_title}."
            self.tray_icon.showMessage("NotionLink: Sync Success", message, QSystemTrayIcon.Information, 3000)

    def on_tray_icon_activated(self, reason):
        # Handle tray icon click
        print(f"Tray icon activated, reason={reason}")
        try:
            trigger_value = QSystemTrayIcon.ActivationReason.Trigger
        except Exception:
            trigger_value = getattr(QSystemTrayIcon, 'Trigger', None)

        if reason == trigger_value or reason == QSystemTrayIcon.Trigger:
            print("Tray click detected: opening dashboard")
            self.show_dashboard()
            # Trigger a status check when opening dashboard to refresh status
            self.manual_status_check()
        
    def create_color_icon(self, color_hex):
        # Create a small colored circle icon
        pixmap = QPixmap(16, 16)
        pixmap.fill(Qt.transparent)
        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setBrush(QColor(color_hex))
        painter.setPen(Qt.NoPen)
        painter.drawEllipse(2, 2, 12, 12)
        painter.end()
        return QIcon(pixmap)

    def add_menu_action(self, text, callback, bold=False):
        # add action to tray menu
        action = QAction(text, self)
        if bold:
            font = action.font()
            font.setBold(True)
            action.setFont(font)
        action.triggered.connect(callback)
        self.menu.addAction(action)

    def run_status_check_thread(self):
        # Check Notion token status periodically
        def check_and_update():
            check_notion_status_once(self.update_status_ui_from_thread)
        
        threading.Thread(target=check_and_update, daemon=True).start()
        
        if not self.status_check_timer:
            self.status_check_timer = QTimer(self)
            self.status_check_timer.timeout.connect(self.run_status_check_thread)
            self.status_check_timer.start(300000)  # 5 minutes

    def update_status_ui_from_thread(self, status):
        # Emit signal from background thread
        self.status_updated.emit(status)

    def manual_status_check(self):
        # User-triggered manual check of Notion token status (runs in background)
        def _check():
            try:
                # Show an intermediate checking state
                self.status_updated.emit("Notion: Checking...")
                # Force check even if in offline mode
                check_notion_status_once(self.update_status_ui_from_thread, force=True)
            except Exception as e:
                print(f"Manual status check failed: {e}")
                # Fallback to disconnected
                self.status_updated.emit("Notion: Disconnected")

        threading.Thread(target=_check, daemon=True).start()

    def start_auto_retry_loop(self):
        # Start auto-retry loop for connection
        if self.is_auto_retrying:
            return
            
        print("Starting auto-retry loop...")
        self.is_auto_retrying = True
        self.status_updated.emit("Notion: Retrying...")
        
        # Disable offline mode temporarily to allow checks
        import src.core as core_module
        core_module.offline_mode = False
        
        def _retry_check():
            if not self.is_auto_retrying:
                return
                
            def _callback(status):
                if status == "Notion: Connected":
                    print("Auto-retry successful! Connection restored.")
                    self.is_auto_retrying = False
                    self.status_updated.emit(status)
                    self.tray_icon.showMessage("NotionLink", "Internet connection restored.", QSystemTrayIcon.Information, 3000)
                    
                    # Process any pending uploads that were queued during outage
                    process_pending_uploads()
                    
                    if self.auto_retry_timer:
                        self.auto_retry_timer.stop()
                        self.auto_retry_timer = None
                else:
                    print(f"Auto-retry failed ({status}). Retrying in 10s...")
                    # Don't update UI to "Disconnected" constantly to avoid flickering/spam
                    # Just keep "Retrying..." or update if it changes to something specific like "Invalid Token"
                    if status != "Notion: Connection Error" and status != "Notion: Disconnected":
                         self.status_updated.emit(status)
            
            check_notion_status_once(_callback, force=True)

        # Create timer for periodic checks
        self.auto_retry_timer = QTimer(self)
        self.auto_retry_timer.timeout.connect(lambda: threading.Thread(target=_retry_check, daemon=True).start())
        self.auto_retry_timer.start(10000) # Check every 10 seconds
        
        # Run first check immediately
        threading.Thread(target=_retry_check, daemon=True).start()

    def update_status_ui(self, status):
        # Update status UI in main thread
        self.current_token_status = status
        self.status_action.setText(status)
        if status == "Notion: Connected":
            self.status_action.setIcon(self.green_icon)
        elif status == "Notion: Disconnected" or status == "Notion: Connection Error":
            self.status_action.setIcon(self.red_icon)
        elif status == "Notion: Offline Mode":
            self.status_action.setIcon(self.gray_icon)
        elif status == "Notion: No Token":
            self.status_action.setIcon(self.gray_icon)
        else:
            self.status_action.setIcon(self.yellow_icon)
            
        if self.dashboard_window:
            self.dashboard_window.update_token_status(status)

    def sync_autostart_ui(self, is_checked):
        # Sync autostart checkbox across UI components
        if hasattr(self, "autostart_action") and self.autostart_action:
            self.autostart_action.blockSignals(True)
            self.autostart_action.setChecked(is_checked)
            self.autostart_action.blockSignals(False)
        
        if self.dashboard_window:
            self.dashboard_window.autostart_checkbox.blockSignals(True)
            self.dashboard_window.autostart_checkbox.setChecked(is_checked)
            self.dashboard_window.autostart_checkbox.blockSignals(False)

    def toggle_autostart(self, checked):
        # Toggle Windows autostart
        global config
        print(f"Setting autostart to: {checked}")
        try:
            manage_autostart(checked)
            config["autostart_with_windows"] = checked
            with open(config_file_path, "w") as f:
                json.dump(config, f, indent=4)
            self.sync_autostart_ui(checked)
                
        except Exception as e:
            print(f"Error toggling autostart: {e}")
            self.sync_autostart_ui(not checked) 
            
            error_dialog = QMessageBox()
            error_dialog.setWindowIcon(QIcon(TRAY_ICON_ICO))
            error_dialog.setStyleSheet(DARK_STYLESHEET.replace("QMenu", "QMessageBox"))
            error_dialog.setIcon(QMessageBox.Warning)
            error_dialog.setText("Autostart Error")
            error_dialog.setInformativeText(f"Could not update autostart setting.\nError: {e}")
            error_dialog.setStandardButtons(QMessageBox.Ok)
            error_dialog.exec()

    def sync_sentry_ui(self, is_checked):
        # Sync Sentry checkbox across UI components
        if self.dashboard_window:
            self.dashboard_window.sentry_checkbox.blockSignals(True)
            self.dashboard_window.sentry_checkbox.setChecked(is_checked)
            self.dashboard_window.sentry_checkbox.blockSignals(False)

    def toggle_sentry(self, checked):
        # Toggle Sentry error reporting
        global config
        print(f"Setting Sentry to: {checked}")
        try:
            config["sentry_enabled"] = checked
            with open(config_file_path, "w") as f:
                json.dump(config, f, indent=4)
            self.sync_sentry_ui(checked)
            
            if checked:
                print("Sentry enabled. Will take effect on next restart.")
            else:
                print("Sentry disabled. Will take effect on next restart.")
                
        except Exception as e:
            print(f"Error toggling Sentry: {e}")
            self.sync_sentry_ui(not checked)

    def stop_file_observer(self):
        # Stop file system observer
        global observer
        if observer:
            try:
                observer.stop()
                observer.join(timeout=2)
                print("Observer stopped.")
                observer = None
            except Exception as e:
                print(f"Error stopping observer: {e}")

    def start_file_observer(self):
        # Start file system observer for all mapped folders
        global observer, config
        self.stop_file_observer()
        
        observer = Observer()
        all_mappings = [("page", pm) for pm in config.get("page_mappings", [])] + \
                       [("database", dbm) for dbm in config.get("database_mappings", [])]
        
        if all_mappings:
            print("--- (Re)starting Watcher Setup ---")
            for mapping_type, mapping in all_mappings:
                if not mapping.get("enabled", True):
                    continue
                notion_id = mapping.get("notion_id")
                for folder_path in mapping.get("folders", []):
                    if not folder_path or not notion_id:
                        continue
                    path = os.path.expandvars(folder_path)
                    if os.path.isdir(path):
                        event_handler = NotionFileHandler(config, mapping, mapping_type, self)
                        recursive_watch = bool(mapping.get("folder_discovery", False))
                        observer.schedule(event_handler, path, recursive=recursive_watch)
                        mode = "recursive" if recursive_watch else "non-recursive"
                        print(f"--> Watching ({mode}): {path} -> {mapping_type} ID: ...{notion_id[-6:]}")
            if observer.emitters:
                observer.start()
                print("File watcher(s) started.")
        else:
            print("No folder mappings configured to watch.")

    def restart_file_observer(self):
        # Restart file observer in background thread
        threading.Thread(target=self.start_file_observer, daemon=True).start()

    def upload_folder_to_notion(self, folder_path, mapping_config, mapping_type):
        # Manual upload all files in folder to Notion
        print(f"Starting manual upload for folder: {folder_path}")
        global config, notified_errors

        if not folder_path or not os.path.isdir(folder_path):
            msg = f"Mapped folder path is missing or inaccessible: {folder_path}"
            print(msg)
            self.user_error_signal.emit(msg)
            self.show_notification("NotionLink: Folder Warning", msg, QSystemTrayIcon.Warning, 5000)
            return
        
        target_page_id = mapping_config.get("notion_id")
        notion_title = mapping_config.get("notion_title", "Unknown")
        if not target_page_id:
            print(f"Error: No Notion ID found for folder '{folder_path}'.")
            return
            
        print(f"Found mapping. Uploading files to {mapping_type} ...{target_page_id[-6:]}")
        try:
            notion = Client(auth=config.get("notion_token"))
            if mapping_type == "page":
                get_existing_links(target_page_id, notion, force_refresh=True)
                
                # Note: Don't warn about empty cache here - pages might legitimately be empty on first sync
                # If there's a real access issue, get_existing_links() will catch it and set cache to empty on API error
            
            files_uploaded_count = 0
            handler = NotionFileHandler(config, mapping_config, mapping_type, self)
            discover_subfolder_files = bool(mapping_config.get("folder_discovery", False))
            add_subfolder_links = bool(mapping_config.get("folder_links", False))

            if add_subfolder_links:
                for name in os.listdir(folder_path):
                    full_path = os.path.join(folder_path, name)
                    if os.path.isdir(full_path):
                        sync_file_to_notion(full_path, config, mapping_config, mapping_type, self, is_batch=True)
                        files_uploaded_count += 1
                        time.sleep(0.05)

            if discover_subfolder_files:
                file_paths = []
                for root, _, files in os.walk(folder_path):
                    for filename in files:
                        file_paths.append(os.path.join(root, filename))
            else:
                file_paths = []
                for filename in os.listdir(folder_path):
                    full_file_path = os.path.join(folder_path, filename)
                    if os.path.isfile(full_file_path):
                        file_paths.append(full_file_path)

            for full_file_path in file_paths:
                filename = os.path.basename(full_file_path)
                try:
                    ignore_exts = handler.mapping_config.get("ignore_extensions", [])
                    if any(fnmatch.fnmatch(filename, p) for p in ignore_exts):
                        print(f"Skipping (ext filter): {filename}")
                        continue
                    
                    ignore_files = handler.mapping_config.get("ignore_files", [])
                    if any(fnmatch.fnmatch(filename, p) for p in ignore_files):
                        print(f"Skipping (file/wildcard filter): {filename}")
                        continue
                except Exception as e:
                    print(f"Error applying filters: {e}")

                sync_file_to_notion(full_file_path, config, mapping_config, mapping_type, self, is_batch=True)
                files_uploaded_count += 1
                time.sleep(0.05)
                    
            print(f"Upload complete. {files_uploaded_count} files processed for {folder_path}.")
        except Exception as e:
            error_str = str(e).lower()
            error_key = f"{notion_title}:upload:{type(e).__name__}:{error_str[:50]}"
            
            if is_user_error(e):
                if error_key not in notified_errors:
                    notified_errors.add(error_key)
                    
                    if '404' in error_str or 'could not find' in error_str:
                        msg = f"Cannot access Notion page '{notion_title}'. Please ensure the page is shared with your integration."
                    elif '401' in error_str or 'unauthorized' in error_str or 'invalid token' in error_str:
                        msg = f"Invalid Notion token. Please update your token in settings."
                    elif '403' in error_str or 'forbidden' in error_str or 'not shared' in error_str:
                        msg = f"Access denied to '{notion_title}'. Check Notion page sharing permissions."
                    else:
                        msg = f"Configuration issue accessing '{notion_title}'. Please check your settings."
                    
                    print(f"ERROR: {msg}")
                    if self:
                        self.tray_icon.showMessage("NotionLink: Configuration Error", msg, QSystemTrayIcon.Warning, 5000)
                        self.user_error_signal.emit(msg)
                return
            else:
                if error_key not in notified_errors:
                    notified_errors.add(error_key)
                    
                    sentry_active = 'sentry_sdk' in globals() and sentry_sdk is not None
                    if sentry_active:
                        bug_msg = f"An unexpected error occurred during upload to '{notion_title}'. The problem has been logged and sent to the developer for fixing in the next version."
                    else:
                        bug_msg = f"An unexpected error occurred during upload to '{notion_title}'. The problem has been logged for review."
                    
                    print(f"An unexpected error occurred during upload: {e}")
                    if self:
                        self.tray_icon.showMessage("NotionLink: Application Error", bug_msg, QSystemTrayIcon.Critical, 5000)
                        self.user_error_signal.emit(bug_msg)
                raise e

    def show_window(self, window_name, window_class, **kwargs):
        # Generic window/dialog display handler
        print(f"Opening dialog: {window_name}")
        dialog = window_class(self, **kwargs) 
        print(f"Dialog created: {window_name}")
        result = dialog.exec()
        print(f"Closed dialog: {window_name} with result: {result}")
        
        if result == QDialog.Accepted:
            if window_name == "upload" and hasattr(dialog, 'selected_task') and dialog.selected_task:
                folder, mapping, m_type = dialog.selected_task
                print(f"Starting manual upload for: {folder}")
                threading.Thread(target=self.upload_folder_to_notion, args=(folder, mapping, m_type), daemon=True).start()
        
        print(f"Dialog {window_name} cleanup complete, continuing...")
        return result
    
    def show_dashboard(self, page_key="dashboard", notice_text=None):
        # Show main dashboard window
        print("show_dashboard called")
        if self.dashboard_window is None:
            print("Creating new dashboard window...")
            self.dashboard_window = MainDashboardWindow(self)

        self.dashboard_window.show()
        if page_key:
            if page_key == "dashboard":
                self.dashboard_window.navigate_to(page_key, push_history=False)
            else:
                self.dashboard_window.navigate_to(page_key, push_history=True)
        if notice_text:
            self.dashboard_window.update_status_panel_warning(notice_text)
        self.dashboard_window.activateWindow()
        self.dashboard_window.raise_()

    def _open_dashboard_from_signal(self, notice_text=""):
        self.show_dashboard("dashboard", notice_text=notice_text)

    def open_dashboard_from_handoff(self, notice_text):
        # Safe cross-thread handoff from HTTP server to UI thread.
        self.dashboard_open_signal.emit(notice_text)

    def on_dashboard_closed(self):
        # Handle dashboard window closure
        print("Dashboard window closed.")
        self.dashboard_window = None
    
    def show_feedback_dialog(self):
        # Open feedback page inside dashboard
        print("show_feedback_dialog called")
        self.show_dashboard("feedback")
        print("show_feedback_dialog finished")
    
    def show_convert_path(self):
        # Open convert page inside dashboard
        print("show_convert_path called")
        self.show_dashboard("convert")
        print("show_convert_path finished")

    def show_token(self):
        # Open token page inside dashboard
        print("show_token called")
        self.show_dashboard("token")
        print("show_token finished")

    def show_page_mappings(self):
        # Open page mappings inside dashboard
        print("show_page_mappings called")
        self.show_dashboard("page_mappings")
        print("show_page_mappings finished")

    def show_database_mappings(self):
        # Open database mappings inside dashboard
        print("show_database_mappings called")
        self.show_dashboard("database_mappings")
        print("show_database_mappings finished")

    def show_manual_upload(self):
        # Show manual upload dialog
        print("show_manual_upload called")
        self.show_window("upload", ManualUploadWindow)
        print("show_manual_upload finished")

    def minimize_to_tray(self):
        if self.dashboard_window:
            self.dashboard_window.hide()
        self.show_notification("NotionLink", "Minimized to tray.")

    def activate_offline_mode_manually(self):
        # Manually activate offline mode
        import src.core as core_module
        core_module.offline_mode = True
        self.on_offline_mode_activated()

    def trigger_offline_mode_ui(self):
        # Trigger offline mode UI update from background thread
        self.offline_mode_signal.emit()

    def on_offline_mode_activated(self):
        # Handle offline mode activation (runs in main thread)
        print("Offline mode activated - showing popup")
        
        # Update status UI
        self.update_status_ui("Notion: Offline Mode")
        if self.dashboard_window:
            self.dashboard_window.update_status_panel_warning("Offline Mode Active. Restart NotionLink to reconnect.")
            
        # Show popup
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Warning)
        msg.setWindowTitle("NotionLink - Offline Mode")
        msg.setWindowIcon(QIcon(TRAY_ICON_ICO))
        msg.setText("Offline Mode Activated")
        msg.setInformativeText(
            "NotionLink could not connect to Notion after retrying.\n\n"
            "• Existing links will continue to work locally as long as NotionLink is running.\n"
            "• New files will NOT be synced automatically.\n"
            "• To add new links, use 'Convert Path to Link' and paste them manually into Notion.\n"
            "• These links are hosted locally and are only available on this computer."
        )
        
        # Add buttons
        retry_btn = msg.addButton("Keep Retrying", QMessageBox.ActionRole)
        stay_offline_btn = msg.addButton("Stay Offline", QMessageBox.ActionRole)
        msg.setDefaultButton(stay_offline_btn)
        
        msg.exec()
        
        if msg.clickedButton() == retry_btn:
            self.start_auto_retry_loop()

    def quit_app(self):
        # Graceful application shutdown
        global observer, httpd
        print("=== QUIT_APP CALLED - Shutting down... ===")

        if not config.get("skip_close_confirm", False):
            confirm_dialog = QMessageBox()
            confirm_dialog.setIcon(QMessageBox.Question)
            confirm_dialog.setWindowTitle("Close NotionLink")
            confirm_dialog.setWindowIcon(QIcon(TRAY_ICON_ICO))
            confirm_dialog.setText("Close NotionLink?")
            confirm_dialog.setInformativeText("NotionLink will stop syncing while closed.")
            do_not_remind = QCheckBox("Don't remind me again")
            confirm_dialog.setCheckBox(do_not_remind)
            confirm_dialog.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
            confirm_dialog.setDefaultButton(QMessageBox.No)
            if confirm_dialog.exec() != QMessageBox.Yes:
                return
            if do_not_remind.isChecked():
                config["skip_close_confirm"] = True
                try:
                    with open(config_file_path, "w") as cfg_file:
                        json.dump(config, cfg_file, indent=4)
                except Exception as e:
                    print(f"Failed to persist close-confirm preference: {e}")
        
        if self.dashboard_window:
            self.dashboard_window.close()
            
        if hasattr(self, 'status_check_timer') and self.status_check_timer:
            self.status_check_timer.stop()
            print("Status check timer stopped.")
        
        if hasattr(self, 'notification_timer') and self.notification_timer:
            self.notification_timer.stop()
            print("Notification batch timer stopped.")
            
        self.stop_file_observer()
        
        if httpd:
            try:
                httpd.shutdown()
                httpd.server_close()
                print("HTTP server stopped and socket closed.")
            except Exception as e:
                print(f"Error stopping server: {e}")
        
        if hasattr(self, 'tray_icon'):
            self.tray_icon.hide()
            print("Tray icon hidden.")
        
        try:
            import logging
            logging.shutdown()
            print("Log handlers shut down.")
        except Exception:
            pass
            
        print("Calling app.quit()...")
        self.app.quit()
        print("=== PySide6 App quit complete ===")
