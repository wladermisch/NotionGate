# NotionLink - UI Styles
# Copyright (c) 2025 wladermisch. All Rights Reserved.
#
# Dark theme stylesheet for all UI components

DARK_STYLESHEET = """
    QMenu {
        background-color: #2b2b2b;
        color: #ffffff;
        border: 1px solid #555555;
        padding: 5px;
    }
    QMenu::item {
        padding: 5px 25px 5px 20px;
    }
    QMenu::item:disabled {
        color: #888888;
    }
    QMenu::item:selected {
        background-color: #0078d7;
        color: #ffffff;
    }
    QMenu::separator {
        height: 1px;
        background-color: #555555;
        margin-top: 5px;
        margin-bottom: 5px;
    }
    QDialog {
        background-color: #2b2b2b;
        color: #ffffff;
    }
    QWidget#Dashboard {
        background-color: #2b2b2b;
    }
    QWidget#RightColumn {
        background-color: #3c3c3c;
        border-radius: 4px;
    }
    QLabel {
        color: #ffffff;
    }
    QLabel#h2 {
        font-size: 14px;
        font-weight: bold;
        color: #ffffff;
        margin-top: 10px;
        margin-bottom: 5px;
    }
    QLabel#buildInfo {
        font-size: 10px;
        color: #bbbbbb;
        padding: 5px;
    }
    QLabel#hintLabel {
        font-size: 10px;
        color: #bbbbbb;
    }
    QLabel#ServerErrorLabel {
        color: #ffc107; 
        font-weight: bold;
        background-color: #443300;
        border: 1px solid #ffc107;
        padding: 5px;
        border-radius: 4px;
    }
    QLabel#StatusPanelOK {
        color: #28a745;
        font-weight: bold;
        background-color: #1a3d1a;
        border: 1px solid #28a745;
        padding: 10px;
        border-radius: 4px;
        font-size: 13px;
    }
    QLabel#StatusPanelWarning {
        color: #ffc107;
        font-weight: bold;
        background-color: #443300;
        border: 1px solid #ffc107;
        padding: 10px;
        border-radius: 4px;
        font-size: 13px;
    }
    QLabel#StatusPanelError {
        color: #dc3545;
        font-weight: bold;
        background-color: #3d1a1a;
        border: 1px solid #dc3545;
        padding: 10px;
        border-radius: 4px;
        font-size: 13px;
    }
    QLineEdit, QTextEdit {
        background-color: #3c3c3c;
        color: #ffffff;
        border: 1px solid #555555;
        border-radius: 4px;
        padding: 5px;
    }
    QTextEdit#LogDisplay {
        font-family: Consolas, 'Courier New', monospace;
        color: #cccccc;
        background-color: #2b2b2b;
        border: 1px solid #444444;
    }
    QPushButton {
        background-color: #228B22;
        color: #ffffff;
        border: none;
        border-radius: 4px;
        padding: 8px 12px;
    }
    QPushButton:hover {
        background-color: #2E8B57;
    }
    QPushButton:pressed {
        background-color: #1E5631;
    }
    QPushButton#secondaryButton {
        background-color: #3c3c3c;
        border: 1px solid #555555;
    }
    QPushButton#secondaryButton:hover {
        background-color: #4a4a4a;
    }
    QPushButton#secondaryButton:pressed {
        background-color: #5a5a5a;
    }
    QPushButton#dangerButton {
        background-color: #992222;
        border: 1px solid #553333;
    }
    QPushButton#dangerButton:hover {
        background-color: #aa3333;
    }
    QCheckBox {
        color: #ffffff;
        padding: 5px;
    }
    QCheckBox::indicator {
        width: 13px;
        height: 13px;
        border: 1px solid #555555;
        border-radius: 3px;
        background-color: #3c3c3c;
    }
    QCheckBox::indicator:checked {
        background-color: #228B22;
        border: 1px solid #2E8B57;
    }
    QCheckBox:disabled {
        color: #888888;
    }
    QCheckBox::indicator:disabled {
        background-color: #4a4a4a;
        border: 1px solid #555555;
    }
    QCheckBox::indicator:checked:disabled {
        background-color: #2E8B57;
        border: 1px solid #444444;
    }
    QScrollArea, QListWidget {
        background-color: #3c3c3c;
        border: 1px solid #555555;
        color: #ffffff;
    }
    QListWidget::item:selected {
        background-color: #0078d7;
        color: #ffffff;
    }
    QFrame {
        border: none;
    }
"""
