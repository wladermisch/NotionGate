[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/E1E21NYEKB)
# NotionLink 🔗

![NotionLink Tray Menu](https://gdurl.com/gYfT)

**NotionLink** is a Windows tray utility that bridges your local file system with Notion. It creates clickable `http://localhost` links for your local files, allowing you to open them directly from your Notion pages.

---

## 💾 Installation & Setup

### For Users (Recommended, Binary)

1.  Download the latest `NotionLink_Installer_gvX.X.exe` from the **[Releases Page](https://github.com/wladermisch/NotionLink/releases)**.
2.  Follow the instructions from the installer, select your desired program location and settings.
3.  Run `NotionLink` from Windows Search. The app will create `config.json` and log files (`notionlink.log`, `error.log`) in the same folder.
4.  Follow the **First-Time Setup** steps below.

### For Developers (from Source)

1.  Clone this repository: `git clone https://github.com/wladermisch/NotionLink.git`
2.  Create a virtual environment: `python -m venv .venv`
3.  Activate it: `.venv\Scripts\activate`
4.  Install dependencies:
    ```bash
    pip install PySide6 Pillow pyperclip pyautogui notion-client watchdog pywin32 sentry-sdk
    ```
5.  Run the script: `python NotionLink.pyw`

---

## ⚙️ How to Use

### 1. First-Time Setup (Wizard)

When you first launch the app, a welcome wizard will appear.

1.  **Create Token:** The app needs an "Internal Integration Token" to talk to your Notion account.
    * Click the button to open the [Notion Integrations page](https://www.notion.so/my-integrations).
    * Create a "New integration" (e.g., name it "NotionLink").
    * Copy the "Internal Integration Token" (Secret).
2.  **Paste Token:** Paste the secret token into the input field.
3.  **Share:** Go to the Notion page(s) you want to use, click "..." (Top right) -> "Add connections" -> and select your "NotionLink" integration. **This step is mandatory!**
4.  **Autostart:** Check the box if you want the app to start with Windows.
5.  **Error Reporting:** Leave the checkbox enabled to help us fix bugs anonymously.
6.  Click "Save and Start". The app will save and launch in your system tray.

> **Security Warning:**
> Keep your secret token hidden! Never upload your `config.json` file or post your token in issues or any other public place. If you fear your token is compromised, visit the integrations page again to renew it. The app will never write your token to the log files.

### 2. The Tray Menu (Right-Click)

Once running, find the NotionLink icon in your Windows tray (it may be in the `^` overflow menu). A **Right-Click** opens the main menu:

* **Notion: [Status]:** Shows your connection status (Green, Red, Gray). Clicking this forces a re-check.
* **Convert Path:** Opens a dialog to convert a Windows path into a `localhost` link.
* **Convert Clipboard Path:** Instantly converts the path in your clipboard and copies the new link.
* **Notion: Start Manual Upload:** Lets you select a mapped folder to trigger a manual "backfill" of all its files.
* **Manage Folder-Page Mappings:** The heart of the app. Define *which* folder syncs to *which* page here.
* **Manage Notion Token:** Update your token if it ever changes.
* **Start with Windows:** Toggles the autostart feature on or off.
* **Quit:** Shuts down the application.

### 3. Managing Mappings (Setting up Auto-Sync)

To make the app work automatically, you must set up mappings:

1.  Right-click the tray icon -> **Manage Folder-Page Mappings**.
2.  Click **Add New Mapping (Folder + Page)**.
3.  **Step 1:** Select the **local folder** you want to monitor.
4.  **Step 2:** Paste the **Notion page link** where the file links should appear.
5.  Click "Save".
6.  **IMPORTANT:** You must **Restart the application** after saving new mappings. This is required for the file watcher (`watchdog`) to start monitoring the new folders.

### 4. Troubleshooting & Submitting Bug Reports

If you encounter a problem, you can find detailed logs in the same folder as your `NotionLink.exe`.

* `notionlink.log`: Contains general information and a history of app activity (e.g., "File synced", "Server started").
* `error.log`: Contains details about any errors or crashes that have occurred.

If you open a new [GitHub Issue](https://github.com/wladermisch/NotionLink/issues) to report a bug, **please manually attach the `error.log` file.**

> **Privacy Note:** The `error.log` file *does* contain some system information (like your username, hostname, and machine ID) to help with debugging. This file **never** leaves your computer automatically. By manually attaching it to a GitHub issue, you are giving your consent to share this debugging information.

---

## [Changelog](https://wladermisch.github.io/NotionLink/resources/changelog.html)

**You can access the changelog on the [github page](https://wladermisch.github.io/NotionLink/resources/changelog.html).**

## 📋 Features

* **Runs Silently in the Tray:** Stays out of your way and is ready when you need it.
* **Automatic Folder Sync:** Map a local folder to a Notion page. Any *new* file you save in that folder is automatically added as a link to that page.
* **Startup & Backfill Sync:** On start (or when adding a new mapping), the app syncs *all* existing files in the folder to the Notion page.
* **Smart Deduplication:** The app checks for existing URLs *and* filenames to prevent uploading duplicates.
* **Starts with Windows:** A built-in option (in setup and the menu) to launch the app on boot.
* **Simple Setup Wizard:** A one-time setup dialog guides you through creating your Notion token.
* **Dark Mode GUI:** A modern, dark-themed UI for managing your token and folder-page mappings.
* **Link Converter Tools:** Menu options to quickly convert a path or your clipboard content into a `localhost` link.
* **Reliable Tray Menu:** A standard, stable context menu.

---

## 🚀 The Problem

Notion is fantastic for organization, but it has one major limitation: it cannot link directly to local files (like `C:\Users\YourName\Documents\Project.pdf`).

Your options were:
1.  **Upload to Notion:** This uses storage space, creates duplicates, and is impractical for large files or entire project folders.
2.  **Edited files not synced:** Made some changes to your PDF? Because you uploaded the file into Notion, you won't be able to see the change.
3.  **Use `file://` links:** These are often unclickable in Notion, forcing you to copy-paste the link into your browser or file explorer.

## ✨ The Solution

NotionLink solves this by running a tiny, local HTTP server on your PC.

1.  It converts a path like `C:\Projects\File.pdf` into a link: `http://localhost:3030/C:/Projects/File.pdf`.
2.  When you click this link in Notion, NotionLink intercepts the request.
3.  Instead of serving a webpage, it simply opens `C:\Projects\File.pdf` directly with Windows Explorer.

Best of all, it can monitor entire folders and automatically add links for new files to a specific Notion page.

---

## 🐞 Error Reporting & Privacy

**This is very important to us.** To find and fix bugs, NotionLink uses an automated error reporting service called **Sentry**.
for NotionLink >v3
* **What it does:** If the application crashes unexpectedly, Sentry automatically sends us a anonymous report about the crash (e.g., "Error in line X"). This helps us fix bugs in future versions.
* **What about Privacy?** We do **not** send or collect any of your personal data. The automated reports **do not include** your Notion Token, your file paths, your IP address, or any other personally identifiable information.
* **What about Performance?** It does not affect the app's performance. It only runs for a split second if an error occurs and is otherwise completely inactive.
* **Can I disable it?** Yes, you can disable this anonymous reporting at any time. You can uncheck the box during the initial setup, or later by setting `"sentry_enabled": false` in your `config.json` file.

---

## 🛠️ Technical Overview

* **Backend:** All background processes (HTTP server, Watchdog file observer, startup syncs) run in stable, separate standard Python `threading.Thread`s.
* **Frontend:** The UI (dialogs, tray menu) is built entirely with **PySide6**.
* **Error Reporting:** Uncaught exceptions are automatically and **anonymously** sent to **Sentry** (if enabled) to help with debugging.
* **Logging:** All application output is redirected to a robust `logging` system, writing to `notionlink.log` (for info) and `error.log` (for detailed errors, including PII).
* **Configuration:** All settings, including your token and mappings, are stored locally in a `config.json` file.

---

## 📜 License & Contributions

This project is **Source-Available**, not Open Source. All rights are reserved by the author.

* **You MAY...** download, compile, and use the software for personal, non-commercial purposes.
* **You MAY...** fork this repository to submit improvement suggestions (Pull Requests) to this project.
* **You MAY NOT...** redistribute, sell, or use this code (in whole or in part) in another project (commercial or non-commercial) without the author's express written permission.

For all licensing or contribution inquiries, please open a GitHub Issue.
See the full [LICENSE.md](LICENSE.md) file for details.
