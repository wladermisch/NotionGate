import re
import time
import threading
import random
from urllib.parse import urlparse, quote, unquote
from pathlib import Path
from datetime import datetime
from notion_client import Client
import os
import fnmatch
from PySide6.QtWidgets import QSystemTrayIcon

from .core import (
    config, link_cache, notified_errors, file_to_page_map,
    logger, is_user_error, sentry_sdk, NETWORK_ERROR_STRINGS
)
import src.core as core_module  # Import core module to access offline_mode flag


def retry_api_call(func, max_retries=3, backoff_factor=2):
    last_exception = None
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            last_exception = e
            error_str = str(e).lower()
            # Check for timeout or connection errors
            if any(x in error_str for x in NETWORK_ERROR_STRINGS):
                if attempt < max_retries - 1:
                    sleep_time = (backoff_factor ** attempt) + random.uniform(0, 1)
                    print(f"Network error ({type(e).__name__}), retrying in {sleep_time:.1f}s... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(sleep_time)
                    continue
            raise e
    raise last_exception


def get_notion_title(notion_id, token, is_db=False):
    try:
        notion = Client(auth=token)
        if is_db:
            response = notion.databases.retrieve(database_id=notion_id)
            title_array = response.get("title", [])
            if title_array and len(title_array) > 0:
                return title_array[0].get("plain_text") or title_array[0].get("text", {}).get("content")
        else:
            response = notion.pages.retrieve(page_id=notion_id)
            properties = response.get("properties", {})
            
            for prop_name, prop_value in properties.items():
                if prop_value.get("type") == "title":
                    title_array = prop_value.get("title", [])
                    if title_array and len(title_array) > 0:
                        return title_array[0].get("plain_text") or title_array[0].get("text", {}).get("content")
            
            for common_name in ["title", "Title", "Name", "name"]:
                if common_name in properties:
                    title_prop = properties[common_name]
                    if title_prop.get("type") == "title":
                        title_array = title_prop.get("title", [])
                        if title_array and len(title_array) > 0:
                            return title_array[0].get("plain_text") or title_array[0].get("text", {}).get("content")
        
        return None
    except Exception as e:
        print(f"API Error: Could not fetch title for {notion_id}: {e}")
        return None


def extract_id_and_title_from_link(text_input):
    if not text_input:
        return None
    
    id_match = re.search(r'([a-fA-F0-9]{32})', text_input)
    if not id_match:
        print(f"Could not extract ID from: {text_input}")
        return None
        
    notion_id = id_match.group(1)
    
    title_from_url = None
    if "notion.so" in text_input:
        try:
            path_part = urlparse(text_input).path
            slug = path_part.split('/')[-1]
            if '-' in slug:
                title_parts = slug.split('-')
                if title_parts[-1] == notion_id:
                    title_from_url = " ".join(title_parts[:-1])
        except Exception:
            pass
            
    print(f"Extracted ID: {notion_id}, Title from URL: {title_from_url}")
    return notion_id, title_from_url


def get_existing_links(page_id, notion_client, force_refresh=False):
    # Fetch and cache all existing links from a Notion page.
    global link_cache
    
    if not force_refresh and page_id in link_cache:
        print(f"Cache hit for page ...{page_id[-6:]}.")
        return

    transaction = None
    if sentry_sdk is not None:
        transaction = sentry_sdk.start_transaction(
            op="function",
            name="get_existing_links",
            description="Fetch all blocks from a Notion page."
        )

    print(f"Fetching existing content for page ...{page_id[-6:]} (Force Refresh: {force_refresh})...")
    
    # Start with existing cache data if available, otherwise create new
    if page_id in link_cache:
        links_data = {
            "urls": link_cache[page_id]["urls"].copy(),
            "filenames": link_cache[page_id]["filenames"].copy(),
            "blocks": link_cache[page_id].get("blocks", {}).copy(),
            "url_to_block": link_cache[page_id].get("url_to_block", {}).copy()
        }
        print(f"Merging with existing cache ({len(links_data['urls'])} URLs, {len(links_data['filenames'])} filenames)...")
    else:
        links_data = {"urls": set(), "filenames": set(), "blocks": {}, "url_to_block": {}}
    
    try:
        next_cursor = None
        while True:
            def _fetch_blocks():
                return notion_client.blocks.children.list(block_id=page_id, start_cursor=next_cursor)
            
            response = retry_api_call(_fetch_blocks)
            results = response.get("results", [])
        
            for block in results:
                block_type = block.get("type")
                if block_type in ("paragraph", "heading_1", "heading_2", "heading_3", "bulleted_list_item", "numbered_list_item"):
                    rich_text = block.get(block_type, {}).get("rich_text", [])
                    for item in rich_text:
                        text_content = item.get("text", {}).get("content", "")
                        link = item.get("text", {}).get("link")
                        if link and link.get("url"):
                            url = link["url"]
                            links_data["urls"].add(url)
                            links_data["url_to_block"][url] = block["id"]
                            if text_content:
                                links_data["filenames"].add(text_content)
                                links_data["blocks"][text_content] = block["id"]
            if response.get("has_more"):
                next_cursor = response.get("next_cursor")
            else:
                break
        link_cache[page_id] = links_data
        print(f"Cached {len(links_data['urls'])} URLs and {len(links_data['filenames'])} filenames for page ...{page_id[-6:]}.")
    except Exception as e:
        if is_user_error(e):
            print(f"WARNING: Cannot access page ...{page_id[-6:]} - {e}")
            print("Please check that the page is shared with your Notion integration.")
            if transaction:
                transaction.set_status("not_found")
            link_cache[page_id] = {"urls": set(), "filenames": set()}
            return
        
        print(f"Error fetching existing content: {e}")
        if transaction:
            transaction.set_status("internal_error")
        raise e
    finally:
        if transaction:
            transaction.finish()


def _sync_to_page(notion, notion_id, filename, server_link, force_refresh):
    get_existing_links(notion_id, notion, force_refresh=force_refresh)
    
    cached_data = link_cache.get(notion_id, {"urls": set(), "filenames": set(), "blocks": {}, "url_to_block": {}})
    if filename in cached_data["filenames"] or server_link in cached_data["urls"]:
        print(f"Skipping (already exists on page): {filename}")
        return False
    
    print(f"Sending file to Page ...{notion_id[-6:]}: {filename}")
    blocks_to_append = [{
        "object": "block",
        "type": "paragraph",
        "paragraph": {
            "rich_text": [{
                "type": "text",
                "text": {
                    "content": filename,
                    "link": {"url": server_link}
                }
            }]
        }
    }]
    
    def _append_block():
        response = notion.blocks.children.append(block_id=notion_id, children=blocks_to_append)
        # Cache the new block ID
        if response and "results" in response and len(response["results"]) > 0:
            new_block_id = response["results"][0]["id"]
            cached_data["blocks"][filename] = new_block_id
            cached_data["url_to_block"][server_link] = new_block_id
    
    retry_api_call(_append_block)
    
    cached_data["urls"].add(server_link)
    cached_data["filenames"].add(filename)
    link_cache[notion_id] = cached_data
    return True


def _sync_to_database(notion, notion_id, filename, server_link, full_file_path, mapping_config):
    print(f"Sending file to Database ...{notion_id[-6:]}: {filename}")
    
    file_stat = Path(full_file_path).stat()
    created_iso = datetime.fromtimestamp(file_stat.st_ctime).isoformat()
    modified_iso = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
    size_bytes = file_stat.st_size
    
    properties = {
        "Name": {"title": [{"text": {"content": filename}}]},
        "Link": {"url": server_link},
        "Created": {"date": {"start": created_iso}},
        "Modified": {"date": {"start": modified_iso}},
        "Size (Bytes)": {"number": size_bytes}
    }
    
    def _create_page():
        return notion.pages.create(parent={"database_id": notion_id}, properties=properties)
    
    response = retry_api_call(_create_page)
    
    if mapping_config.get("full_lifecycle_sync", True):
        global file_to_page_map
        page_id = response["id"]
        file_to_page_map[full_file_path] = {
            "page_id": page_id,
            "database_id": notion_id,
            "filename": filename
        }
    return True


def _attempt_connection_recovery(tray_app):
    # Global connection recovery loop.
    # Retries connection 3 times with 5s delay.
    # If successful, processes pending uploads.
    # If failed, activates offline mode.
    print("Starting global connection recovery loop...")
    
    # Notify user via tray status (but no popup yet)
    if tray_app:
        tray_app.status_updated.emit("Notion: Retrying Connection...")

    for attempt in range(1, 4):
        print(f"Connection retry attempt {attempt}/3 in 5 seconds...")
        time.sleep(5)
        
        # Check connection
        connection_status = {"status": "Unknown"}
        def _status_cb(status):
            connection_status["status"] = status
            
        # Force check
        check_notion_status_once(_status_cb, force=True)
        
        if connection_status["status"] == "Notion: Connected":
            print("Connection restored! Processing pending uploads...")
            core_module.is_recovering_connection = False
            
            if tray_app:
                tray_app.status_updated.emit("Notion: Connected")
                tray_app.tray_icon.showMessage("NotionLink", "Internet connection restored.", QSystemTrayIcon.Information, 3000)
            
            # Process pending uploads
            process_pending_uploads()
            return

    # If we get here, all retries failed
    print("All connection retries failed. Activating Offline Mode.")
    core_module.is_recovering_connection = False
    core_module.offline_mode = True
    if tray_app:
        tray_app.trigger_offline_mode_ui()


def process_pending_uploads():
    # Process all tasks in the pending uploads queue.
    with core_module.pending_uploads_lock:
        if not core_module.pending_uploads:
            return
        pending_tasks = list(core_module.pending_uploads)
        core_module.pending_uploads.clear()
    
    print(f"Processing {len(pending_tasks)} pending uploads...")
    for task_args in pending_tasks:
        try:
            threading.Thread(target=sync_file_to_notion, args=task_args, daemon=True).start()
        except Exception as e:
            print(f"Error restarting pending task: {e}")


def sync_file_to_notion(full_file_path, config_data, mapping_config, mapping_type, tray_app, is_batch=True, retry_count=0, notion_client=None, force_refresh=True):
    # Sync a file to Notion (page or database).
    global link_cache, notified_errors
    
    if core_module.offline_mode:
        print(f"Skipping sync for {os.path.basename(full_file_path)} (Offline Mode active)")
        return False

    # If we are currently recovering connection, queue this task and return
    if core_module.is_recovering_connection:
        print(f"Connection recovery in progress. Queuing {os.path.basename(full_file_path)}...")
        with core_module.pending_uploads_lock:
            core_module.pending_uploads.append((full_file_path, config_data, mapping_config, mapping_type, tray_app, is_batch))
        return False

    from .core import notification_batch  # Import here to avoid circular dependency
    
    transaction = None
    if sentry_sdk is not None:
        transaction = sentry_sdk.start_transaction(
            op="function",
            name="sync_file_to_notion",
            description=f"Sync file to {mapping_type}"
        )
    
    filename = os.path.basename(full_file_path)
    notion_id = mapping_config["notion_id"]
    notion_title = mapping_config["notion_title"]

    try:
        notion_token = config_data.get("notion_token")
        server_host = config_data.get("server_host")
        port = config_data.get("server_port")
        if not notion_token or "EINFUEGEN" in notion_token or "PLEASE_ENTER" in notion_token:
            print("Notion Token not configured. Skipping upload.")
            return False
            
        server_address = f"{server_host}:{port}/"
        url_path = full_file_path.replace("\\", "/")
        if url_path.startswith('/'):
            url_path = url_path[1:]
        
        # Encode path to handle spaces and special characters
        url_path = quote(url_path, safe='/')
        server_link = server_address + url_path
        
        # Reuse client if provided, else create new
        notion = notion_client if notion_client else Client(auth=notion_token)
        
        success = False
        if mapping_type == "page":
            success = _sync_to_page(notion, notion_id, filename, server_link, force_refresh)
        elif mapping_type == "database":
            success = _sync_to_database(notion, notion_id, filename, server_link, full_file_path, mapping_config)

        if success:
            print(f"Successfully uploaded {filename} to {mapping_type} '{notion_title}'.")
            if tray_app:
                if is_batch:
                    notification_batch[notion_title].append(filename)
                    if hasattr(tray_app, 'reset_notification_timer'):
                        tray_app.reset_notification_timer()
                else:
                    tray_app.tray_icon.showMessage(
                        "NotionLink: Sync Success",
                        f"'{filename}' was added to {notion_title}.",
                        QSystemTrayIcon.Information,
                        3000
                    )
                # Signal success to clear any previous error states in UI
                if hasattr(tray_app, 'op_success_signal'):
                    tray_app.op_success_signal.emit()
        return success

    except Exception as e:
        error_str = str(e).lower()
        print(f"Error sending file {full_file_path} to Notion: {e}")
        
        error_key = f"{notion_id}:{type(e).__name__}:{error_str[:50]}"
        
        # Check for network errors explicitly first
        is_network_error = any(x in error_str for x in NETWORK_ERROR_STRINGS)
        
        if is_user_error(e) or is_network_error:
            if error_key not in notified_errors or is_network_error:
                if not is_network_error:
                    notified_errors.add(error_key)
                
                if '404' in error_str or 'could not find' in error_str:
                    user_msg = f"Cannot access Notion page '{notion_title}'. Please ensure the page is shared with your integration."
                elif '401' in error_str or 'unauthorized' in error_str or 'invalid token' in error_str:
                    user_msg = f"Invalid Notion token. Please update your token in settings."
                elif '403' in error_str or 'forbidden' in error_str:
                    user_msg = f"Access denied to '{notion_title}'. Check page sharing permissions."
                elif is_network_error:
                    user_msg = f"Network error connecting to Notion. Queuing for retry..."
                    
                    # Queue the task for retry
                    print(f"Network error detected. Adding {filename} to pending uploads.")
                    with core_module.pending_uploads_lock:
                        core_module.pending_uploads.append((full_file_path, config_data, mapping_config, mapping_type, tray_app, is_batch))
                    
                        # Start global recovery if not already running
                        if not core_module.is_recovering_connection:
                            core_module.is_recovering_connection = True
                            threading.Thread(target=_attempt_connection_recovery, args=(tray_app,), daemon=True).start()
                    
                    # Update status UI (throttled)
                    if tray_app:
                        current_time = time.time()
                        if current_time - core_module.last_network_notification_time > 60:
                            tray_app.status_updated.emit("Notion: Connection Error")
                            tray_app.tray_icon.showMessage("NotionLink: Connection Error", "Network error. Retrying in background...", QSystemTrayIcon.Warning, 5000)
                            core_module.last_network_notification_time = current_time
                        else:
                            tray_app.status_updated.emit("Notion: Connection Error")
                    
                    return False
                else:
                    user_msg = f"Configuration issue with '{notion_title}'. Check settings and permissions."
                
                if tray_app and 'Network error' not in user_msg:
                    tray_app.tray_icon.showMessage("NotionLink: Configuration Error", user_msg, QSystemTrayIcon.Warning, 5000)
                    tray_app.user_error_signal.emit(user_msg)
            
            if transaction:
                transaction.set_status("permission_denied")
            return False
        else:
            if error_key not in notified_errors:
                notified_errors.add(error_key)
                
                sentry_active = sentry_sdk is not None
                if sentry_active:
                    bug_msg = f"An unexpected error occurred. The problem has been logged and sent to the developer for fixing in the next version."
                else:
                    bug_msg = f"An unexpected error occurred. The problem has been logged for review."
                
                if tray_app:
                    tray_app.tray_icon.showMessage("NotionLink: Application Error", bug_msg, QSystemTrayIcon.Critical, 5000)
                    tray_app.user_error_signal.emit(f"Application error with '{filename}': {bug_msg}")
            
            if transaction:
                transaction.set_status("internal_error")
            raise e
    finally:
        if transaction:
            transaction.finish()


def find_notion_page_by_filename(database_id, filename, notion_token):
    # Find a page in a database by filename.
    try:
        notion = Client(auth=notion_token)
        response = notion.databases.query(
            database_id=database_id,
            filter={
                "property": "Name",
                "title": {
                    "equals": filename
                }
            }
        )
        
        results = response.get("results", [])
        if results:
            return results[0]["id"]
        return None
    except Exception as e:
        print(f"Error finding page for {filename}: {e}")
        return None


def archive_notion_page(page_id, notion_token, filename, tray_app=None):
    # Archive a Notion page (soft delete).
    try:
        notion = Client(auth=notion_token)
        notion.pages.update(page_id=page_id, archived=True)
        print(f"Archived Notion page for deleted file: {filename}")
        
        if tray_app:
            tray_app.tray_icon.showMessage(
                "NotionLink: File Deleted",
                f"'{filename}' was removed from Notion (archived).",
                QSystemTrayIcon.Information,
                3000
            )
        return True
    except Exception as e:
        print(f"Error archiving page for {filename}: {e}")
        return False


def update_notion_page_filename(page_id, new_filename, new_link, notion_token, old_filename, tray_app=None):
    # Update a Notion page with new filename and link.
    try:
        notion = Client(auth=notion_token)
        
        # Extract file path from server link and get stats
        file_path_parts = new_link.split("//", 1)[1].split("/", 1)
        if len(file_path_parts) > 1:
            # Decode URL-encoded path before using it
            file_path = unquote(file_path_parts[1]).replace("/", "\\")
            file_stat = Path(file_path).stat()
            modified_iso = datetime.fromtimestamp(file_stat.st_mtime).isoformat()
        else:
            modified_iso = datetime.now().isoformat()
        
        properties = {
            "Name": {"title": [{"text": {"content": new_filename}}]},
            "Link": {"url": new_link},
            "Modified": {"date": {"start": modified_iso}}
        }
        
        notion.pages.update(page_id=page_id, properties=properties)
        print(f"Updated Notion page: {old_filename} → {new_filename}")
        
        if tray_app:
            tray_app.tray_icon.showMessage(
                "NotionLink: File Renamed",
                f"'{old_filename}' → '{new_filename}' updated in Notion.",
                QSystemTrayIcon.Information,
                3000
            )
        return True
    except Exception as e:
        print(f"Error updating page for {old_filename}: {e}")
        return False


def check_notion_status_once(status_callback, force=False):
    # Check if Notion API is accessible with current token.
    if core_module.offline_mode and not force:
        status_callback("Notion: Offline Mode")
        return

    try:
        token = config.get("notion_token")
        if not token or "PLEASE_ENTER" in token:
            status_callback("Notion: No Token")
        else:
            notion = Client(auth=token)
            notion.users.me()
            status_callback("Notion: Connected")
    except Exception as e:
        error_str = str(e).lower()
        print(f"Notion connection check failed: {e}")
        
        if '401' in error_str or 'unauthorized' in error_str:
            status_callback("Notion: Invalid Token")
        elif '403' in error_str or 'forbidden' in error_str:
            status_callback("Notion: Access Denied")
        elif any(x in error_str for x in ['timeout', 'timed out', 'connection', 'handshake', 'getaddrinfo', 'host', 'socket', 'client', 'remote', '10065', '10054', '10060', '10061', '11001']):
            status_callback("Notion: Connection Error")
        else:
            status_callback("Notion: Disconnected")


def run_startup_sync(tray_app):
    # Run initial sync of all mapped folders on application startup.
    page_mappings = config.get("page_mappings", [])
    db_mappings = config.get("database_mappings", [])
    if not page_mappings and not db_mappings:
        return
    
    transaction = None
    if sentry_sdk is not None:
        transaction = sentry_sdk.start_transaction(
            op="function",
            name="run_startup_sync",
            description="Run full sync on app start."
        )
    
    print("--- Starting Initial Sync (Background) ---")
    try:
        startup_notion_client = Client(auth=config.get("notion_token"))
        
        # Prime cache in parallel for faster startup
        print("Priming link cache on startup (for Page mappings)...")
        cache_threads = []
        page_ids_to_prime = set()
        for mapping in page_mappings:
            target_page_id = mapping.get("notion_id")
            if target_page_id and target_page_id not in page_ids_to_prime:
                page_ids_to_prime.add(target_page_id)
                t = threading.Thread(
                    target=get_existing_links,
                    args=(target_page_id, startup_notion_client, True),
                    daemon=True
                )
                cache_threads.append(t)
                t.start()
    
        # Wait max 0.5s per thread to avoid blocking UI
        for t in cache_threads:
            t.join(timeout=0.5)
        print("Cache priming started (running in background)...")
        
    except Exception as e:
        if is_user_error(e):
            print(f"Startup sync skipped due to configuration issue: {e}")
            if tray_app:
                tray_app.user_error_signal.emit(f"Startup sync failed: {e}")
            if transaction:
                transaction.set_status("permission_denied")
                transaction.finish()
            return
        else:
            print(f"Could not create Notion client for startup sync: {e}")
            if transaction:
                transaction.set_status("internal_error")
            raise e
    
    all_mappings = [("page", pm) for pm in page_mappings] + [("database", dbm) for dbm in db_mappings]
    
    print(f"Starting initial file sync for {len(all_mappings)} mapping(s)...")
    for mapping_type, mapping in all_mappings:
        for folder_path in mapping.get("folders", []):
            if folder_path and os.path.isdir(folder_path):
                print(f"--> Queuing startup sync for: {folder_path}")
                sync_thread = threading.Thread(
                    target=tray_app.upload_folder_to_notion,
                    args=(folder_path, mapping, mapping_type),
                    daemon=True
                )
                sync_thread.start()
            else:
                warning_msg = f"Mapped folder path is missing or inaccessible: {folder_path}"
                print(f"--> Skipping startup sync for invalid path: {folder_path}")
                if tray_app:
                    tray_app.user_error_signal.emit(warning_msg)
            
    if transaction:
        transaction.finish()


def remove_file_from_page(page_id, filename, token, tray_app=None, server_link=None):
    # Remove a file block from a Notion page.
    try:
        notion = Client(auth=token)
        
        # Ensure cache is populated
        if page_id not in link_cache or filename not in link_cache[page_id].get("blocks", {}):
            get_existing_links(page_id, notion, force_refresh=True)
            
        cached_data = link_cache.get(page_id)
        if not cached_data:
            print(f"Could not find cache for page ...{page_id[-6:]}")
            return False
            
        block_id = cached_data.get("blocks", {}).get(filename)
        
        # If not found by filename, try by URL
        if not block_id and server_link:
            block_id = cached_data.get("url_to_block", {}).get(server_link)
            
        if not block_id:
            print(f"Could not find block for {filename} (or link) on page ...{page_id[-6:]}")
            return False
            
        notion.blocks.delete(block_id=block_id)
        print(f"Removed block for {filename} from page.")
        
        # Update cache
        if filename in cached_data["filenames"]:
            cached_data["filenames"].remove(filename)
        if filename in cached_data["blocks"]:
            del cached_data["blocks"][filename]
        if server_link and server_link in cached_data["url_to_block"]:
            del cached_data["url_to_block"][server_link]
            
        if tray_app:
            tray_app.tray_icon.showMessage(
                "NotionLink: File Removed",
                f"Link for '{filename}' removed from page.",
                QSystemTrayIcon.Information,
                3000
            )
        return True
    except Exception as e:
        print(f"Error removing file from page: {e}")
        return False


def update_file_on_page(page_id, old_filename, new_filename, new_link, token, tray_app=None, old_link=None):
    # Update a file block on a Notion page.
    try:
        notion = Client(auth=token)
        
        # Ensure cache is populated
        if page_id not in link_cache or old_filename not in link_cache[page_id].get("blocks", {}):
            get_existing_links(page_id, notion, force_refresh=True)
            
        cached_data = link_cache.get(page_id)
        if not cached_data:
            print(f"Could not find cache for page ...{page_id[-6:]}")
            return False
            
        block_id = cached_data.get("blocks", {}).get(old_filename)
        
        # If not found by filename, try by URL
        if not block_id and old_link:
            block_id = cached_data.get("url_to_block", {}).get(old_link)
            
        if not block_id:
            print(f"Could not find block for {old_filename} (or link) on page ...{page_id[-6:]}")
            return False
        
        # Update block content
        notion.blocks.update(
            block_id=block_id,
            paragraph={
                "rich_text": [{
                    "type": "text",
                    "text": {
                        "content": new_filename,
                        "link": {"url": new_link}
                    }
                }]
            }
        )
        print(f"Updated block: {old_filename} → {new_filename}")
        
        # Update cache
        if old_filename in cached_data["filenames"]:
            cached_data["filenames"].remove(old_filename)
        cached_data["filenames"].add(new_filename)
        
        if old_filename in cached_data["blocks"]:
            del cached_data["blocks"][old_filename]
        cached_data["blocks"][new_filename] = block_id
        
        if old_link and old_link in cached_data["url_to_block"]:
            del cached_data["url_to_block"][old_link]
        cached_data["url_to_block"][new_link] = block_id
        
        cached_data["urls"].add(new_link)
        
        if tray_app:
            tray_app.tray_icon.showMessage(
                "NotionLink: File Renamed",
                f"Link updated: '{old_filename}' → '{new_filename}'",
                QSystemTrayIcon.Information,
                3000
            )
        return True
    except Exception as e:
        print(f"Error updating file on page: {e}")
        return False
