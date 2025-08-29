#!/usr/bin/env python3
"""
Streamlit App for Flipkart Ninjutsu Automation Workflows
Combines Gmail attachment downloader and PDF/Excel GRN processor with real-time tracking
"""

import streamlit as st
import os
import json
import base64
import tempfile
import time
import logging
import pandas as pd
import PyPDF2
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from io import BytesIO, StringIO
import threading
import queue
import re
import warnings

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
import dateutil.parser
from google.oauth2 import service_account

warnings.filterwarnings("ignore")

# Configure Streamlit page
st.set_page_config(
    page_title="Flipkart Ninjutsu Automation",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded"
)

class FlipkartNinjutsuAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        
        # API scopes
        self.gmail_scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes = ['https://www.googleapis.com/auth/drive']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
    
    def authenticate_from_secrets(self, progress_bar, status_text):
    try:
        status_text.text("Authenticating with Google APIs...")
        progress_bar.progress(10)
        
        if 'oauth_token' in st.session_state:
            try:
                combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
                creds = Credentials.from_authorized_user_info(st.session_state.oauth_token, combined_scopes)
                if creds and creds.valid:
                    progress_bar.progress(50)
                    self.gmail_service = build('gmail', 'v1', credentials=creds)
                    self.drive_service = build('drive', 'v3', credentials=creds)
                    self.sheets_service = build('sheets', 'v4', credentials=creds)
                    progress_bar.progress(100)
                    status_text.text("Authentication successful!")
                    return True
                elif creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    st.session_state.oauth_token = json.loads(creds.to_json())
                    self.gmail_service = build('gmail', 'v1', credentials=creds)
                    self.drive_service = build('drive', 'v3', credentials=creds)
                    self.sheets_service = build('sheets', 'v4', credentials=creds)
                    progress_bar.progress(100)
                    status_text.text("Authentication successful!")
                    return True
            except Exception as e:
                st.info(f"Cached token invalid, requesting new authentication: {str(e)}")
        
        if "google" in st.secrets and "credentials_json" in st.secrets["google"]:
            creds_data = json.loads(st.secrets["google"]["credentials_json"])
            redirect_uri = st.secrets.get("redirect_uri")
            if not redirect_uri:
                st.error("Redirect URI missing in Streamlit secrets")
                st.stop()
            combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
            
            flow = Flow.from_client_config(
                client_config=creds_data,
                scopes=combined_scopes,
                redirect_uri=redirect_uri
            )
            
            auth_url, _ = flow.authorization_url(prompt='consent')
            query_params = st.query_params
            if "code" in query_params:
                st.write("Received code:", query_params["code"])  # Debug
                try:
                    code = query_params["code"][0]
                    flow.fetch_token(code=code)
                    creds = flow.credentials
                    st.session_state.oauth_token = json.loads(creds.to_json())
                    progress_bar.progress(50)
                    self.gmail_service = build('gmail', 'v1', credentials=creds)
                    self.drive_service = build('drive', 'v3', credentials=creds)
                    self.sheets_service = build('sheets', 'v4', credentials=creds)
                    progress_bar.progress(100)
                    status_text.text("Authentication successful!")
                    st.query_params.clear()
                    return True
                except Exception as e:
                    st.error(f"Authentication failed: {str(e)}")
                    return False
            else:
                st.markdown("### Google Authentication Required")
                st.markdown(f"[Authorize with Google]({auth_url})")
                st.info("Click the link above to authorize, you'll be redirected back automatically")
                st.stop()
        else:
            st.error("Google credentials missing in Streamlit secrets")
            return False
            
    except Exception as e:
        st.error(f"Authentication failed: {str(e)}")
        return False

    def process_gmail_workflow(self, config: dict, log_queue: queue.Queue):
        """Process Gmail attachment download workflow"""
        log_queue.put("[START] Starting Gmail to Google Drive automation")
        log_queue.put(f"[CONFIG] Parameters: sender='{config['sender']}', search_term='{config['search_term']}', days_back={config['days_back']}")
        
        emails = self.search_emails(config['sender'], config['search_term'], config['days_back'], config['max_results'])
        
        if not emails:
            log_queue.put("[INFO] No emails found matching criteria")
            return {'success': True, 'processed': 0}
        
        stats = self.process_emails(emails, config['search_term'], config.get('gdrive_folder_id'), log_queue)
        
        log_queue.put("[COMPLETE] AUTOMATION COMPLETE!")
        log_queue.put(f"[STATS] Emails processed: {stats['processed_emails']}/{stats['total_emails']}")
        log_queue.put(f"[STATS] Total attachments: {stats['total_attachments']}")
        log_queue.put(f"[STATS] Successful uploads: {stats['successful_uploads']}")
        log_queue.put(f"[STATS] Failed uploads: {stats['failed_uploads']}")
        
        return {'success': True, 'processed': stats['total_attachments']}
    
    def search_emails(self, sender: str, search_term: str, days_back: int, max_results: int) -> List[Dict]:
        query_parts = ["has:attachment"]
        if sender:
            query_parts.append(f"from:{sender}")
        if search_term:
            query_parts.append(f'"{search_term}"')
        start_date = datetime.now() - timedelta(days=days_back)
        query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
        query = " ".join(query_parts)
        result = self.gmail_service.users().messages().list(userId='me', q=query, maxResults=max_results).execute()
        return result.get('messages', [])
    
    def process_emails(self, emails: List[Dict], search_term: str, gdrive_folder_id: str, log_queue: queue.Queue) -> Dict:
        stats = {
            'total_emails': len(emails),
            'processed_emails': 0,
            'total_attachments': 0,
            'successful_uploads': 0,
            'failed_uploads': 0
        }
        
        base_folder_id = self.create_drive_folder("Gmail_Attachments", gdrive_folder_id)
        if not base_folder_id:
            log_queue.put("[ERROR] Failed to create base folder in Google Drive")
            return stats
        
        for i, email in enumerate(emails):
            log_queue.put(f"[PROCESS] Processing email {i+1}/{len(emails)}")
            sender_info = self.get_email_details(email['id'])
            message = self.gmail_service.users().messages().get(userId='me', id=email['id']).execute()
            attachment_count = self.extract_attachments_from_email(email['id'], message['payload'], sender_info, search_term, base_folder_id, log_queue)
            stats['total_attachments'] += attachment_count
            stats['successful_uploads'] += attachment_count
            stats['processed_emails'] += 1
        return stats
    
    def get_email_details(self, message_id: str) -> Dict:
        message = self.gmail_service.users().messages().get(userId='me', id=message_id, format='metadata').execute()
        headers = message['payload'].get('headers', [])
        return {
            'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
            'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)")
        }
    
    def create_drive_folder(self, folder_name: str, parent_id: str) -> str:
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        existing = self.drive_service.files().list(q=query, fields='files(id)').execute()
        if existing.get('files', []):
            return existing['files'][0]['id']
        folder_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
        if parent_id:
            folder_metadata['parents'] = [parent_id]
        folder = self.drive_service.files().create(body=folder_metadata, fields='id').execute()
        return folder.get('id')
    
    def extract_attachments_from_email(self, message_id: str, payload: Dict, sender_info: Dict, search_term: str, base_folder_id: str, log_queue: queue.Queue) -> int:
        processed_count = 0
        if "parts" in payload:
            for part in payload["parts"]:
                processed_count += self.extract_attachments_from_email(message_id, part, sender_info, search_term, base_folder_id, log_queue)
        elif payload.get("filename") and "body" in payload and "attachmentId" in payload["body"]:
            if self.process_attachment(message_id, payload, sender_info, search_term, base_folder_id, log_queue):
                processed_count += 1
        return processed_count
    
    def process_attachment(self, message_id: str, part: Dict, sender_info: Dict, search_term: str, base_folder_id: str, log_queue: queue.Queue) -> bool:
        filename = part.get("filename", "")
        if not filename:
            return False
        clean_filename = self.sanitize_filename(filename)
        final_filename = f"{message_id}_{clean_filename}"
        attachment_id = part["body"].get("attachmentId")
        att = self.gmail_service.users().messages().attachments().get(userId='me', messageId=message_id, id=attachment_id).execute()
        file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
        sender_email = sender_info.get('sender', 'Unknown').split("<")[1].split(">")[0].strip() if "<" in sender_info.get('sender', 'Unknown') else 'Unknown'
        sender_folder_id = self.create_drive_folder(sender_email, base_folder_id)
        search_folder_id = self.create_drive_folder(search_term if search_term else "all-attachments", sender_folder_id)
        type_folder_id = self.create_drive_folder(self.classify_extension(filename), search_folder_id)
        success = self.upload_to_drive(file_data, final_filename, type_folder_id, log_queue)
        return success
    
    def sanitize_filename(self, filename: str) -> str:
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split('.')
            if len(name_parts) > 1:
                cleaned = f"{name_parts[0][:95]}.{name_parts[-1]}"
            else:
                cleaned = cleaned[:100]
        return cleaned
    
    def classify_extension(self, filename: str) -> str:
        if '.' not in filename:
            return "Other"
        ext = filename.split(".")[-1].lower()
        if ext == "pdf":
            return "PDFs"
        return "Other"
    
    def upload_to_drive(self, file_data: bytes, filename: str, folder_id: str, log_queue: queue.Queue) -> bool:
        query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
        existing = self.drive_service.files().list(q=query, fields='files(id)').execute()
        if existing.get('files', []):
            log_queue.put(f"[DRIVE] File already exists, skipping: {filename}")
            return True
        file_metadata = {'name': filename, 'parents': [folder_id]}
        media = MediaIoBaseUpload(BytesIO(file_data), mimetype='application/octet-stream', resumable=True)
        file = self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        log_queue.put(f"[DRIVE] Uploaded to Drive: {filename}")
        return True
    
    def process_pdf_workflow(self, config: dict, log_queue: queue.Queue):
        """Process PDF GRN workflow"""
        log_queue.put("[START] Starting PDF GRN workflow")
        pdf_files = self.get_pdf_files(config['drive_folder_id'], config['days_back'])
        if not pdf_files:
            log_queue.put("[INFO] No PDF files found")
            return {'success': True, 'processed': 0}
        
        processed_count = 0
        sheet_has_headers = False
        is_first_file = True
        
        for i, file in enumerate(pdf_files):
            log_queue.put(f"[PROCESS] Processing PDF {i+1}/{len(pdf_files)}: {file['name']}")
            df = self.read_pdf_file(file['id'], file['name'], log_queue)
            if df.empty:
                log_queue.put(f"[WARNING] No data extracted from {file['name']}")
                continue
            self.append_to_sheet(config['spreadsheet_id'], config['sheet_range'], df, is_first_file, sheet_has_headers, log_queue)
            processed_count += 1
            is_first_file = False
        
        if processed_count > 0:
            self.remove_duplicates_from_sheet(config['spreadsheet_id'], config['sheet_range'], log_queue)
        
        log_queue.put("[COMPLETE] PDF workflow completed")
        return {'success': True, 'processed': processed_count}
    
    def get_pdf_files(self, folder_id: str, days_back: int):
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%dT00:00:00Z')
        query = f"'{folder_id}' in parents and mimeType='application/pdf' and createdTime >= '{start_date}'"
        results = self.drive_service.files().list(q=query, fields="files(id, name, createdTime)").execute()
        return results.get('files', [])
    
    def read_pdf_file(self, file_id: str, filename: str, log_queue: queue.Queue) -> pd.DataFrame:
        request = self.drive_service.files().get_media(fileId=file_id)
        file_stream = BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        file_stream.seek(0)
        
        try:
            reader = PyPDF2.PdfReader(file_stream)
            text = ""
            for page in reader.pages:
                text += page.extract_text() or ""
            log_queue.put(f"[INFO] Extracted text from PDF: {len(text)} characters")
            # Simple text parsing to DataFrame (customize based on your PDF structure)
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            if not lines:
                return pd.DataFrame()
            # Assume a simple tabular structure (e.g., space-separated)
            data = [line.split() for line in lines if ' ' in line]
            if not data or len(data[0]) < 2:
                return pd.DataFrame()
            df = pd.DataFrame(data[1:], columns=data[0] if data[0][0].isalpha() else [f"Col{i}" for i in range(len(data[0]))])
            df = self.clean_dataframe(df)
            log_queue.put(f"[SUCCESS] Parsed PDF to DataFrame: {df.shape}")
            return df
        except Exception as e:
            log_queue.put(f"[ERROR] PDF text extraction failed: {str(e)}")
            return pd.DataFrame()
    
    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.replace("'", "", regex=False)
        if len(df.columns) >= 2:
            second_col = df.columns[1]
            df = df[df[second_col].astype(str).str.strip() != ""]
        df = df.drop_duplicates()
        return df
    
    def append_to_sheet(self, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame, is_first_file: bool, sheet_has_headers: bool, log_queue: queue.Queue):
        result = self.sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1").execute()
        existing_rows = result.get('values', [])
        start_row = len(existing_rows) + 1 if existing_rows else 1
        if sheet_has_headers and not is_first_file:
            start_row = 2 if len(existing_rows) >= 1 else 1
        if is_first_file:
            values = [df.columns.tolist()] + df.values.tolist()
        else:
            values = df.values.tolist()
        self.sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A{start_row}",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        log_queue.put(f"[SUCCESS] Appended {len(values)} rows to sheet")
    
    def remove_duplicates_from_sheet(self, spreadsheet_id: str, sheet_name: str, log_queue: queue.Queue):
        result = self.sheets_service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A:ZZ").execute()
        values = result.get('values', [])
        if not values:
            log_queue.put("[INFO] Sheet is empty, skipping duplicate removal")
            return
        headers = values[0]
        df = pd.DataFrame(values[1:], columns=headers)
        before = len(df)
        if "PurchaseOrderId" in df.columns and "SkuId" in df.columns:
            df = df.drop_duplicates(subset=["PurchaseOrderId", "SkuId"])
        after = len(df)
        self.sheets_service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=sheet_name).execute()
        body = {"values": [headers] + df.values.tolist()}
        self.sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body=body
        ).execute()
        log_queue.put(f"[INFO] Removed {before - after} duplicate rows. Final row count: {after}")

def run_workflow_with_logs(target_func, config, log_container):
    log_queue = queue.Queue()
    thread = threading.Thread(target=target_func, args=(config, log_queue))
    thread.start()
    logs = []
    while thread.is_alive():
        try:
            log = log_queue.get(timeout=0.1)
            logs.append(log)
        except queue.Empty:
            pass
        log_container.text_area("Logs", "\n".join(logs), height=400)
    while True:
        try:
            log = log_queue.get_nowait()
            logs.append(log)
        except queue.Empty:
            break
    log_container.text_area("Logs", "\n".join(logs), height=400)
    thread.join()
    return logs

def create_streamlit_ui():
    st.title("🔥 Flipkart Ninjutsu Automation")
    st.markdown("### Automated Gmail Attachment Processing & PDF GRN Consolidation")
    
    if 'automation' not in st.session_state:
        st.session_state.automation = FlipkartNinjutsuAutomation()
    
    # Sidebar for log tracker
    st.sidebar.title("Log Tracker")
    log_container = st.sidebar.empty()
    
    # Authentication
    if st.button("Authenticate Google APIs"):
        with st.spinner("Authenticating..."):
            progress_bar = st.progress(0)
            status_text = st.empty()
            success = st.session_state.automation.authenticate_from_secrets(progress_bar, status_text)
            if success:
                st.session_state.authenticated = True
            else:
                st.session_state.authenticated = False
    
    if not st.session_state.get('authenticated', False):
        st.warning("Please authenticate first")
        st.stop()
    
    # Show configurations after authentication
    st.header("Current Configuration")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Gmail Configuration")
        gmail_config = {
            "sender": st.text_input("Sender", value="ninjutsu_bot@ninjacart.com"),
            "search_term": st.text_input("Search Term", value="grn & purchase return"),
            "days_back": st.number_input("Days Back", value=7, key="gmail_days"),
            "max_results": st.number_input("Max Results", value=1000),
            "gdrive_folder_id": st.text_input("GDrive Folder ID", value="141D67nCRsJ3HM9WkhvY9enI7-B6Ws")
        }
    
    with col2:
        st.subheader("PDF Configuration")
        pdf_config = {
            "drive_folder_id": st.text_input("Drive Folder ID", value="19basSTaOUB-X0FLrwmBkeVULGe8nBQ5X"),
            "spreadsheet_id": st.text_input("Spreadsheet ID", value="16WLCJkFKSLKTjIi0962aSkgTGbkO9PMdJTgkWnn11fW"),
            "sheet_range": st.text_input("Sheet Range", value="instamart_grn"),
            "days_back": st.number_input("Days Back", value=1, key="pdf_days")
        }
    
    workflow_choice = st.selectbox("Select Workflow", ["Gmail Workflow", "PDF Workflow", "Combined Workflow"])
    
    if st.button("🚀 Start Workflow", type="primary"):
        with st.spinner("Processing workflow..."):
            if workflow_choice == "Gmail Workflow":
                run_workflow_with_logs(st.session_state.automation.process_gmail_workflow, gmail_config, log_container)
                st.success("Gmail workflow completed!")
            elif workflow_choice == "PDF Workflow":
                run_workflow_with_logs(st.session_state.automation.process_pdf_workflow, pdf_config, log_container)
                st.success("PDF workflow completed!")
            else:  # Combined
                st.info("Running Gmail workflow...")
                gmail_result = run_workflow_with_logs(st.session_state.automation.process_gmail_workflow, gmail_config, log_container)
                st.info("Running PDF workflow...")
                pdf_result = run_workflow_with_logs(st.session_state.automation.process_pdf_workflow, pdf_config, log_container)
                st.success("Combined workflow completed!")

if __name__ == "__main__":
    create_streamlit_ui()


