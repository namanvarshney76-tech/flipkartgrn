import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import os
import zipfile
from lxml import etree
import tempfile
import warnings
import subprocess
import sys
from datetime import datetime, timezone
import dateutil.parser
warnings.filterwarnings("ignore")

# Define the scopes
SCOPES = [
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets'
]

def install_package(package):
    """Install package if not available"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"Installed {package}")
    except:
        print(f"Failed to install {package}")

def get_header_row_input():
    """Get header row selection from user"""
    print("\n" + "="*50)
    print("HEADER ROW CONFIGURATION")
    print("="*50)
    print("Please specify where the headers are located in your Excel files:")
    print("  0 = First row (default)")
    print("  1 = Second row")
    print("  2 = Third row")
    print("  etc.")
    print("  -1 = No headers (will create generic column names)")
    
    while True:
        try:
            user_input = input("\nEnter header row number (0 for first row, -1 for no headers): ").strip()
            if user_input == "":
                header_row = 0
                print("Using default: First row (0)")
                break
            
            header_row = int(user_input)
            if header_row >= -1:
                if header_row == -1:
                    print("No headers will be used - generic column names will be created")
                else:
                    print(f"Headers will be read from row {header_row + 1} (index {header_row})")
                break
            else:
                print("Please enter a number >= -1")
                
        except ValueError:
            print("Please enter a valid number")
    
    return header_row

def authenticate():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def get_todays_date_range():
    """Get today's date range in UTC for filtering files"""
    today = datetime.now().date()
    start_of_today = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)
    end_of_today = datetime.combine(today, datetime.max.time()).replace(tzinfo=timezone.utc)
    return start_of_today, end_of_today

def is_file_created_today(created_time_str):
    """Check if a file was created today"""
    try:
        # Parse the createdTime from Google Drive API (RFC 3339 format)
        created_time = dateutil.parser.parse(created_time_str)
        if created_time.tzinfo is None:
            created_time = created_time.replace(tzinfo=timezone.utc)
        
        start_of_today, end_of_today = get_todays_date_range()
        return start_of_today <= created_time <= end_of_today
    except Exception as e:
        print(f"Error parsing date {created_time_str}: {e}")
        return False

def get_excel_files(drive_service, folder_id, page_size=1000):
    """Get all Excel files created today, with pagination support"""
    files = []
    next_page_token = None
    today_start, today_end = get_todays_date_range()
    
    # Format dates for Google Drive API query (RFC 3339)
    today_start_str = today_start.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    today_end_str = today_end.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    print(f"Searching for files created today ({today_start.strftime('%Y-%m-%d')})")
    
    while True:
        # Query for Excel files in folder created today
        query = (f"'{folder_id}' in parents and "
                f"(mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
                f"mimeType='application/vnd.ms-excel') and "
                f"createdTime >= '{today_start_str}' and "
                f"createdTime <= '{today_end_str}'")
        
        request_params = {
            'q': query,
            'fields': "files(id, name, createdTime), nextPageToken",
            'pageSize': page_size,
            'orderBy': 'createdTime desc'  # Most recent first
        }
        
        if next_page_token:
            request_params['pageToken'] = next_page_token
        
        results = drive_service.files().list(**request_params).execute()
        batch_files = results.get('files', [])
        
        # Double-check the creation date (additional safety check)
        today_files = []
        for file in batch_files:
            if is_file_created_today(file.get('createdTime', '')):
                today_files.append(file)
                print(f"  Found today's file: {file['name']} (created: {file['createdTime']})")
        
        files.extend(today_files)
        print(f"Found {len(today_files)} Excel files created today in this batch (Total so far: {len(files)})")
        
        next_page_token = results.get('nextPageToken')
        if not next_page_token:
            break
    
    return files

def clean_cell_value(value):
    """Clean and standardize cell values"""
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return ""
        return str(value)
    cleaned = str(value).strip().replace("'", "")
    return cleaned

def clean_dataframe(df):
    """Clean DataFrame by removing rows with blank B column, duplicates, and single quotes"""
    if df.empty:
        return df
    
    print(f"    Original DataFrame shape: {df.shape}")
    string_columns = df.select_dtypes(include=['object']).columns
    for col in string_columns:
        df[col] = df[col].astype(str).str.replace("'", "", regex=False)
    print(f"    Removed single quotes from {len(string_columns)} columns")
    
    if len(df.columns) >= 2:
        second_col = df.columns[1]
        mask = ~(
            df[second_col].isna() | 
            (df[second_col].astype(str).str.strip() == "") |
            (df[second_col].astype(str).str.strip() == "nan")
        )
        df = df[mask]
        print(f"    After removing rows with blank second column '{second_col}': {df.shape}")
    else:
        print("    Warning: DataFrame has less than 2 columns, skipping blank B column removal")
    
    original_count = len(df)
    df = df.drop_duplicates()
    duplicates_removed = original_count - len(df)
    if duplicates_removed > 0:
        print(f"    Removed {duplicates_removed} duplicate rows")
    
    print(f"    Final cleaned DataFrame shape: {df.shape}")
    return df

def try_xlsxwriter_read(file_stream):
    """Try using xlsxwriter's read capabilities via conversion"""
    try:
        return pd.DataFrame()
    except:
        return pd.DataFrame()

def try_pyxlsb(file_stream, filename, header_row):
    """Try pyxlsb for .xlsb files or as alternative"""
    try:
        import pyxlsb
        file_stream.seek(0)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsb') as tmp_file:
            tmp_file.write(file_stream.read())
            tmp_file.flush()
            if header_row == -1:
                df = pd.read_excel(tmp_file.name, engine='pyxlsb', header=None)
            else:
                df = pd.read_excel(tmp_file.name, engine='pyxlsb', header=header_row)
            os.unlink(tmp_file.name)
            return df
    except ImportError:
        print("    pyxlsb not available, skipping...")
        return pd.DataFrame()
    except Exception as e:
        print(f"    pyxlsb failed: {str(e)[:50]}...")
        return pd.DataFrame()

def try_xlwings(file_stream, filename, header_row):
    """Try xlwings if available (Windows/Mac with Excel)"""
    try:
        import xlwings as xw
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            file_stream.seek(0)
            tmp_file.write(file_stream.read())
            tmp_file.flush()
            app = xw.App(visible=False)
            wb = app.books.open(tmp_file.name)
            ws = wb.sheets[0]
            used_range = ws.used_range
            if used_range:
                data = used_range.value
                if data and len(data) > header_row + 1:
                    if header_row == -1:
                        num_cols = len(data[0]) if data else 0
                        headers = [f"Column_{i+1}" for i in range(num_cols)]
                        df = pd.DataFrame(data, columns=headers)
                    else:
                        headers = [str(h) if h else f"Column_{i+1}" for i, h in enumerate(data[header_row])]
                        df = pd.DataFrame(data[header_row+1:], columns=headers)
                else:
                    df = pd.DataFrame()
            else:
                df = pd.DataFrame()
            wb.close()
            app.quit()
            os.unlink(tmp_file.name)
            return df
    except ImportError:
        print("    xlwings not available, skipping...")
        return pd.DataFrame()
    except Exception as e:
        print(f"    xlwings failed: {str(e)[:50]}...")
        return pd.DataFrame()

def try_xlrd2(file_stream, header_row):
    """Try xlrd2 as alternative to xlrd"""
    try:
        import xlrd2
        file_stream.seek(0)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            tmp_file.write(file_stream.read())
            tmp_file.flush()
            if header_row == -1:
                df = pd.read_excel(tmp_file.name, engine='xlrd2', header=None)
            else:
                df = pd.read_excel(tmp_file.name, engine='xlrd2', header=header_row)
            os.unlink(tmp_file.name)
            return df
    except ImportError:
        print("    xlrd2 not available, skipping...")
        return pd.DataFrame()
    except Exception as e:
        print(f"    xlrd2 failed: {str(e)[:50]}...")
        return pd.DataFrame()

def try_raw_xml_extraction(file_stream, header_row):
    """More aggressive raw XML extraction with proper text handling"""
    try:
        file_stream.seek(0)
        with zipfile.ZipFile(file_stream, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            shared_strings = {}
            shared_strings_file = 'xl/sharedStrings.xml'
            if shared_strings_file in file_list:
                try:
                    with zip_ref.open(shared_strings_file) as ss_file:
                        ss_content = ss_file.read().decode('utf-8', errors='ignore')
                        import re
                        string_pattern = r'<t[^>]*>([^<]*)</t>'
                        strings = re.findall(string_pattern, ss_content, re.DOTALL)
                        for i, string_val in enumerate(strings):
                            shared_strings[str(i)] = string_val.strip()
                        print(f"    Found {len(shared_strings)} shared strings")
                except Exception as e:
                    print(f"    Failed to read shared strings: {str(e)[:30]}...")
            
            worksheet_files = [f for f in file_list if 'xl/worksheets/' in f and f.endswith('.xml')]
            if not worksheet_files:
                return pd.DataFrame()
            
            with zip_ref.open(worksheet_files[0]) as xml_file:
                content = xml_file.read().decode('utf-8', errors='ignore')
                import re
                cell_pattern = r'<c[^>]*r="([A-Z]+\d+)"[^>]*(?:t="([^"]*)")?[^>]*>(?:.*?<v[^>]*>([^<]*)</v>)?(?:.*?<is><t[^>]*>([^<]*)</t></is>)?'
                cells = re.findall(cell_pattern, content, re.DOTALL)
                
                if not cells:
                    return pd.DataFrame()
                
                cell_data = {}
                max_row = 0
                max_col = 0
                
                for cell_ref, cell_type, v_value, is_value in cells:
                    col_letters = ''.join([c for c in cell_ref if c.isalpha()])
                    row_num = int(''.join([c for c in cell_ref if c.isdigit()]))
                    col_num = 0
                    for c in col_letters:
                        col_num = col_num * 26 + (ord(c) - ord('A') + 1)
                    
                    if is_value:
                        cell_value = is_value.strip()
                    elif cell_type == 's' and v_value:
                        cell_value = shared_strings.get(v_value, v_value)
                    elif cell_type == 'str' and v_value:
                        cell_value = v_value.strip()
                    elif v_value:
                        cell_value = v_value.strip()
                    else:
                        cell_value = ""
                    
                    cell_data[(row_num, col_num)] = clean_cell_value(cell_value)
                    max_row = max(max_row, row_num)
                    max_col = max(max_col, col_num)
                
                if not cell_data:
                    return pd.DataFrame()
                
                data = []
                for row in range(1, max_row + 1):
                    row_data = []
                    for col in range(1, max_col + 1):
                        row_data.append(cell_data.get((row, col), ""))
                    if any(cell for cell in row_data):
                        data.append(row_data)
                
                if len(data) < max(1, header_row + 2):
                    return pd.DataFrame()
                
                if header_row == -1:
                    headers = [f"Column_{i+1}" for i in range(len(data[0]))]
                    return pd.DataFrame(data, columns=headers)
                else:
                    if len(data) > header_row:
                        headers = [str(h) if h else f"Column_{i+1}" for i, h in enumerate(data[header_row])]
                        return pd.DataFrame(data[header_row+1:], columns=headers)
                    else:
                        return pd.DataFrame()
                
    except Exception as e:
        print(f"    raw XML extraction failed: {str(e)[:50]}...")
        return pd.DataFrame()

def convert_with_libreoffice(file_stream, filename, header_row):
    """Try converting with LibreOffice command line"""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_input:
            file_stream.seek(0)
            tmp_input.write(file_stream.read())
            tmp_input.flush()
            with tempfile.TemporaryDirectory() as tmp_dir:
                result = subprocess.run([
                    'libreoffice', '--headless', '--convert-to', 'csv',
                    '--outdir', tmp_dir, tmp_input.name
                ], capture_output=True, timeout=30)
                if result.returncode == 0:
                    csv_file = os.path.join(tmp_dir, os.path.splitext(os.path.basename(tmp_input.name))[0] + '.csv')
                    if os.path.exists(csv_file):
                        if header_row == -1:
                            df = pd.read_csv(csv_file, header=None)
                        else:
                            df = pd.read_csv(csv_file, header=header_row)
                        os.unlink(tmp_input.name)
                        return df
        os.unlink(tmp_input.name)
        return pd.DataFrame()
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
        print(f"    LibreOffice conversion failed: {str(e)[:50]}...")
        return pd.DataFrame()

def try_csv_conversion_with_ssconvert(file_stream, filename, header_row):
    """Try using Gnumeric's ssconvert"""
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_input:
            file_stream.seek(0)
            tmp_input.write(file_stream.read())
            tmp_input.flush()
            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_output:
                result = subprocess.run([
                    'ssconvert', tmp_input.name, tmp_output.name
                ], capture_output=True, timeout=30)
                if result.returncode == 0 and os.path.exists(tmp_output.name):
                    if header_row == -1:
                        df = pd.read_csv(tmp_output.name, header=None)
                    else:
                        df = pd.read_csv(tmp_output.name, header=header_row)
                    os.unlink(tmp_input.name)
                    os.unlink(tmp_output.name)
                    return df
        try:
            os.unlink(tmp_input.name)
            os.unlink(tmp_output.name)
        except:
            pass
        return pd.DataFrame()
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
        print(f"    ssconvert failed: {str(e)[:50]}...")
        return pd.DataFrame()

def read_excel_file(drive_service, file_id, filename, header_row):
    """Ultra-robust Excel reader with maximum fallback strategies"""
    request = drive_service.files().get_media(fileId=file_id)
    file_stream = io.BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    file_stream.seek(0)

    print(f"  Attempting to read {filename}...")
    print(f"  File size: {len(file_stream.getvalue())} bytes")
    print(f"  Header row setting: {header_row if header_row != -1 else 'No headers'}")

    try:
        file_stream.seek(0)
        if header_row == -1:
            df = pd.read_excel(file_stream, engine="openpyxl", header=None)
        else:
            df = pd.read_excel(file_stream, engine="openpyxl", header=header_row)
        if not df.empty:
            print(f"  SUCCESS with openpyxl")
            df = clean_dataframe(df)
            return df
    except Exception as e:
        print(f"  openpyxl failed: {str(e)[:50]}...")

    if filename.lower().endswith('.xls'):
        try:
            file_stream.seek(0)
            if header_row == -1:
                df = pd.read_excel(file_stream, engine="xlrd", header=None)
            else:
                df = pd.read_excel(file_stream, engine="xlrd", header=header_row)
            if not df.empty:
                print(f"  SUCCESS with xlrd")
                df = clean_dataframe(df)
                return df
        except Exception as e:
            print(f"  xlrd failed: {str(e)[:50]}...")

    try:
        file_stream.seek(0)
        if header_row == -1:
            df = pd.read_excel(file_stream, engine="calamine", header=None)
        else:
            df = pd.read_excel(file_stream, engine="calamine", header=header_row)
        if not df.empty:
            print(f"  SUCCESS with calamine")
            df = clean_dataframe(df)
            return df
    except Exception as e:
        print(f"  calamine failed: {str(e)[:50]}...")

    engines_to_try = []
    try:
        import pyxlsb
        engines_to_try.append('pyxlsb')
    except ImportError:
        pass
    
    for engine in engines_to_try:
        try:
            file_stream.seek(0)
            if header_row == -1:
                df = pd.read_excel(file_stream, engine=engine, header=None)
            else:
                df = pd.read_excel(file_stream, engine=engine, header=header_row)
            if not df.empty:
                print(f"  SUCCESS with {engine}")
                df = clean_dataframe(df)
                return df
        except Exception as e:
            print(f"  {engine} failed: {str(e)[:50]}...")

    df = try_xlwings(file_stream, filename, header_row)
    if not df.empty:
        print(f"  SUCCESS with xlwings")
        df = clean_dataframe(df)
        return df

    df = try_raw_xml_extraction(file_stream, header_row)
    if not df.empty:
        print(f"  SUCCESS with raw XML extraction")
        df = clean_dataframe(df)
        return df

    df = convert_with_libreoffice(file_stream, filename, header_row)
    if not df.empty:
        print(f"  SUCCESS with LibreOffice conversion")
        df = clean_dataframe(df)
        return df

    df = try_csv_conversion_with_ssconvert(file_stream, filename, header_row)
    if not df.empty:
        print(f"  SUCCESS with ssconvert")
        df = clean_dataframe(df)
        return df

    try:
        file_stream.seek(0)
        with zipfile.ZipFile(file_stream, 'r') as zip_ref:
            for file_info in zip_ref.filelist:
                if file_info.filename.endswith('.xml'):
                    try:
                        with zip_ref.open(file_info.filename) as xml_file:
                            content = xml_file.read().decode('utf-8', errors='ignore')
                            import re
                            text_matches = re.findall(r'>([^<]{2,})<', content)
                            if len(text_matches) > 10:
                                print(f"  Found some text in {file_info.filename}, but cannot structure it properly")
                                break
                    except:
                        continue
    except Exception as e:
        print(f"  Final text extraction failed: {str(e)[:50]}...")

    print(f"  FAILED - All {7} strategies failed for {filename}")
    file_stream.seek(0)
    first_bytes = file_stream.read(1000)
    print(f"  First 20 bytes (hex): {first_bytes[:20].hex()}")
    return pd.DataFrame()

def append_to_sheet(sheets_service, spreadsheet_id, sheet_name, data, append_headers, sheet_has_headers=False):
    """Append data to Google Sheet, adding headers only if specified and sheet is empty"""
    try:
        # Check if sheet already has headers
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1"
        ).execute()
        existing_rows = result.get('values', [])
        start_row = len(existing_rows) + 1 if existing_rows else 1
        
        # If sheet has headers and we're not forcing new headers, start appending after existing headers
        if sheet_has_headers and not append_headers:
            start_row = 2 if len(existing_rows) >= 1 else 1
        
        clean_data = data.fillna('').astype(str)
        
        # Only include headers if explicitly requested and sheet is empty or we're forcing headers
        if append_headers and (not existing_rows or start_row == 1):
            values = [clean_data.columns.tolist()] + clean_data.values.tolist()
        else:
            values = clean_data.values.tolist()

        sheets_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A{start_row}",
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
    except Exception as e:
        print(f"Error appending to sheet: {str(e)}")
        raise

def process_files_in_batches(excel_files, batch_size=1000):
    """Process files in batches of specified size"""
    for i in range(0, len(excel_files), batch_size):
        batch = excel_files[i:i+batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(excel_files) + batch_size - 1) // batch_size
        print(f"\n{'='*60}")
        print(f"PROCESSING BATCH {batch_num} of {total_batches}")
        print(f"Files {i+1} to {min(i+batch_size, len(excel_files))} of {len(excel_files)}")
        print(f"{'='*60}")
        yield batch, batch_num, total_batches

def remove_duplicates_from_sheet(sheets_service, spreadsheet_id, sheet_name):
    """Remove duplicate rows from the Google Sheet based on Item Code + po_number."""
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1:ZZ"
        ).execute()
        values = result.get('values', [])
        if not values:
            print("Sheet is empty, skipping duplicate removal.")
            return
        headers = values[0]
        rows = values[1:]
        df = pd.DataFrame(rows, columns=headers)
        before = len(df)
        if "PurchaseOrderId" in df.columns and "SkuId" in df.columns:
            df = df.drop_duplicates(subset=["PurchaseOrderId", "SkuId"], keep="first")
            after = len(df)
            removed = before - after
        else:
            print("⚠️ Warning: 'PurchaseOrderId' or 'SkuId' column not found, skipping duplicate removal.")
            removed = 0
            after = before
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=sheet_name
        ).execute()
        body = {"values": [headers] + df.values.tolist()}
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body=body
        ).execute()
        print(f"Removed {removed} duplicate rows. Final row count: {after}")
    except Exception as e:
        print(f"Error while removing duplicates: {str(e)}")

def main():
    FOLDER_ID = '1_Q-DC7WyBle-re4Y1avJhcZSievNTP5M'
    SPREADSHEET_ID = '1cIjurlePErCYfSCAkOC0z7FnMwsmIoBeGI47_Qk0pq8'
    SHEET_NAME = 'ninjutsu_grn'
    BATCH_SIZE = 50

    today = datetime.now().strftime('%Y-%m-%d')
    print(f"Enhanced Excel Reader v3.0 - Today's Files Only ({today})")
    print("Installing additional packages if needed...")
    
    packages_to_try = ['pyxlsb', 'xlwings', 'xlrd2', 'python-dateutil']
    for package in packages_to_try:
        try:
            __import__(package.replace('-', '_'))
            print(f"  {package} is available")
        except ImportError:
            if package != 'python-dateutil':
                print(f"  {package} not available - will skip related strategies")

    header_row = get_header_row_input()
    creds = authenticate()
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)

    print(f"\nSearching for Excel files created today ({today})...")
    excel_files = get_excel_files(drive_service, FOLDER_ID, page_size=BATCH_SIZE)
    
    if not excel_files:
        print(f"No Excel files found that were created today ({today}) in the specified folder.")
        print("Note: The script only processes files created today. If you need to process files from other dates, modify the date filter in the script.")
        return

    print(f"\nFound {len(excel_files)} Excel files created today:")
    for i, file in enumerate(excel_files[:10]):
        created_time = dateutil.parser.parse(file['createdTime']).strftime('%Y-%m-%d %H:%M:%S')
        print(f"  - {file['name']} (created: {created_time})")
    if len(excel_files) > 10:
        print(f"  ... and {len(excel_files) - 10} more files")

    print(f"\nProcessing with header row setting: {header_row if header_row != -1 else 'No headers (generic column names)'}")
    print(f"Batch size: {BATCH_SIZE} files per batch")

    # Check if sheet already has headers
    sheet_has_headers = False
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A1"
        ).execute()
        sheet_has_headers = bool(result.get('values', []))
    except:
        pass

    overall_successful_files = 0
    overall_failed_files = 0
    is_first_file = True

    for batch, batch_num, total_batches in process_files_in_batches(excel_files, BATCH_SIZE):
        batch_successful = 0
        batch_failed = 0
        
        for i, file in enumerate(batch, 1):
            file_index = ((batch_num - 1) * BATCH_SIZE) + i
            created_time = dateutil.parser.parse(file['createdTime']).strftime('%Y-%m-%d %H:%M:%S')
            print(f"\n[Batch {batch_num}/{total_batches}] [{i}/{len(batch)}] [Overall: {file_index}/{len(excel_files)}]")
            print(f"Processing: {file['name']} (created: {created_time})")
            
            df = read_excel_file(drive_service, file['id'], file['name'], header_row)
            
            if df.empty:
                print(f"  SKIPPED - No data extracted")
                batch_failed += 1
                continue
            
            try:
                print(f"  Data shape: {df.shape}")
                print(f"  Columns: {list(df.columns)[:3]}{'...' if len(df.columns) > 3 else ''}")
                
                # Only append headers for the first file if the sheet is empty
                append_headers = is_first_file and not sheet_has_headers
                append_to_sheet(sheets_service, SPREADSHEET_ID, SHEET_NAME, df, append_headers, sheet_has_headers)
                print(f"  APPENDED to Google Sheet successfully")
                batch_successful += 1
                is_first_file = False
                sheet_has_headers = True  # Once headers are written, assume sheet has headers
            except Exception as e:
                print(f"  FAILED to append to Google Sheet: {str(e)}")
                batch_failed += 1

        print(f"\n--- BATCH {batch_num} SUMMARY ---")
        print(f"Successfully processed: {batch_successful} files")
        print(f"Failed to process: {batch_failed} files")
        
        overall_successful_files += batch_successful
        overall_failed_files += batch_failed

    print(f"\n{'='*60}")
    print(f"FINAL RESULTS - ALL BATCHES COMPLETED")
    print(f"{'='*60}")
    print(f"Date processed: {today}")
    print(f"Total Excel files found (created today): {len(excel_files)}")
    print(f"Successfully processed: {overall_successful_files} files")
    print(f"Failed to process: {overall_failed_files} files")
    print(f"Header row used: {header_row if header_row != -1 else 'No headers'}")
    print(f"Files processed in {((len(excel_files) + BATCH_SIZE - 1) // BATCH_SIZE)} batches of {BATCH_SIZE}")
    
    if overall_successful_files > 0:
        print("\nRemoving duplicates from Google Sheet...")
        remove_duplicates_from_sheet(sheets_service, SPREADSHEET_ID, SHEET_NAME)
    
    if overall_failed_files > 0:
        print(f"\nTo improve success rate, consider:")
        print(f"1. Installing LibreOffice: sudo apt-get install libreoffice")
        print(f"2. Installing Gnumeric: sudo apt-get install gnumeric")
        print(f"3. Re-saving problematic files in Excel as .xlsx format")

if __name__ == '__main__':
    main()