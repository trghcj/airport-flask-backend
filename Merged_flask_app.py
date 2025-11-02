# Merged_flask_app.py — UPDATED
from flask import Flask, request, jsonify, make_response, send_file
from flask_cors import CORS
import pandas as pd
import io
import base64
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import os
import logging
from werkzeug.utils import secure_filename
import firebase_admin
from firebase_admin import credentials, firestore
from google.api_core import exceptions
import json
import time
import traceback
import sys
import pytz
import re
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet
import math
import random

# --------------------------
# Logging
# --------------------------
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s IST - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('flask.log'), logging.StreamHandler()],
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# --------------------------
# Root endpoint
# --------------------------
@app.route("/", methods=["GET"])
def home():
    return {
        "status": "ok",
        "message": "Flask backend is running successfully 🚀",
        "endpoints": ["/upload", "/analyze", "/download_dashboard_pdf", "/search", "/stats"]
    }

# --------------------------
# Date/time helper
# --------------------------
IST = pytz.timezone('Asia/Kolkata')
UTC = pytz.UTC
def now_ist_str():
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

# --------------------------
# CORS
# --------------------------
CORS(app, resources={r"/*": {"origins": "*", "supports_credentials": True}})

app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB
app.config['SECRET_KEY'] = os.urandom(24)

# --------------------------
# Firestore init (env JSON)
# --------------------------
def initialize_firestore():
    try:
        firebase_creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
        if not firebase_creds_json:
            logger.error("Missing GOOGLE_APPLICATION_CREDENTIALS_JSON env var")
            raise ValueError("Missing GOOGLE_APPLICATION_CREDENTIALS_JSON")

        cred_data = json.loads(firebase_creds_json)
        cred = credentials.Certificate(cred_data)

        try:
            firebase_admin.get_app()
            logger.info("Firebase app already initialized.")
        except ValueError:
            firebase_admin.initialize_app(cred, {'projectId': cred_data.get('project_id')})

        db = firestore.client()
        logger.info("Firestore initialized successfully.")
        return db
    except Exception as e:
        logger.error(f"Failed to initialize Firestore: {e}\n{traceback.format_exc()}")
        raise

db = initialize_firestore()

# --------------------------
# Safe write helper with simple retries
# --------------------------
def safe_set_doc(ref_callable, max_retries=4, delay=1.0):
    """ref_callable: zero-arg callable that performs the write (e.g. lambda: ref.set(data))"""
    attempt = 0
    while True:
        try:
            return ref_callable()
        except Exception as e:
            attempt += 1
            logger.warning(f"Firestore write failed (attempt {attempt}/{max_retries}): {e}")
            if attempt >= max_retries:
                logger.error("Max retries reached for Firestore write. Raising.")
                raise
            sleep_for = delay * (2 ** (attempt - 1)) + random.random() * 0.2
            time.sleep(sleep_for)

# --------------------------
# Excel date parsing helpers
# --------------------------
def parse_excel_serial_date(serial_num, hhmm_str=None):
    if pd.isna(serial_num) or serial_num is None:
        return None
    try:
        serial_num = float(serial_num)
        base_date = datetime(1899, 12, 30)
        date = base_date + timedelta(days=serial_num)
        # apply hhmm if provided
        if hhmm_str and not pd.isna(hhmm_str):
            hhmm = str(hhmm_str).replace(":", "").strip()
            if hhmm.isdigit() and len(hhmm) == 4:
                hrs = int(hhmm[:2]); mins = int(hhmm[2:])
                if 0 <= hrs <= 23 and 0 <= mins <= 59:
                    date = date.replace(hour=hrs, minute=mins, second=0, microsecond=0)
        return UTC.localize(date)
    except Exception:
        logger.debug(f"parse_excel_serial_date failed for {serial_num}, {hhmm_str}")
        return None

def safe_iso_to_dt(s):
    if not s or not isinstance(s, str):
        return None
    try:
        # python 3.11+: fromisoformat supports offset; works for "YYYY-MM-DDTHH:MM:SS+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None

# --------------------------
# Utility helpers
# --------------------------
def clean_out_of_range(df):
    for col in df.columns:
        mask = df[col].apply(lambda x: isinstance(x, (int, float)) and (abs(x) > 1e10 or math.isnan(x)))
        if mask.any():
            df.loc[mask, col] = None
    return df

def normalize_column_name(col):
    col = str(col).strip()
    col_key = re.sub(r'[^\w\s\.]', '', col).strip()
    mapping = {
        'Operator': 'Operator_Name', 'Operator Name': 'Operator_Name',
        'Reg No.': 'Reg_No', 'Reg No': 'Reg_No', 'Reg_No': 'Reg_No',
        'Landing': 'Landing', 'UDF Charge': 'UDF_Charge',
        'Arr Date': 'Arr_Date', 'Dep Date': 'Dep_Date', 'Arr GMT': 'Arr_GMT', 'Dep GMT': 'Dep_GMT',
        'Aircraft Type': 'Aircraft_Type', 'Departure Location': 'Dep_Location', 'Dest Location': 'Dest_Location',
        'Airport Name': 'Airport_Name', 'Region': 'Region'
    }
    for k, v in mapping.items():
        if k.lower() == col_key.lower() or k.lower() in col_key.lower():
            return v
    # fallback
    newkey = re.sub(r'[\s\./&]+', '_', col_key)
    newkey = re.sub(r'_+', '_', newkey).strip('_')
    return newkey or col_key

# --------------------------
# Process Excel file (big function) — cleaned/consistent
# --------------------------
def process_excel_file(file_obj, file_type='departure', filename="upload.xlsx"):
    """
    Reads an uploaded Excel file and returns structured analysis results.
    - file_type: 'departure' or 'base'
    """
    try:
        stream = io.BytesIO(file_obj.read())
        if stream.getbuffer().nbytes == 0:
            return {"error": "Empty file"}

        excel = pd.ExcelFile(stream, engine='openpyxl')
        result = {}

        for sheet in excel.sheet_names:
            logger.info(f"Processing sheet {sheet} from {filename}")
            stream.seek(0)

            if file_type == 'departure':
                # Aggressive header handling: skip top meta lines (if needed)
                skiprows = 3  # tuned for your files; adjust if necessary
                df = pd.read_excel(stream, sheet_name=sheet, header=None, skiprows=skiprows, engine='openpyxl')
                # Map to expected departure columns list (trim/expand as required)
                departure_cols = [
                    'SL_No','Airport_Code','Airport_Name','Region','Profit_Center','Operator_Name',
                    'CA12_No','Reg_No','Max_Allup_Wt','Seating_Capacity','Helicopter','Aircraft_Type',
                    'Arr_Date','Arr_GMT','Arr_Flight_No','Dep_Location','Arr_Nature','Arr_GCD','Arr_Sch',
                    'Arr_RCS_Status','Arr_RCS_Category','Dep_Date','Dep_GMT','Dep_Flight_No','Dest_Location',
                    'Dep_Nature','Dep_GCD','Dep_Sch','Dep_RCS_Status','Dep_RCS_Category','Credit_Facility',
                    'Operator_Type','Landing','Parking','Open_Parking','Housing','RNFC','TNLC','Arr_Watch',
                    'Dep_Watch','Counter','XRay','UDF_Charge','OLD_IN_PAX','OLD_US_PAX','NEW_IN_PAX','NEW_US_PAX',
                    'OLD_IN_RATE','OLD_US_RATE','NEW_IN_RATE','NEW_US_RATE','Unique_Id','Arr_Bill_Status',
                    'Dep_Bill_Status','UDF_Bill_Status'
                ]
                if len(df.columns) >= len(departure_cols):
                    df = df.iloc[:, :len(departure_cols)]
                    df.columns = departure_cols
                else:
                    # fallback: try reading with header row and normalizing columns
                    stream.seek(0)
                    df = pd.read_excel(stream, sheet_name=sheet, header=0, engine='openpyxl')
                    df.columns = [normalize_column_name(c) for c in df.columns]
            else:
                stream.seek(0)
                df = pd.read_excel(stream, sheet_name=sheet, header=0, engine='openpyxl')
                df.columns = [normalize_column_name(c) for c in df.columns]

            if df.empty or df.columns.empty:
                result[sheet] = {"error": "Empty sheet"}
                continue

            # Ensure minimal columns exist
            if 'Operator_Name' not in df.columns:
                # try to infer possible operator-like column names
                found = False
                for c in df.columns:
                    if 'operator' in c.lower():
                        df = df.rename(columns={c: 'Operator_Name'})
                        found = True
                        break
                if not found:
                    logger.warning(f"Operator_Name missing in {sheet}, marking rows Unknown")
                    df['Operator_Name'] = 'Unknown'

            # Pre-clean numeric columns
            df = clean_out_of_range(df)

            processed_rows = []
            if file_type == 'departure':
                # parse date columns (serial + hhmm)
                df['Arr_Datetime_GMT'] = df.apply(lambda r: parse_excel_serial_date(r.get('Arr_Date'), r.get('Arr_GMT')), axis=1)
                df['Dep_Datetime_GMT'] = df.apply(lambda r: parse_excel_serial_date(r.get('Dep_Date'), r.get('Dep_GMT')), axis=1)

                numeric_cols = ['Landing','Parking','Open_Parking','Housing','RNFC','TNLC','Arr_Watch','Dep_Watch','Counter','XRay','UDF_Charge']
                for c in numeric_cols:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)

                for idx, row in df.iterrows():
                    arr_dt = row.get('Arr_Datetime_GMT')
                    dep_dt = row.get('Dep_Datetime_GMT')
                    airtime_hours = 0.0
                    if arr_dt and dep_dt and isinstance(arr_dt, datetime) and isinstance(dep_dt, datetime):
                        airtime_hours = abs((dep_dt - arr_dt).total_seconds() / 3600.0)
                    # robust operator/region
                    raw_op = row.get('Operator_Name', '')
                    operator_name = str(raw_op).strip() if (raw_op is not None and str(raw_op).strip() != '') else 'Unknown'
                    if operator_name.upper() in ('N/A','NA','-'):
                        operator_name = 'Unknown'
                    raw_region = row.get('Region', '')
                    region = str(raw_region).strip() if (raw_region is not None and str(raw_region).strip() != '') else 'Unknown'
                    if region.upper() in ('N/A','NA','-'):
                        region = 'Unknown'

                    # Unique / Reg
                    raw_reg = row.get('Reg_No', '')
                    reg_no = str(raw_reg).strip() if (raw_reg is not None and str(raw_reg).strip() != '') else 'Unknown'

                    # Create processed row
                    processed = {
                        'Unique_Id': f"FLIGHT_{idx}_{int(time.time())}",
                        'Arr_Datetime_GMT': arr_dt.isoformat() if isinstance(arr_dt, datetime) else "",
                        'Dep_Datetime_GMT': dep_dt.isoformat() if isinstance(dep_dt, datetime) else "",
                        'Arr_Local': arr_dt.astimezone(IST).isoformat() if isinstance(arr_dt, datetime) else "",
                        'Dep_Local': dep_dt.astimezone(IST).isoformat() if isinstance(dep_dt, datetime) else "",
                        'Dep_Location': str(row.get('Dep_Location','')) or '',
                        'Dest_Location': str(row.get('Dest_Location','')) or '',
                        'Airport_Name': str(row.get('Airport_Name','')) or '',
                        'Operator_Name': operator_name,
                        'Region': region,
                        'Aircraft_Type': str(row.get('Aircraft_Type','')) or 'Unknown',
                        'Reg_No': reg_no,
                        'Airtime_Hours': float(f"{airtime_hours:.2f}") if airtime_hours is not None else 0.0,
                        'Linkage_Status': 'Same' if str(row.get('Dep_Location','')) == str(row.get('Dest_Location','')) else 'Different',
                        'Landing': float(row.get('Landing', 0.0) or 0.0),
                        'Parking': float(row.get('Parking', 0.0) or 0.0),
                        'UDF_Charge': float(row.get('UDF_Charge', 0.0) or 0.0),
                        'Arr_Bill_Status': row.get('Arr_Bill_Status', 'unbilled'),
                        'Dep_Bill_Status': row.get('Dep_Bill_Status', 'unbilled'),
                        'UDF_Bill_Status': row.get('UDF_Bill_Status', 'unbilled'),
                        'file_type': 'departure',
                        'source_sheet': sheet
                    }
                    processed_rows.append(processed)
            else:
                # base file
                for idx, row in df.iterrows():
                    processed = {
                        'Unique_Id': f"BASE_{idx}_{int(time.time())}",
                        'Operator_Name': str(row.get('Operator_Name','Unknown')) or 'Unknown',
                        'Assessment': float(row.get('Assessment', 0.0) or 0.0),
                        'Realisation': float(row.get('Realisation', 0.0) or 0.0),
                        'Closing_Balance': float(row.get('Closing_Balance', 0.0) or 0.0),
                        'Fleet_Count': float(row.get('Fleet_Count', 0.0) or 0.0),
                        'file_type': 'base',
                        'source_sheet': sheet
                    }
                    processed_rows.append(processed)

            # convert to DataFrame and summary
            uploaded_df = pd.DataFrame(processed_rows)
            if uploaded_df.empty:
                result[sheet] = {"error": "No rows processed"}
                continue

            # Generate charts optionally
            chart_bar_b64 = ""
            chart_pie_b64 = ""
            try:
                if 'Operator_Name' in uploaded_df.columns and 'Landing' in uploaded_df.columns:
                    plt.figure(figsize=(8,4))
                    agg = uploaded_df.groupby('Operator_Name')['Landing'].sum().nlargest(10)
                    if not agg.empty:
                        agg.plot(kind='bar')
                        plt.tight_layout()
                        buf = io.BytesIO(); plt.savefig(buf, format='png'); plt.close()
                        chart_bar_b64 = base64.b64encode(buf.getvalue()).decode('utf-8'); buf.close()
                if 'Aircraft_Type' in uploaded_df.columns:
                    counts = uploaded_df['Aircraft_Type'].value_counts().head(6)
                    if not counts.empty:
                        plt.figure(figsize=(6,6)); plt.pie(counts.astype(float), labels=counts.index, autopct='%1.1f%%'); plt.tight_layout()
                        buf = io.BytesIO(); plt.savefig(buf, format='png'); plt.close()
                        chart_pie_b64 = base64.b64encode(buf.getvalue()).decode('utf-8'); buf.close()
            except Exception as e:
                logger.warning(f"Chart generation failed: {e}")

            # prepare stats
            stats = {}
            if not uploaded_df.empty:
                stats['total_records'] = len(uploaded_df)
                if 'Operator_Name' in uploaded_df.columns:
                    stats['unique_operators'] = int(uploaded_df['Operator_Name'].nunique())
                    try:
                        stats['top_operator'] = uploaded_df['Operator_Name'].value_counts().idxmax()
                    except Exception:
                        stats['top_operator'] = None
                if 'Airtime_Hours' in uploaded_df.columns:
                    stats['avg_airtime'] = float(uploaded_df['Airtime_Hours'].astype(float).mean()) if len(uploaded_df) > 0 else 0.0

            # Firestore-safe JSON conversion: convert datetimes to strings already handled above
            doc_id = f"analysis_{file_type}_{sheet}_{int(time.time())}"
            main_doc = {
                'sheet_name': sheet,
                'file_type': file_type,
                'columns': uploaded_df.columns.tolist(),
                'rows': uploaded_df.fillna('').to_dict(orient='records')[:200],
                'stats': stats,
                'chart_bar': chart_bar_b64,
                'chart_pie': chart_pie_b64,
                'formal_summary': f"Processed {len(uploaded_df)} rows for {sheet}",
                'timestamp': firestore.SERVER_TIMESTAMP,
                'total_records': len(uploaded_df)
            }

            # Save main doc
            def do_set_main():
                db.collection("analysis_results").document(doc_id).set(main_doc)
            safe_set_doc(do_set_main)

            # Save chunked data under collection/data
            data_records = uploaded_df.fillna('').to_dict(orient='records')
            chunk_size = 500
            for i in range(0, len(data_records), chunk_size):
                chunk = data_records[i:i+chunk_size]
                sub_id = f"data_chunk_{i//chunk_size}"
                def do_set_chunk(chunk=chunk, sub_id=sub_id):
                    db.collection("analysis_results").document(doc_id).collection("data").document(sub_id).set({'records': chunk})
                safe_set_doc(lambda c=chunk, s=sub_id: do_set_chunk())

            result[sheet] = {
                'doc_id': doc_id,
                'sheet_name': sheet,
                'rows': uploaded_df.head(100).fillna('').to_dict(orient='records'),
                'stats': stats,
                'chart_bar': chart_bar_b64,
                'chart_pie': chart_pie_b64
            }

        return result
    except Exception as e:
        logger.error(f"process_excel_file error: {e}\n{traceback.format_exc()}")
        return {"error": str(e), "details": traceback.format_exc()}

# --------------------------
# Upload endpoint
# --------------------------
@app.route('/upload', methods=['POST', 'OPTIONS'])
def upload():
    if request.method == 'OPTIONS':
        resp = make_response('', 204)
        resp.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin', '*')
        resp.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return resp

    departure_files = request.files.getlist('departure_files[]') or request.files.getlist('departure_files') or []
    if not departure_files:
        return make_response(jsonify({'success': False, 'error': 'No files provided under departure_files[]'}), 400)

    try:
        batch_doc_id = f"analysis_departure_batch_{int(time.time())}"
        all_processed = []
        all_stats = {'total_flights': 0}
        chart_bar = chart_pie = ""

        for f in departure_files:
            if not f.filename.lower().endswith(('.xlsx','.xls')):
                logger.info(f"Skipping non-excel file {f.filename}")
                continue
            sheet_res = process_excel_file(f, file_type='departure', filename=f.filename)
            # Merge results
            for sheet_name, sheet_data in sheet_res.items():
                if 'error' in sheet_data:
                    # include sheet error in response
                    logger.warning(f"sheet processing error: {sheet_name} -> {sheet_data.get('error')}")
                else:
                    # read the doc we just saved for data: sheet_data['doc_id']
                    doc_id = sheet_data.get('doc_id')
                    # append stats
                    stats = sheet_data.get('stats', {})
                    all_stats['total_flights'] = all_stats.get('total_flights', 0) + stats.get('total_records', 0)
                    if not chart_bar and sheet_data.get('chart_bar'):
                        chart_bar = sheet_data.get('chart_bar')
                    if not chart_pie and sheet_data.get('chart_pie'):
                        chart_pie = sheet_data.get('chart_pie')

        # return batch id (we saved each sheet separately already)
        return make_response(jsonify({'success': True, 'message': 'Files processed', 'batch_doc_id': batch_doc_id}), 200)
    except Exception as e:
        logger.error(f"/upload failed: {e}\n{traceback.format_exc()}")
        return make_response(jsonify({'success': False, 'error': str(e)}), 500)

# --------------------------
# Analyze endpoint (single base file)
# --------------------------
@app.route('/analyze', methods=['POST','OPTIONS'])
def analyze():
    if request.method == 'OPTIONS':
        resp = make_response('', 204)
        resp.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin', '*')
        return resp

    base_file = request.files.get('base_file')
    if not base_file or not base_file.filename or not base_file.filename.lower().endswith(('.xlsx','.xls')):
        return make_response(jsonify({'success': False, 'error': 'Valid base Excel file is required (base_file)'}), 400)
    try:
        res = process_excel_file(base_file, file_type='base', filename=base_file.filename)
        return make_response(jsonify({'success': True, 'result': res}), 200)
    except Exception as e:
        logger.error(f"/analyze failed: {e}\n{traceback.format_exc()}")
        return make_response(jsonify({'success': False, 'error': str(e)}), 500)

# --------------------------
# Search endpoint (UPDATED to handle nested dicts)
# --------------------------
@app.route('/search', methods=['GET', 'OPTIONS'])
def search():
    if request.method == 'OPTIONS':
        resp = make_response('', 204)
        resp.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin', '*')
        resp.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
        resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return resp

    doc_id = request.args.get('doc_id')
    query = (request.args.get('query') or '').strip().lower()

    if not doc_id:
        return make_response(jsonify({'error': 'doc_id is required'}), 400)

    try:
        doc_ref = db.collection('stats_upload').document(doc_id)
        doc = doc_ref.get()

        if not doc.exists:
            return make_response(jsonify({'error': 'No data found for doc_id'}), 404)

        data = doc.to_dict() or {}

        # 🔹 Recursively flatten nested dicts for searching
        def flatten_dict(d, parent_key='', sep='.'):
            items = {}
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.update(flatten_dict(v, new_key, sep=sep))
                else:
                    items[new_key] = v
            return items

        flat_data = flatten_dict(data)

        # 🔹 Search through flattened data (case-insensitive)
        matches = {}
        for key, value in flat_data.items():
            if isinstance(value, str) and query in value.lower():
                matches[key] = value
            elif isinstance(value, (int, float)) and str(value).lower() == query:
                matches[key] = value

        # 🔹 Return appropriate JSON response
        if matches:
            return make_response(jsonify({
                'matches': matches,
                'full_data': data
            }), 200)
        else:
            return make_response(jsonify({
                'message': 'No match found for query',
                'data': data
            }), 200)

    except Exception as e:
        logger.error(f"/search error: {e}\n{traceback.format_exc()}")
        return make_response(jsonify({'error': str(e)}), 500)

# --------------------------
# List available Firestore documents (for debugging)
# --------------------------
@app.route('/list_docs', methods=['GET'])
def list_docs():
    docs = db.collection('stats_upload').stream()
    ids = [d.id for d in docs]
    return jsonify({'count': len(ids), 'doc_ids': ids})
 
# --------------------------
# Stats endpoint
# --------------------------
@app.route('/stats', methods=['GET','OPTIONS'])
def stats():
    if request.method == 'OPTIONS':
        resp = make_response('', 204)
        resp.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin', '*')
        return resp

    doc_id = request.args.get('doc_id')
    group_by = (request.args.get('group_by') or 'operator').lower()

    if not doc_id:
        return make_response(jsonify({'error': 'doc_id is required'}), 400)

    try:
        coll = db.collection("analysis_results").document(doc_id).collection("data")
        docs = coll.get()
        if not docs:
            return make_response(jsonify({'error': 'No data found for doc_id'}), 404)

        rows = []
        for d in docs:
            rows += d.to_dict().get('records', [])

        # group
        summary = {}
        if group_by == 'operator':
            for r in rows:
                if r.get('file_type') != 'departure':
                    continue
                op = r.get('Operator_Name') or 'Unknown'
                op = op if str(op).strip() != '' else 'Unknown'
                s = summary.setdefault(op, {
                    'Operator_Name': op,
                    'Region': r.get('Region','Unknown'),
                    'Flight_Count': 0,
                    'Total_Airtime_Hours': 0.0,
                    'Same_Linkage_Count': 0,
                    'Different_Linkage_Count': 0,
                    'Arr_Billed_Count': 0,
                    'Arr_Unbilled_Count': 0,
                    'Total_Landing_Charges': 0.0,
                    'Total_UDF_Charges': 0.0
                })
                s['Flight_Count'] += 1
                ah = float(r.get('Airtime_Hours') or 0.0)
                s['Total_Airtime_Hours'] += ah
                if r.get('Linkage_Status') == 'Same':
                    s['Same_Linkage_Count'] += 1
                else:
                    s['Different_Linkage_Count'] += 1
                s['Arr_Billed_Count'] += 1 if r.get('Arr_Bill_Status') == 'billed' else 0
                s['Arr_Unbilled_Count'] += 1 if r.get('Arr_Bill_Status') == 'unbilled' else 0
                s['Total_Landing_Charges'] += float(r.get('Landing') or 0.0)
                s['Total_UDF_Charges'] += float(r.get('UDF_Charge') or 0.0)
            # finalize avg airtime
            out = []
            for k,v in summary.items():
                v['Avg_Airtime_Hours'] = round((v['Total_Airtime_Hours']/v['Flight_Count']) if v['Flight_Count']>0 else 0.0, 2)
                out.append(v)
            return make_response(jsonify(out), 200)

        elif group_by == 'region':
            for r in rows:
                if r.get('file_type') != 'departure':
                    continue
                region = r.get('Region') or 'Unknown'
                region = region if str(region).strip() != '' else 'Unknown'
                s = summary.setdefault(region, {
                    'Region': region,
                    'Flight_Count': 0,
                    'Total_Airtime_Hours': 0.0,
                    'Total_Landing_Charges': 0.0,
                    'Total_UDF_Charges': 0.0
                })
                s['Flight_Count'] += 1
                s['Total_Airtime_Hours'] += float(r.get('Airtime_Hours') or 0.0)
                s['Total_Landing_Charges'] += float(r.get('Landing') or 0.0)
                s['Total_UDF_Charges'] += float(r.get('UDF_Charge') or 0.0)
            out = []
            for k,v in summary.items():
                v['Avg_Airtime_Hours'] = round((v['Total_Airtime_Hours']/v['Flight_Count']) if v['Flight_Count']>0 else 0.0, 2)
                out.append(v)
            return make_response(jsonify(out), 200)

        elif group_by == 'airport':
            for r in rows:
                if r.get('file_type') != 'departure':
                    continue
                ap = r.get('Airport_Name') or 'Unknown'
                s = summary.setdefault(ap, {'Airport_Name': ap, 'Flight_Count': 0, 'Total_Landing_Charges': 0.0})
                s['Flight_Count'] += 1
                s['Total_Landing_Charges'] += float(r.get('Landing') or 0.0)
            return make_response(jsonify(list(summary.values())), 200)

        else:
            return make_response(jsonify({'error': f'Unsupported group_by: {group_by}'}), 400)
    except Exception as e:
        logger.error(f"/stats error: {e}\n{traceback.format_exc()}")
        return make_response(jsonify({'error': str(e)}), 500)

# --------------------------
# Download PDF endpoint
# --------------------------
@app.route('/download_dashboard_pdf', methods=['GET','OPTIONS'])
def download_dashboard_pdf():
    if request.method == 'OPTIONS':
        resp = make_response('', 204)
        resp.headers['Access-Control-Allow-Origin'] = request.headers.get('Origin', '*')
        return resp

    doc_id = request.args.get('doc_id')
    if not doc_id:
        return make_response(jsonify({'error': 'doc_id required'}), 400)
    try:
        doc_ref = db.collection("analysis_results").document(doc_id)
        doc = doc_ref.get()
        if not doc.exists:
            return make_response(jsonify({'error': 'doc not found'}), 404)
        data = doc.to_dict()
        stats = data.get('stats', {})
        chart_bar = data.get('chart_bar', '')
        chart_pie = data.get('chart_pie', '')

        buffer = io.BytesIO()
        doc_pdf = SimpleDocTemplate(buffer, pagesize=letter)
        styles = getSampleStyleSheet()
        elements = []
        elements.append(Paragraph(f"{data.get('sheet_name','Dashboard')} Report", styles['Title']))
        elements.append(Spacer(1,12))
        elements.append(Paragraph(data.get('formal_summary',''), styles['Normal']))
        elements.append(Spacer(1,12))

        table_data = [['Metric','Value']]
        for k,v in stats.items():
            table_data.append([str(k), str(v)])
        t = Table(table_data)
        t.setStyle([('BACKGROUND',(0,0),(-1,0),colors.grey),('TEXTCOLOR',(0,0),(-1,0),colors.whitesmoke),
                    ('ALIGN',(0,0),(-1,-1),'CENTER'),('GRID',(0,0),(-1,-1),1,colors.black)])
        elements.append(t)
        elements.append(Spacer(1,12))

        if chart_bar:
            try:
                bbuf = io.BytesIO(base64.b64decode(chart_bar))
                elements.append(Paragraph("Bar Chart", styles['Heading2']))
                elements.append(Spacer(1,6))
                elements.append(Image(bbuf, width=450, height=250))
                elements.append(Spacer(1,12))
                bbuf.close()
            except Exception as e:
                logger.warning(f"Failed to include bar chart: {e}")

        if chart_pie:
            try:
                pbuf = io.BytesIO(base64.b64decode(chart_pie))
                elements.append(Paragraph("Pie Chart", styles['Heading2']))
                elements.append(Spacer(1,6))
                elements.append(Image(pbuf, width=350, height=350))
                elements.append(Spacer(1,12))
                pbuf.close()
            except Exception as e:
                logger.warning(f"Failed to include pie chart: {e}")

        doc_pdf.build(elements)
        buffer.seek(0)
        return send_file(buffer, as_attachment=True, download_name=f"dashboard_{doc_id}.pdf", mimetype='application/pdf')
    except Exception as e:
        logger.error(f"/download_dashboard_pdf error: {e}\n{traceback.format_exc()}")
        return make_response(jsonify({'error': str(e)}), 500)

# --------------------------
# Run
# --------------------------
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5003))
    app.run(host='0.0.0.0', port=port, debug=False)
