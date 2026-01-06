import logging
import pandas as pd
import matplotlib
# Backend Agg wajib untuk server (Render/VPS) agar tidak error GUI
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from telegram import Update, InputFile
from telegram.ext import Application, MessageHandler, filters, CommandHandler, ContextTypes
from telegram.request import HTTPXRequest
import io
import os
import json
from fastapi import FastAPI, Request, Response
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

# --- LIBRARY GOOGLE SHEETS (Opsional, hanya jika diaktifkan) ---
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    HAS_GSPREAD = True
except ImportError:
    HAS_GSPREAD = False

# ==========================================
# 1. KONFIGURASI UTAMA
# ==========================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- TOKEN & WEBHOOK ---
BOT_TOKEN = "8330450329:AAGEPd2j2a1dZ1PEJ7BrneykiZ-3Kv1T3LI"
WEBHOOK_URL = "https://psbiqbal.onrender.com"
WEBHOOK_PATH = "/telegram"

# --- PENGATURAN FITUR ---
# Ubah ke True jika Anda ingin bot mengupdate Google Sheet
# Pastikan file credentials.json ada di folder yang sama.
ENABLE_GOOGLE_SHEETS = True 

CREDENTIALS_FILE = "credentials.json"
KPRO_TARGET_SHEET_URL = "https://docs.google.com/spreadsheets/d/1wPeYLmInP7JlPCLZ1XYR-A75l9oHQfZ_U2R4Pc6ohVY/"
KPRO_TARGET_SHEET_NAME = "REPORT PS INDIHOME"
KPRO_MICRO_UPDATE_SHEET_NAME = "UPDATE PER 2JAM"

# ==========================================
# 2. FUNGSI GOOGLE SHEETS
# ==========================================
def get_gspread_client():
    if not HAS_GSPREAD: return None
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        if os.path.exists(CREDENTIALS_FILE):
            creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
            return gspread.authorize(creds)
        else:
            logger.warning("File credentials.json tidak ditemukan.")
            return None
    except Exception as e:
        logger.error(f"Google Auth Error: {e}")
        return None

# MAPPING DATA UNTUK GOOGLE SHEET
KPRO_STO_ROW_MAP = {'CID': 9, 'CPP': 10, 'GBC': 11, 'GBI': 12, 'KMY': 13}
KPRO_COLUMN_INDEX_MAP = {
    'RE HI': 9, 'LEWAT MANJA': 10, 'MANJA HI': 11, 'MANJA H+': 12, 'MANJA H++': 13,
    'KENDALA PELANGGAN': 15, 'KENDALA JARINGAN': 16, 'POTENSI PI ORBIT': 18,
    'VALSTART ENDSATATE': 23, 'PS ENDSTATE': 24, 'EST PS H-1': 28, 'EST PS W-1': 29, 'PS ORBIT HI': 30,
}
KPRO_MICRO_STO_ROW_MAP = {'CID': 10, 'CPP': 11, 'GBC': 12, 'GBI': 13, 'KMY': 14}
KPRO_MICRO_COLUMN_INDEX_MAP = {
    'STARTWORK': 4, 'PENDWORK': 5, 'CONTWORK': 6, 'INSTCOMP': 7, 'ACTCOMP': 8,
    'VALSTART': 9, 'VALCOMP': 10, 'WORKFAIL': 11, 'CANCLWORK': 12, 'COMPWORK': 13, 'TOTAL WO': 14
}

# ==========================================
# 3. LOGIKA DASHBOARD GAMBAR (Visualisasi)
# ==========================================
def create_summary_text(status_counts: pd.Series) -> str:
    def get_count(s): return status_counts.get(s, 0)
    ps = get_count('COMPWORK')
    acom = get_count('ACOMP') + get_count('VALSTART') + get_count('VALCOMP')
    pi = get_count('STARTWORK')
    pi_progress = get_count('INSTCOMP') + get_count('CONTWORK') + get_count('PENDWORK')
    kendala = get_count('WORKFAIL')
    est_ps = ps + acom
    return (
        f"Ringkasan Metrik Harian:\n--------------------------------------\n"
        f"PS (COMPWORK)                 = {ps}\n"
        f"ACOM (ACOMP+VALSTART+VALCOMP) = {acom}\n"
        f"PI (STARTWORK)                  = {pi}\n"
        f"PI PROGRESS (INSTCOMP+PENDWORK+CONTWORK) = {pi_progress}\n"
        f"KENDALA (WORKFAIL)              = {kendala}\n"
        f"EST PS (PS+ACOM)                = {est_ps}"
    )

def create_integrated_dashboard(daily_df: pd.DataFrame, report_timestamp: datetime, status_counts: pd.Series) -> io.BytesIO:
    stos = sorted(daily_df['STO'].unique())
    status_order = ['CANCLWORK', 'COMPWORK', 'ACOMP', 'VALCOMP', 'VALSTART', 'STARTWORK', 'INSTCOMP', 'PENDWORK', 'CONTWORK', 'WORKFAIL']
    table_data, row_styles = [], {}
    unique_statuses = daily_df['STATUS'].unique()
    
    for status in status_order:
        if status not in unique_statuses: continue
        status_df = daily_df[daily_df['STATUS'] == status]
        row_data = {'KATEGORI': status}
        for sto in stos: row_data[sto] = len(status_df[status_df['STO'] == sto])
        table_data.append(row_data); row_styles[len(table_data) - 1] = {'level': 1, 'status': status}

        if status == 'WORKFAIL':
            for error_code, error_group in status_df.groupby('ERRORCODE'):
                if str(error_code).upper() in ['NAN', 'N/A']: continue
                error_row = {'KATEGORI': f"  ↳ {error_code}"}
                for sto in stos: error_row[sto] = len(error_group[error_group['STO'] == sto])
                table_data.append(error_row); row_styles[len(table_data) - 1] = {'level': 2, 'status': status}
                for sub_e, sub_g in error_group.groupby('SUBERRORCODE'):
                    if str(sub_e).upper() in ['NAN', 'N/A']: continue
                    sub_row = {'KATEGORI': f"    → {sub_e}"}
                    for sto in stos: sub_row[sto] = len(sub_g[sub_g['STO'] == sto])
                    if sum(list(sub_row.values())[1:]) > 0:
                        table_data.append(sub_row); row_styles[len(table_data) - 1] = {'level': 3, 'status': status}

    if not table_data:
        fig, ax = plt.subplots(figsize=(10, 3)); ax.axis('off')
        fig.suptitle(f"DATA KOSONG/TIDAK RELEVAN", fontsize=16)
        img = io.BytesIO(); plt.savefig(img, format='png'); img.seek(0); plt.close(fig); return img

    display_df = pd.DataFrame(table_data, columns=['KATEGORI'] + stos).fillna(0)
    display_df['Grand Total'] = display_df[stos].sum(axis=1)
    
    # Render Tabel
    fig = plt.figure(figsize=(12, len(display_df) * 0.5 + 4.5))
    gs = fig.add_gridspec(3, 1, height_ratios=[1.5, len(display_df), 5])
    ax_title = fig.add_subplot(gs[0]); ax_title.axis('off')
    ax_table = fig.add_subplot(gs[1]); ax_table.axis('off')
    ax_text = fig.add_subplot(gs[2]); ax_text.axis('off')
    
    ax_title.text(0.05, 0.95, f"REPORT DAILY ENDSTATE JAKPUS - {report_timestamp.strftime('%d %B %Y %H:%M:%S').upper()}", 
                  ha='left', va='top', fontsize=16, weight='bold', color='#2F3E46')
    
    col_widths = [0.35] + [0.08] * (len(stos) + 1)
    table = ax_table.table(cellText=display_df.values, colLabels=display_df.columns, 
                           loc='center', cellLoc='center', colWidths=col_widths)
    table.auto_set_font_size(False); table.set_fontsize(10); table.scale(1, 2)
    
    # Styling Warna
    color_map = {
        'COMPWORK': ('#556B2F', 'white'), 'ACOMP': ('#9ACD32', 'black'), 'VALCOMP': ('#9ACD32', 'black'), 
        'VALSTART': ('#9ACD32', 'black'), 'WORKFAIL': ('#FF8C00', 'white'), 'KENDALA_ERROR': ('#FFDAB9', 'black'), 
        'STARTWORK': ('#FFFACD', 'black'), 'CANCLWORK': ('#DC143C', 'white'), 'Total': ('#F5F5F5', 'black')
    }
    for (row_idx, col_idx), cell in table.get_celld().items():
        cell.set_edgecolor('#D3D3D3'); cell.set_linewidth(0.8)
        if row_idx == 0:
            cell.set_facecolor('#404040'); cell.set_text_props(color='white', weight='bold'); continue
        style = row_styles.get(row_idx - 1, {}); status_key = style.get('status')
        if style.get('level') == 2 and status_key == 'WORKFAIL': status_key = 'KENDALA_ERROR'
        bg, txt = color_map.get(status_key, ('white', 'black'))
        cell.set_facecolor(bg); cell.get_text().set_color(txt)
        if col_idx > 0: cell.set_text_props(weight='bold')

    ax_text.text(0.05, 0.9, create_summary_text(status_counts), ha='left', va='top', fontsize=10, family='monospace')
    img = io.BytesIO(); plt.savefig(img, format='png', dpi=200, bbox_inches='tight'); img.seek(0); plt.close(fig)
    return img

# ==========================================
# 4. LOGIKA INTEGRASI KPRO (Google Sheet)
# ==========================================
async def process_kpro_logic(raw_df):
    """Fungsi pembantu untuk memproses logika update Sheet & Report Teks"""
    if not ENABLE_GOOGLE_SHEETS: return False, "", {}

    client = get_gspread_client()
    if not client: return False, "⚠️ Gagal koneksi Google Sheet (Credentials Missing).", {}

    msg = []
    # --- HITUNG KPI KPRO ---
    wib_tz = timezone(timedelta(hours=7)); today = datetime.now(wib_tz).date()
    yesterday = today - timedelta(days=1); seven_days = today - timedelta(days=6)
    
    raw_df['DATECREATED'] = pd.to_datetime(raw_df['DATECREATED'], errors='coerce')
    raw_df['STATUSDATE'] = pd.to_datetime(raw_df['STATUSDATE'], errors='coerce')
    raw_df['TGL_MANJA'] = pd.to_datetime(raw_df['TGL_MANJA'], errors='coerce')
    
    # 1. Update Checkpoint Sheet
    try:
        sh = client.open_by_url(KPRO_TARGET_SHEET_URL)
        ws = sh.worksheet(KPRO_TARGET_SHEET_NAME)
        updates = []
        
        for sto, row in KPRO_STO_ROW_MAP.items():
            sto_df = raw_df[raw_df['STO'] == sto]
            # Logika Hitungan
            is_today = sto_df['STATUSDATE'].dt.date == today
            is_created_today = sto_df['DATECREATED'].dt.date == today
            
            val_map = {
                'RE HI': len(sto_df[is_created_today]),
                'PS ENDSTATE': len(sto_df[is_today & (sto_df['STATUS'] == 'COMPWORK')]),
                'VALSTART ENDSATATE': len(sto_df[is_today & sto_df['STATUS'].isin(['INSTCOMP','ACTCOMP','VALCOMP','VALSTART'])]),
                'EST PS H-1': len(sto_df[(sto_df['STATUSDATE'].dt.date == yesterday) & (sto_df['STATUS'] == 'COMPWORK')]),
                'EST PS W-1': len(sto_df[(sto_df['STATUSDATE'].dt.date >= seven_days) & (sto_df['STATUSDATE'].dt.date <= today) & (sto_df['STATUS'] == 'COMPWORK')]),
                # Manja
                'MANJA HI': len(sto_df[(sto_df['STATUS']=='STARTWORK') & (sto_df['TGL_MANJA'].dt.date == today)]),
                'LEWAT MANJA': len(sto_df[(sto_df['STATUS']=='STARTWORK') & (sto_df['TGL_MANJA'].dt.date < today)]),
                # Kendala
                'KENDALA PELANGGAN': len(sto_df[(sto_df['STATUS']=='WORKFAIL') & (sto_df['ERRORCODE']=='KENDALA PELANGGAN')]),
                'KENDALA JARINGAN': len(sto_df[(sto_df['STATUS']=='WORKFAIL') & (sto_df['ERRORCODE']=='KENDALA TEKNIK')])
            }
            
            for col_name, val in val_map.items():
                if col_name in KPRO_COLUMN_INDEX_MAP:
                    updates.append(gspread.Cell(row, KPRO_COLUMN_INDEX_MAP[col_name], int(val)))
        
        if updates: ws.update_cells(updates, value_input_option='USER_ENTERED')
        msg.append("✅ Google Sheet (Checkpoint) Updated.")
    except Exception as e:
        msg.append(f"❌ Error Checkpoint Sheet: {e}")

    # 2. Update Micro Sheet & Get Details
    wonum_details = {}
    try:
        ws_micro = sh.worksheet(KPRO_MICRO_UPDATE_SHEET_NAME)
        micro_updates = []
        today_df = raw_df[raw_df['STATUSDATE'].dt.date == today]
        
        for sto, row in KPRO_MICRO_STO_ROW_MAP.items():
            sto_data = today_df[today_df['STO'] == sto]
            wonum_details[sto] = {}
            total_wo = 0
            for status, col_idx in KPRO_MICRO_COLUMN_INDEX_MAP.items():
                if status == 'TOTAL WO': continue
                subset = sto_data[sto_data['STATUS'] == status]
                count = len(subset)
                total_wo += count
                micro_updates.append(gspread.Cell(row, col_idx, count))
                if count > 0: wonum_details[sto][status] = subset['WONUM'].tolist()
            micro_updates.append(gspread.Cell(row, KPRO_MICRO_COLUMN_INDEX_MAP['TOTAL WO'], total_wo))
            
        if micro_updates: ws_micro.update_cells(micro_updates, value_input_option='USER_ENTERED')
        msg.append("✅ Google Sheet (Micro Update) Updated.")
    except Exception as e:
        msg.append(f"❌ Error Micro Sheet: {e}")

    return True, "\n".join(msg), wonum_details

# ==========================================
# 5. HANDLER UTAMA (FILE UPLOAD)
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "Halo! Bot siap menerima laporan.\n"
        "Kirim file Excel (.xls/.xlsx) Anda.\n"
        "-------------------------------\n"
        "1. Bot akan membuat **Gambar Dashboard**.\n"
    )
    if ENABLE_GOOGLE_SHEETS:
        text += "2. Bot akan **Mengupdate Google Sheet** dan memberikan rincian teks.\n"
    else:
        text += "*(Fitur Google Sheet dinonaktifkan)*\n"
    
    await update.message.reply_text(text)

async def handle_excel_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    doc = update.message.document
    if not (doc.file_name.endswith('.xls') or doc.file_name.endswith('.xlsx')):
        await update.message.reply_text("❌ Format file harus .xls atau .xlsx")
        return

    status_msg = await update.message.reply_text("⏳ Menerima file, memproses data...")

    try:
        # 1. Download & Baca File
        f = await context.bot.get_file(doc.file_id)
        f_bytes = io.BytesIO()
        await f.download_to_memory(f_bytes)
        f_bytes.seek(0)
        
        df = pd.read_excel(f_bytes) # Default engine auto-detect
        
        # Bersihkan Data
        cols_to_clean = ['STO', 'STATUS', 'ERRORCODE', 'SUBERRORCODE', 'SCORDERNO']
        for col in cols_to_clean:
            if col in df.columns: df[col] = df[col].astype(str).str.upper().str.strip()

        # ---------------------------------------------------
        # TUGAS 1: BUAT GAMBAR DASHBOARD
        # ---------------------------------------------------
        df['STATUSDATE'] = pd.to_datetime(df['STATUSDATE'], errors='coerce')
        valid_date_df = df.dropna(subset=['STATUSDATE'])
        
        if not valid_date_df.empty:
            latest_date = valid_date_df['STATUSDATE'].dt.date.max()
            daily_df = valid_date_df[valid_date_df['STATUSDATE'].dt.date == latest_date].copy()
            wib_time = update.message.date.astimezone(timezone(timedelta(hours=7)))
            
            # Buat Gambar
            img_buffer = create_integrated_dashboard(daily_df, wib_time, daily_df['STATUS'].value_counts())
            await update.message.reply_photo(
                photo=InputFile(img_buffer, filename="dashboard.png"),
                caption=f"📊 Report Endstate - {latest_date.strftime('%d/%m/%Y')}"
            )
        else:
            await update.message.reply_text("⚠️ Tidak ada data tanggal valid untuk Dashboard Gambar.")

        # ---------------------------------------------------
        # TUGAS 2: PROSES GOOGLE SHEET (JIKA DIAKTIFKAN)
        # ---------------------------------------------------
        if ENABLE_GOOGLE_SHEETS:
            success, sheet_log, wonum_details = await process_kpro_logic(df)
            
            # Kirim Log Update Sheet
            if sheet_log: await update.message.reply_text(sheet_log)
            
            # Kirim Rincian Teks (Estimasi & Micro)
            if success and wonum_details:
                # Estimasi Report
                wib = timezone(timedelta(hours=7))
                today = datetime.now(wib).date()
                today_comp = valid_date_df[(valid_date_df['STATUSDATE'].dt.date == today) & (valid_date_df['STATUS'] == 'COMPWORK')]
                
                est_msg = [f"📋 **Estimasi PS {today.strftime('%d/%m/%Y')}**"]
                tot_tsel = 0; tot_pda = 0
                for sto in KPRO_STO_ROW_MAP:
                    row_sto = today_comp[today_comp['STO'] == sto]
                    pda_cnt = len(row_sto[row_sto['SCORDERNO'].str.contains('PDA', na=False)])
                    tsel_cnt = len(row_sto) - pda_cnt
                    tot_tsel += tsel_cnt; tot_pda += pda_cnt
                    est_msg.append(f"{sto}: TSEL={tsel_cnt}, PDA={pda_cnt}")
                
                est_msg.append(f"\nTOTAL: TSEL={tot_tsel}, PDA={tot_pda}")
                await update.message.reply_text("\n".join(est_msg))
                
                # Micro Detail Report
                micro_msg = ["🔍 **Rincian Status (Micro)**"]
                for sto, stats in wonum_details.items():
                    if stats:
                        micro_msg.append(f"\n__{sto}__")
                        for st, nums in stats.items():
                            micro_msg.append(f"{st} ({len(nums)}): {', '.join(map(str, nums))}")
                
                full_micro = "\n".join(micro_msg)
                # Pecah pesan jika terlalu panjang
                if len(full_micro) > 4000:
                    for i in range(0, len(full_micro), 4000):
                        await update.message.reply_text(full_micro[i:i+4000])
                else:
                    await update.message.reply_text(full_micro)

    except Exception as e:
        logger.error(f"Error utama: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Terjadi kesalahan sistem: {e}")
    finally:
        await status_msg.delete()

# ==========================================
# 6. SETUP APLIKASI
# ==========================================
request_config = HTTPXRequest(read_timeout=60, connect_timeout=60)
ptb_application = Application.builder().token(BOT_TOKEN).request(request_config).build()

@asynccontextmanager
async def lifespan(app: FastAPI):
    ptb_application.add_handler(CommandHandler("start", start))
    ptb_application.add_handler(MessageHandler(filters.Document.ALL, handle_excel_file))
    
    await ptb_application.initialize()
    await ptb_application.start()
    await ptb_application.bot.set_webhook(url=f"{WEBHOOK_URL}{WEBHOOK_PATH}")
    yield
    await ptb_application.stop()
    await ptb_application.shutdown()

app = FastAPI(lifespan=lifespan)

@app.post(WEBHOOK_PATH)
async def webhook(request: Request):
    await ptb_application.process_update(Update.de_json(await request.json(), ptb_application.bot))
    return Response(status_code=200)

@app.get("/")
async def root(): return {"status": "Bot Running Unified Mode"}
