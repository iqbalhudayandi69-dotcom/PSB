import logging
import pandas as pd
import matplotlib
# Backend Agg wajib untuk server agar tidak error GUI
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

# --- LIBRARY GOOGLE SHEETS ---
try:
    import gspread
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
# Disarankan juga token ditaruh di Env Var, tapi hardcode tidak apa-apa
BOT_TOKEN = os.getenv("BOT_TOKEN", "8330450329:AAGEPd2j2a1dZ1PEJ7BrneykiZ-3Kv1T3LI")
WEBHOOK_URL = "https://psbiqbal.onrender.com"
WEBHOOK_PATH = "/telegram"

# --- PENGATURAN GOOGLE SHEET ---
ENABLE_GOOGLE_SHEETS = True

# --- ID SHEET ---
KPRO_SHEET_ID = "1wPeYLmInP7JlPCLZ1XYR-A75l9oHQfZ_U2R4Pc6ohVY"
KPRO_TARGET_SHEET_NAME = "REPORT PS INDIHOME"
KPRO_MICRO_UPDATE_SHEET_NAME = "UPDATE PER 2JAM"

# ==========================================
# 2. LOGIKA KREDENSIAL DARI ENV
# ==========================================
def get_credentials_dict():
    """Membangun dictionary kredensial dari Environment Variables Render"""
    
    # Ambil Private Key dari Render Environment
    # Pastikan di Render Anda membuat env var bernama: GOOGLE_PRIVATE_KEY
    raw_key = os.getenv("GOOGLE_PRIVATE_KEY")
    
    if not raw_key:
        logger.error("❌ GOOGLE_PRIVATE_KEY tidak ditemukan di Environment Variables!")
        return None

    # FIX PENTING: Render sering mengubah newlines menjadi string literal "\n"
    # Kita harus kembalikan menjadi karakter enter yang asli.
    clean_key = raw_key.replace("\\n", "\n")

    return {
      "type": "service_account",
      "project_id": "decisive-router-474406-n1",
      "private_key_id": "d0e4eff33dc9104254c76df65e5fcc17541b7849",
      "private_key": clean_key, # Menggunakan key dari Env
      "client_email": "provisioning@decisive-router-474406-n1.iam.gserviceaccount.com",
      "client_id": "101251633100420416304",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/provisioning%40decisive-router-474406-n1.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
    }

def get_gspread_client():
    if not HAS_GSPREAD: 
        logger.error("❌ Library 'gspread' TIDAK DITEMUKAN. Cek requirements.txt")
        return None
    try:
        creds_dict = get_credentials_dict()
        if not creds_dict:
            return None
            
        gc = gspread.service_account_from_dict(creds_dict)
        return gc
    except Exception as e:
        logger.error(f"❌ Google Auth Gagal: {e}", exc_info=True)
        return None

# MAPPING DATA SHEET
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
# 3. DASHBOARD (TAMPILAN ASLI)
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
        f"Ringkasan Metrik Harian:\n"
        f"--------------------------------------\n"
        f"PS (COMPWORK)                 = {ps}\n"
        f"ACOM (ACOMP+VALSTART+VALCOMP) = {acom}\n"
        f"PI (STARTWORK)                  = {pi}\n"
        f"PI PROGRESS (INSTCOMP+PENDWORK+CONTWORK) = {pi_progress}\n"
        f"KENDALA (WORKFAIL)              = {kendala}\n"
        f"EST PS (PS+ACOM)                = {est_ps}"
    )

def create_empty_dashboard(report_timestamp: datetime) -> io.BytesIO:
    fig, ax = plt.subplots(figsize=(10, 3)); ax.axis('off')
    fig.suptitle(f"NO DATA - {report_timestamp.strftime('%d %b %Y')}", fontsize=16)
    img = io.BytesIO(); plt.savefig(img, format='png', dpi=150); img.seek(0); plt.close(fig); return img

def create_integrated_dashboard(daily_df: pd.DataFrame, report_timestamp: datetime, status_counts: pd.Series) -> io.BytesIO:
    # --- LOGIKA DASHBOARD ASLI ---
    stos = sorted(daily_df['STO'].unique())
    status_order = ['CANCLWORK', 'COMPWORK', 'ACOMP', 'VALCOMP', 'VALSTART', 'STARTWORK', 'INSTCOMP', 'PENDWORK', 'CONTWORK', 'WORKFAIL']
    
    table_data, row_styles = [], {}
    unique_statuses_in_data = daily_df['STATUS'].unique()
    
    for status in status_order:
        if status not in unique_statuses_in_data: continue
        status_df = daily_df[daily_df['STATUS'] == status]
        row_data = {'KATEGORI': status}
        for sto in stos: row_data[sto] = len(status_df[status_df['STO'] == sto])
        table_data.append(row_data); row_styles[len(table_data) - 1] = {'level': 1, 'status': status}

        if status == 'WORKFAIL':
            for error_code, error_group in status_df.groupby('ERRORCODE'):
                if str(error_code).upper() in ['NAN', 'N/A']: continue
                error_row = {'KATEGORI': f"  ↳ {error_code}"}
                for sto in stos: error_row[sto] = len(error_group[error_group['STO'] == sto])
                table_data.append(error_row); row_styles[len(table_data) - 1] = {'level': 2, 'status': status, 'error': error_code}
                for sub_e, sub_g in error_group.groupby('SUBERRORCODE'):
                    if str(sub_e).upper() in ['NAN', 'N/A']: continue
                    sub_row = {'KATEGORI': f"    → {sub_e}"}
                    for sto in stos: sub_row[sto] = len(sub_g[sub_g['STO'] == sto])
                    if sum(list(sub_row.values())[1:]) > 0:
                        table_data.append(sub_row); row_styles[len(table_data) - 1] = {'level': 3, 'status': status}

    if not table_data: return create_empty_dashboard(report_timestamp)

    display_df = pd.DataFrame(table_data, columns=['KATEGORI'] + stos).fillna(0)
    display_df['Grand Total'] = display_df[stos].sum(axis=1)
    
    relevant_statuses = [row['KATEGORI'] for row in table_data if row_styles.get(table_data.index(row), {}).get('level') == 1]
    total_source_df = daily_df[daily_df['STATUS'].isin(relevant_statuses)]
    grand_total_row = {'KATEGORI': 'Grand Total'}
    for sto in stos: grand_total_row[sto] = len(total_source_df[total_source_df['STO'] == sto])
    grand_total_row['Grand Total'] = len(total_source_df)
    
    display_df = pd.concat([display_df, pd.DataFrame([grand_total_row])], ignore_index=True)
    row_styles[len(display_df)-1] = {'level': 0, 'status': 'Total'}

    num_rows = len(display_df)
    fig_height = num_rows * 0.5 + 4.5
    fig = plt.figure(figsize=(12, fig_height))
    gs = fig.add_gridspec(3, 1, height_ratios=[1.5, num_rows, 5])
    
    ax_title = fig.add_subplot(gs[0]); ax_title.axis('off')
    ax_table = fig.add_subplot(gs[1]); ax_table.axis('off')
    ax_text = fig.add_subplot(gs[2]); ax_text.axis('off')
    
    ax_title.text(0.05, 0.95, f"REPORT DAILY ENDSTATE JAKPUS - {report_timestamp.strftime('%d %B %Y %H:%M:%S').upper()}", 
                  ha='left', va='top', fontsize=16, weight='bold', color='#2F3E46')
    
    col_widths = [0.35] + [0.08] * (len(stos) + 1)
    table = ax_table.table(cellText=display_df.values, colLabels=['KATEGORI'] + stos + ['Grand Total'], 
                           loc='center', cellLoc='center', colWidths=col_widths)
    table.auto_set_font_size(False); table.set_fontsize(10); table.scale(1, 2)

    color_map = {
        'COMPWORK': ('#556B2F', 'white'), 'ACOMP': ('#9ACD32', 'black'), 'VALCOMP': ('#9ACD32', 'black'), 
        'VALSTART': ('#9ACD32', 'black'), 'WORKFAIL': ('#FF8C00', 'white'), 'KENDALA_ERROR': ('#FFDAB9', 'black'), 
        'STARTWORK': ('#FFFACD', 'black'), 'CANCLWORK': ('#DC143C', 'white'), 'Total': ('#F5F5F5', 'black')
    }
    
    for (row_idx, col_idx), cell in table.get_celld().items():
        cell.set_edgecolor('#D3D3D3'); cell.set_linewidth(0.8)
        if row_idx == 0:
            cell.set_facecolor('#404040'); cell.set_text_props(color='white', weight='bold')
            continue
        style = row_styles.get(row_idx - 1, {}); data_row = display_df.iloc[row_idx - 1]
        bg_color, text_color = 'white', 'black'
        status_key = style.get('status')
        if style.get('level') == 2 and status_key == 'WORKFAIL': status_key = 'KENDALA_ERROR'
        if status_key in color_map: bg_color, text_color = color_map[status_key]
        cell.set_facecolor(bg_color); cell.get_text().set_color(text_color)
        if col_idx > 0:
            val = pd.to_numeric(data_row.iloc[col_idx], errors='coerce')
            cell.get_text().set_text(f"{val:,.0f}" if pd.notna(val) and val > 0 else "")
            cell.set_text_props(ha='center', va='center', weight='bold')
        else:
            cell.get_text().set_text(data_row.iloc[col_idx])
            cell.set_text_props(ha='left', va='center', weight='bold'); cell.PAD = 0.05
        if style.get('level') == 3: cell.get_text().set_style('italic')
    
    ax_text.text(0.05, 0.9, create_summary_text(status_counts), ha='left', va='top', fontsize=10, family='monospace')
    img = io.BytesIO(); plt.savefig(img, format='png', dpi=200); img.seek(0); plt.close(fig)
    return img

# ==========================================
# 4. LOGIKA INTEGRASI KPRO (Google Sheet)
# ==========================================
async def process_kpro_logic(raw_df):
    # PERBAIKAN: Variabel didefinisikan di awal agar tidak error "UnboundLocal"
    msg = []
    wonum_details = {}

    if not ENABLE_GOOGLE_SHEETS: return False, "", {}
    
    # 1. Login
    client = get_gspread_client()
    if not client: return False, "⚠️ Gagal Login Google. Cek Env Var 'GOOGLE_PRIVATE_KEY'.", {}

    wib_tz = timezone(timedelta(hours=7)); today = datetime.now(wib_tz).date()
    yesterday = today - timedelta(days=1); seven_days = today - timedelta(days=6)
    
    raw_df['DATECREATED'] = pd.to_datetime(raw_df['DATECREATED'], errors='coerce')
    raw_df['STATUSDATE'] = pd.to_datetime(raw_df['STATUSDATE'], errors='coerce')
    raw_df['TGL_MANJA'] = pd.to_datetime(raw_df['TGL_MANJA'], errors='coerce')
    
    try:
        # 2. Buka Sheet via ID
        sh = client.open_by_key(KPRO_SHEET_ID)
        
        # 3. Update Checkpoint
        ws = sh.worksheet(KPRO_TARGET_SHEET_NAME)
        updates = []
        for sto, row in KPRO_STO_ROW_MAP.items():
            sto_df = raw_df[raw_df['STO'] == sto]
            is_today = sto_df['STATUSDATE'].dt.date == today
            is_created_today = sto_df['DATECREATED'].dt.date == today
            
            val_map = {
                'RE HI': len(sto_df[is_created_today]),
                'PS ENDSTATE': len(sto_df[is_today & (sto_df['STATUS'] == 'COMPWORK')]),
                'VALSTART ENDSATATE': len(sto_df[is_today & sto_df['STATUS'].isin(['INSTCOMP','ACTCOMP','VALCOMP','VALSTART'])]),
                'EST PS H-1': len(sto_df[(sto_df['STATUSDATE'].dt.date == yesterday) & (sto_df['STATUS'] == 'COMPWORK')]),
                'EST PS W-1': len(sto_df[(sto_df['STATUSDATE'].dt.date >= seven_days) & (sto_df['STATUSDATE'].dt.date <= today) & (sto_df['STATUS'] == 'COMPWORK')]),
                'MANJA HI': len(sto_df[(sto_df['STATUS']=='STARTWORK') & (sto_df['TGL_MANJA'].dt.date == today)]),
                'LEWAT MANJA': len(sto_df[(sto_df['STATUS']=='STARTWORK') & (sto_df['TGL_MANJA'].dt.date < today)]),
                'KENDALA PELANGGAN': len(sto_df[(sto_df['STATUS']=='WORKFAIL') & (sto_df['ERRORCODE']=='KENDALA PELANGGAN')]),
                'KENDALA JARINGAN': len(sto_df[(sto_df['STATUS']=='WORKFAIL') & (sto_df['ERRORCODE']=='KENDALA TEKNIK')])
            }
            for col_name, val in val_map.items():
                if col_name in KPRO_COLUMN_INDEX_MAP:
                    updates.append(gspread.Cell(row, KPRO_COLUMN_INDEX_MAP[col_name], int(val)))
        if updates: ws.update_cells(updates, value_input_option='USER_ENTERED')
        msg.append("✅ Checkpoint Updated.")

        # 4. Micro Update
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
                count = len(subset); total_wo += count
                micro_updates.append(gspread.Cell(row, col_idx, count))
                if count > 0: wonum_details[sto][status] = subset['WONUM'].tolist()
            micro_updates.append(gspread.Cell(row, KPRO_MICRO_COLUMN_INDEX_MAP['TOTAL WO'], total_wo))
            
        if micro_updates: ws_micro.update_cells(micro_updates, value_input_option='USER_ENTERED')
        msg.append("✅ Micro Update Updated.")

    except Exception as e:
        msg.append(f"❌ Error Koneksi Sheet: {str(e)}")

    return True, "\n".join(msg), wonum_details

# ==========================================
# 5. HANDLER
# ==========================================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Halo! Kirim file Excel (.xls/.xlsx) untuk update Dashboard & Sheet.")

async def handle_excel_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    doc = update.message.document
    if not (doc.file_name.endswith('.xls') or doc.file_name.endswith('.xlsx')):
        await update.message.reply_text("❌ Format file harus Excel.")
        return

    proc_msg = await update.message.reply_text("⏳ Memproses Dashboard & Sheet...")
    try:
        f = await context.bot.get_file(doc.file_id)
        f_bytes = io.BytesIO(); await f.download_to_memory(f_bytes); f_bytes.seek(0)
        df = pd.read_excel(f_bytes)
        
        cols = ['STO', 'STATUS', 'ERRORCODE', 'SUBERRORCODE', 'SCORDERNO']
        for c in cols: 
            if c in df.columns: df[c] = df[c].astype(str).str.upper().str.strip()

        # 1. Image Dashboard
        df['STATUSDATE'] = pd.to_datetime(df['STATUSDATE'], errors='coerce')
        valid = df.dropna(subset=['STATUSDATE'])
        if not valid.empty:
            latest = valid['STATUSDATE'].dt.date.max()
            daily = valid[valid['STATUSDATE'].dt.date == latest].copy()
            ts = update.message.date.astimezone(timezone(timedelta(hours=7)))
            
            img = create_integrated_dashboard(daily, ts, daily['STATUS'].value_counts())
            await update.message.reply_photo(InputFile(img, filename="dash.png"), caption=f"Report {latest.strftime('%d/%m/%Y')}")
        
        # 2. Google Sheets
        if ENABLE_GOOGLE_SHEETS:
            # Panggil fungsi yang sudah di-fix
            _, log, details = await process_kpro_logic(df)
            if log: await update.message.reply_text(log)
            
            if details:
                today = datetime.now(timezone(timedelta(hours=7))).date()
                today_comp = valid[(valid['STATUSDATE'].dt.date == today) & (valid['STATUS'] == 'COMPWORK')]
                est_txt = [f"📋 *Estimasi PS {today.strftime('%d/%m')}*"]
                tot_t = 0; tot_p = 0
                for sto in KPRO_STO_ROW_MAP:
                    row = today_comp[today_comp['STO'] == sto]
                    pda = len(row[row['SCORDERNO'].str.contains('PDA', na=False)])
                    tsel = len(row) - pda
                    tot_t += tsel; tot_p += pda
                    est_txt.append(f"{sto}: TSEL={tsel}, PDA={pda}")
                est_txt.append(f"TOTAL: TSEL={tot_t}, PDA={tot_p}")
                await update.message.reply_text("\n".join(est_txt))
                
                micro_txt = ["🔍 *Detail Micro*"]
                for sto, stats in details.items():
                    if stats:
                        micro_txt.append(f"\n__{sto}__")
                        for s, n in stats.items(): micro_txt.append(f"{s} ({len(n)}): {', '.join(map(str, n))}")
                
                full_m = "\n".join(micro_txt)
                if len(full_m) > 4000:
                    for i in range(0, len(full_m), 4000): await update.message.reply_text(full_m[i:i+4000])
                else: await update.message.reply_text(full_m)

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Error: {e}")
    finally:
        await proc_msg.delete()

# ==========================================
# 6. APP SETUP
# ==========================================
ptb = Application.builder().token(BOT_TOKEN).request(HTTPXRequest(read_timeout=60, connect_timeout=60)).build()

@asynccontextmanager
async def lifespan(app: FastAPI):
    ptb.add_handler(CommandHandler("start", start))
    ptb.add_handler(MessageHandler(filters.Document.ALL, handle_excel_file))
    await ptb.initialize(); await ptb.start()
    await ptb.bot.set_webhook(f"{WEBHOOK_URL}{WEBHOOK_PATH}")
    yield
    await ptb.stop(); await ptb.shutdown()

app = FastAPI(lifespan=lifespan)
@app.post(WEBHOOK_PATH)
async def webhook(req: Request):
    await ptb.process_update(Update.de_json(await req.json(), ptb.bot))
    return Response(status_code=200)
@app.get("/")
async def root(): return {"status": "ok"}
