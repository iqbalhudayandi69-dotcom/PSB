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

# --- LIBRARY GOOGLE SHEETS ---
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

# --- PENGATURAN GOOGLE SHEET ---
ENABLE_GOOGLE_SHEETS = True

# --- KREDENSIAL DALAM FORMAT DICTIONARY (LEBIH STABIL) ---
# Saya sudah merapikan format Private Key agar terbaca benar oleh Python
GOOGLE_CREDENTIALS_DICT = {
  "type": "service_account",
  "project_id": "decisive-router-474406-n1",
  "private_key_id": "d0e4eff33dc9104254c76df65e5fcc17541b7849",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC4qN8oPT0p2ovQ\n1KzlX1nvglftOpG7F6gKW72mukaVtYUdx6Vw0FZZk3f0CCGXkdbt2aPtuH5H3JEy\nDlJYDhS1yVl/AyyPrFZHdvvt8J2CWlGEJr4ke+fvWacix4U7/woXRK69TIuOIiKo\nI7Wyj1vhWzxRU6N+L7QRis4Etcc1pbzWwFNuFYaW4vm8YxWrvV8+TOJ76DlZix8Y\nGljabsuq1sVmVggjr2o7+5sRu7BSbPoRbvrTKGmDbEFL1KOcv0OVrkJp1nofFnlb\nIHiG92gYQXubU1qaV5ujTZDs39slcwjyl0JjV6iTAmQX8VsAwFZo46hdJHm/Q93g\nk4Fwldj3AgMBAAECggEAAdOIaCVO57flyRoetE1gnin2WlN0IdsRNOUFDP3ro8sT\nTCDvhcbxtloV23Usdot2ijdW/otkEwGJ8r+fLd1vHqslS53JljQt+EVOJuN0fgJh\nqwJtFVRKY3IfYTctnf3Jk5hWIsDRN9smPO55e2YdPQSCJgsYjgTFGE/8oKGhnOoI\nlUAzrQ3GQvysyxFXCHINGUC2kLxzK3i244ApozUzUZ0z40mBcGfg8DNa4j4SNohD\n0nud+piFI489v5uN4EX9RdmPRr/SEWBChZ/iYLDgXYdU2cDyx+hnXWSiugycWLZi\nPXAEXzhoENrCsp5B1AjhmPG0T7LjAj721lHeEtOLaQKBgQDakx5wic47Q0V7L1fb\n8Fuic39+vCw94DEaPvRJH1LEtf1xmulkse4zFUWQC2AOJFw4ptp+7/zoXPOwkWJi\nX9SFGXm4sNihXhwfewW/Iy6w/RioCAZ3+6CC5eC6z6JxzCQCM2+8cFmxJNii6By6\njjq67Hcregi09pwBpyjPb6OoPwKBgQDYRyPsRRA0UM9HwMv9+u/n2r2pviYAHOku\nbn06LKifWkeFPAez3sUYUCHEnhs07r+26vdODcKdny26+6B4u2xWrJaR5Q6jjdUz\nBHpNu8KBXPlpwh3fDmp/k1yYfkRMdHhRh9MC3DzZgfrMsEa5vb6xV/ffqlp8QZcK\nps5EdRVhSQKBgBkwtWRg7Wy1Dw/oX+bQJ69sQjhX9X1YFjChKsQ2oPJcyw3JvbZG\nL16hx/eW6AYZOKuqxymz/ODGvasOxljyFGsWiYm4j+7hCrqyEfJ6Woo5URskeaJg\nVJphZeoBvgYBcfDy/qCoDh41UeZMe+sgMzKRyBYxpUk91rL2EeT+R80/AoGBAK/7\n2ygy3kejhba+E387xCCmJfRL7EHlRHxqnW1Lz32zCUVJnn7nAvuQoJmLiVnd95PQ\nx6D0o2p8jsp6W45B+5rfXrmiZ/H/w/56Y0aDRHbc/3nl4UaSRWg/sXXIMK0BjLHS\n0omeSck28avCuBoFYniNuv19cZlwCYY6Stb7aoU5AoGBALSPOapJbt4dspw0rLVN\neLF6tQW+Lcx36Bsb7VEmk/Mu28HlKmbtjyJa8dAPbNETXPQp5PpLKoOeTqAwApoM\nGY5RfG/fF+RWiTNEZW4+pLFmCmfOeAMNubRjiMQiFNclssZkeFYPeNsfSAMvYdhH\nsc9LMtN36jTejK+XSCNuERJQ\n-----END PRIVATE KEY-----\n",
  "client_email": "provisioning@decisive-router-474406-n1.iam.gserviceaccount.com",
  "client_id": "101251633100420416304",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/provisioning%40decisive-router-474406-n1.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

KPRO_TARGET_SHEET_URL = "https://docs.google.com/spreadsheets/d/1wPeYLmInP7JlPCLZ1XYR-A75l9oHQfZ_U2R4Pc6ohVY/"
KPRO_TARGET_SHEET_NAME = "REPORT PS INDIHOME"
KPRO_MICRO_UPDATE_SHEET_NAME = "UPDATE PER 2JAM"

# ==========================================
# 2. FUNGSI AUTHENTIKASI GOOGLE (DIPERBAIKI)
# ==========================================
def get_gspread_client():
    if not HAS_GSPREAD: return None
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    try:
        # Menggunakan from_json_keyfile_dict lebih stabil daripada string JSON
        creds = ServiceAccountCredentials.from_json_keyfile_dict(GOOGLE_CREDENTIALS_DICT, scope)
        return gspread.authorize(creds)
    except Exception as e:
        logger.error(f"Google Auth Error: {e}")
        return None

# MAPPING DATA
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
# 3. DASHBOARD RAPI (PERBAIKAN TAMPILAN)
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
    # 1. Persiapan Data
    stos = sorted(daily_df['STO'].unique())
    status_order = ['CANCLWORK', 'COMPWORK', 'ACOMP', 'VALCOMP', 'VALSTART', 'STARTWORK', 'INSTCOMP', 'PENDWORK', 'CONTWORK', 'WORKFAIL']
    
    table_data, row_styles = [], {}
    unique_statuses = daily_df['STATUS'].unique()
    
    for status in status_order:
        if status not in unique_statuses: continue
        status_df = daily_df[daily_df['STATUS'] == status]
        
        row_data = {'KATEGORI': status}
        for sto in stos: row_data[sto] = len(status_df[status_df['STO'] == sto])
        
        table_data.append(row_data)
        row_styles[len(table_data) - 1] = {'level': 1, 'status': status}

        if status == 'WORKFAIL':
            for error_code, error_group in status_df.groupby('ERRORCODE'):
                if str(error_code).upper() in ['NAN', 'N/A']: continue
                error_row = {'KATEGORI': f"   ↳ {error_code}"} # Indentasi visual
                for sto in stos: error_row[sto] = len(error_group[error_group['STO'] == sto])
                table_data.append(error_row); row_styles[len(table_data) - 1] = {'level': 2, 'status': status}
                
                for sub_e, sub_g in error_group.groupby('SUBERRORCODE'):
                    if str(sub_e).upper() in ['NAN', 'N/A']: continue
                    sub_row = {'KATEGORI': f"      → {sub_e}"} # Indentasi lebih dalam
                    for sto in stos: sub_row[sto] = len(sub_g[sub_g['STO'] == sto])
                    if sum(list(sub_row.values())[1:]) > 0:
                        table_data.append(sub_row); row_styles[len(table_data) - 1] = {'level': 3, 'status': status}

    if not table_data:
        fig, ax = plt.subplots(figsize=(10, 2)); ax.axis('off')
        fig.suptitle(f"TIDAK ADA DATA RELEVAN", fontsize=14)
        img = io.BytesIO(); plt.savefig(img, format='png'); img.seek(0); plt.close(fig); return img

    # 2. DataFrame untuk Tabel
    display_df = pd.DataFrame(table_data, columns=['KATEGORI'] + stos).fillna(0)
    display_df['Total'] = display_df[stos].sum(axis=1) # Ganti nama jadi Total (lebih pendek)
    
    # 3. Setup Gambar (Layout Lebih Rapi)
    # Menambah tinggi per baris agar tidak gepeng
    row_height = 0.6 
    header_height = 1.5
    footer_height = 4
    total_height = (len(display_df) * row_height) + header_height + footer_height
    
    fig = plt.figure(figsize=(14, total_height)) # Lebar ditambah jadi 14
    gs = fig.add_gridspec(3, 1, height_ratios=[header_height, len(display_df), footer_height])
    
    ax_title = fig.add_subplot(gs[0]); ax_title.axis('off')
    ax_table = fig.add_subplot(gs[1]); ax_table.axis('off')
    ax_text = fig.add_subplot(gs[2]); ax_text.axis('off')
    
    # Judul
    ax_title.text(0.01, 0.5, f"REPORT DAILY ENDSTATE JAKPUS\n{report_timestamp.strftime('%d %B %Y %H:%M:%S').upper()}", 
                  ha='left', va='center', fontsize=18, weight='bold', color='#2F3E46')
    
    # 4. Membuat Tabel dengan Lebar Kolom Dinamis
    # KATEGORI dapat 40% lebar, sisanya dibagi rata untuk STO + Total
    num_cols = len(display_df.columns)
    col_width_kategori = 0.4
    col_width_others = (1.0 - col_width_kategori) / (num_cols - 1)
    col_widths = [col_width_kategori] + [col_width_others] * (num_cols - 1)

    table = ax_table.table(cellText=display_df.values, colLabels=display_df.columns, 
                           loc='center', cellLoc='center', colWidths=col_widths)
    
    # Styling Tabel
    table.auto_set_font_size(False)
    table.set_fontsize(11) # Font agak diperbesar
    table.scale(1, 2.5) # Scale y diperbesar agar baris lebih tinggi (padding)

    # Warna & Format
    color_map = {
        'COMPWORK': '#E8F5E9', 'ACOMP': '#F1F8E9', 'VALCOMP': '#F1F8E9', 'VALSTART': '#F1F8E9',
        'WORKFAIL': '#FFEBEE', 'KENDALA_ERROR': '#FFF3E0', 'STARTWORK': '#FFFDE7',
        'CANCLWORK': '#FFEBEE', 'Total': '#EEEEEE'
    }
    
    for (row_idx, col_idx), cell in table.get_celld().items():
        cell.set_edgecolor('#BDBDBD')
        cell.set_linewidth(0.5)
        
        # Header
        if row_idx == 0:
            cell.set_facecolor('#37474F')
            cell.set_text_props(color='white', weight='bold')
            cell.set_height(0.08)
            continue
        
        # Data Rows
        style = row_styles.get(row_idx - 1, {})
        status_key = style.get('status')
        level = style.get('level', 1)
        
        # Warna Background
        bg_color = 'white'
        if level == 2 and status_key == 'WORKFAIL': status_key = 'KENDALA_ERROR'
        if status_key in color_map: bg_color = color_map[status_key]
        
        # Zebra Striping untuk sub-level agar beda
        if level > 1: cell.set_facecolor('#FAFAFA')
        else: cell.set_facecolor(bg_color)

        # Formatting Teks
        if col_idx == 0: # Kolom Kategori
            cell.set_text_props(ha='left', weight='bold' if level==1 else 'normal')
            cell.PAD = 0.05
        else: # Kolom Angka
            try:
                val = int(float(cell.get_text().get_text()))
                cell.get_text().set_text(f"{val:,}".replace(',', '.')) # Format Ribuan (indo style)
                if val == 0: cell.get_text().set_color('#CFD8DC') # Angka 0 diabu-abukan
            except: pass
            cell.set_text_props(ha='center', weight='bold')

    # Ringkasan Text di Bawah
    ax_text.text(0.01, 0.9, create_summary_text(status_counts), ha='left', va='top', fontsize=12, family='monospace')
    
    img = io.BytesIO()
    plt.savefig(img, format='png', dpi=150, bbox_inches='tight', pad_inches=0.3)
    img.seek(0); plt.close(fig)
    return img

# ==========================================
# 4. LOGIKA INTEGRASI KPRO
# ==========================================
async def process_kpro_logic(raw_df):
    if not ENABLE_GOOGLE_SHEETS: return False, "", {}
    client = get_gspread_client()
    if not client: return False, "⚠️ Gagal koneksi Google Sheet (Cek Izin Share Email).", {}

    msg = []
    wib_tz = timezone(timedelta(hours=7)); today = datetime.now(wib_tz).date()
    yesterday = today - timedelta(days=1); seven_days = today - timedelta(days=6)
    
    raw_df['DATECREATED'] = pd.to_datetime(raw_df['DATECREATED'], errors='coerce')
    raw_df['STATUSDATE'] = pd.to_datetime(raw_df['STATUSDATE'], errors='coerce')
    raw_df['TGL_MANJA'] = pd.to_datetime(raw_df['TGL_MANJA'], errors='coerce')
    
    try:
        sh = client.open_by_url(KPRO_TARGET_SHEET_URL)
        
        # 1. Update Checkpoint
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

        # 2. Micro Update
        ws_micro = sh.worksheet(KPRO_MICRO_UPDATE_SHEET_NAME)
        micro_updates = []
        wonum_details = {}
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
        msg.append(f"❌ Error Sheet: {str(e)}")

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

    proc_msg = await update.message.reply_text("⏳ Memproses...")
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
            _, log, details = await process_kpro_logic(df)
            if log: await update.message.reply_text(log)
            
            # Estimasi Teks
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
