import logging
import pandas as pd
import matplotlib.pyplot as plt
from telegram import Update, InputFile
from telegram.ext import Application, MessageHandler, filters, CommandHandler, ContextTypes
import io
import os
from fastapi import FastAPI, Request, Response
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

# Konfigurasi Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Konfigurasi Bot ---
BOT_TOKEN = "8330450329:AAGEPd2j2a1dZ1PEJ7BrneykiZ-3Kv1T3LI"
WEBHOOK_URL = "https://psbiqbal.onrender.com"
WEBHOOK_PATH = "/telegram" 

# Inisialisasi Application
ptb_application = (
    Application.builder().token(BOT_TOKEN).arbitrary_callback_data(True).build()
)

# --- Fungsi Utility Bot ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Halo! Kirimkan file Excel (.xls atau .xlsx) untuk mendapatkan laporan harian terintegrasi."
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Bot ini akan membuat satu gambar dashboard terintegrasi untuk tanggal paling akhir di file Excel Anda."
    )

# --- FUNGSI HANDLE FILE (DENGAN PERBAIKAN LOGIKA TIMESTAMP FINAL) ---
async def handle_excel_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # --- PERBAIKAN FINAL: Ambil timestamp dari pesan dan konversi ke WIB (UTC+7) ---
    upload_timestamp_utc = update.message.date
    wib_tz = timezone(timedelta(hours=7))
    upload_timestamp_wib = upload_timestamp_utc.astimezone(wib_tz)
    
    document = update.message.document
    
    if not (document.file_name.endswith('.xls') or document.file_name.endswith('.xlsx')):
        await update.message.reply_text("Mohon unggah file dengan format .xls atau .xlsx.")
        return

    await update.message.reply_text("Menerima file Anda, memproses laporan terintegrasi untuk tanggal terupdate...")

    try:
        file_id = document.file_id
        new_file = await context.bot.get_file(file_id)
        
        file_buffer = io.BytesIO()
        await new_file.download_to_memory(file_buffer)
        file_buffer.seek(0)

        df = pd.read_excel(file_buffer)

        required_headers = ['SCORDERNO', 'STO', 'STATUSDATE', 'STATUS', 'ERRORCODE', 'SUBERRORCODE']
        missing_headers = [header for header in required_headers if header not in df.columns]
        if missing_headers:
            await update.message.reply_text(f"File Excel tidak lengkap. Header hilang: {', '.join(missing_headers)}")
            return
            
        for col in ['STO', 'STATUS', 'ERRORCODE', 'SUBERRORCODE']:
            df[col] = df[col].astype(str).str.strip().str.upper()

        df['STATUSDATE'] = pd.to_datetime(df['STATUSDATE'], errors='coerce')
        df.dropna(subset=['STATUSDATE'], inplace=True)
        
        if df.empty:
            await update.message.reply_text("Tidak ditemukan data dengan tanggal valid di kolom STATUSDATE.")
            return
            
        latest_date_in_file = df['STATUSDATE'].dt.date.max()
        daily_df = df[df['STATUSDATE'].dt.date == latest_date_in_file].copy()
        
        status_counts = daily_df['STATUS'].value_counts()
        
        # Kirim timestamp WIB yang sudah benar ke fungsi dashboard
        image_buffer = create_integrated_dashboard(daily_df, upload_timestamp_wib, status_counts) 
        
        caption = f"REPORT DAILY ENDSTATE JAKPUS - {upload_timestamp_wib.strftime('%d %B %Y')}"
        await update.message.reply_photo(photo=InputFile(image_buffer, filename=f"dashboard_{upload_timestamp_wib.strftime('%Y-%m-%d')}.png"), caption=caption)

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        await update.message.reply_text(f"Terjadi kesalahan saat memproses file Anda: {e}. Mohon coba lagi atau periksa format file.")


# --- FUNGSI PEMBUATAN DASHBOARD (Kode tidak berubah, hanya parameter yang diterima) ---
def create_integrated_dashboard(daily_df: pd.DataFrame, report_timestamp: datetime, status_counts: pd.Series) -> io.BytesIO:
    
    # --- 1. Persiapan Data & Konstruksi Tabel ---
    stos = sorted(daily_df['STO'].unique())
    status_order = ['CANCLWORK', 'COMPWORK', 'ACOMP', 'VALCOMP', 'VALSTART', 'STARTWORK', 'INSTCOMP', 'PENDWORK', 'CONTWORK', 'WORKFAIL']
    
    table_data, row_styles = [], {}
    
    unique_statuses_in_data = daily_df['STATUS'].unique()
    
    for status in status_order:
        if status not in unique_statuses_in_data: continue
            
        status_df = daily_df[daily_df['STATUS'] == status]
        
        row_data = {'KATEGORI': status}
        for sto in stos: row_data[sto] = len(status_df[status_df['STO'] == sto])
        
        table_data.append(row_data)
        row_styles[len(table_data) - 1] = {'level': 1, 'status': status}

        if status == 'WORKFAIL':
            for error_code, error_group in status_df.groupby('ERRORCODE'):
                if error_code in ['NAN', 'N/A']: continue
                error_row = {'KATEGORI': f"  ↳ {error_code}"}
                for sto in stos: error_row[sto] = len(error_group[error_group['STO'] == sto])
                table_data.append(error_row)
                row_styles[len(table_data) - 1] = {'level': 2, 'status': status, 'error': error_code}

                for sub_error_code, sub_error_group in error_group.groupby('SUBERRORCODE'):
                    if sub_error_code in ['NAN', 'N/A']: continue
                    sub_error_row = {'KATEGORI': f"    → {sub_error_code}"}
                    for sto in stos: sub_error_row[sto] = len(sub_error_group[sub_error_group['STO'] == sto])
                    if sum(list(sub_error_row.values())[1:]) > 0:
                        table_data.append(sub_error_row)
                        row_styles[len(table_data) - 1] = {'level': 3, 'status': status}

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

    # --- 2. Perhitungan Teks Ringkasan ---
    summary_text = create_summary_text(status_counts)

    # --- 3. Visualisasi ---
    num_rows = len(display_df)
    fig_height = num_rows * 0.5 + 4.5
    
    fig = plt.figure(figsize=(12, fig_height))
    gs = fig.add_gridspec(3, 1, height_ratios=[1.5, num_rows, 5])
    
    ax_title = fig.add_subplot(gs[0]); ax_title.axis('off')
    ax_table = fig.add_subplot(gs[1]); ax_table.axis('off')
    ax_text = fig.add_subplot(gs[2]); ax_text.axis('off')
    
    # Menggunakan timestamp unggah (report_timestamp) yang sudah dikonversi untuk judul
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
            numeric_value = pd.to_numeric(data_row.iloc[col_idx], errors='coerce')
            cell.get_text().set_text(f"{numeric_value:,.0f}" if pd.notna(numeric_value) and numeric_value > 0 else "")
            cell.set_text_props(ha='center', va='center', weight='bold')
        else:
            cell.get_text().set_text(data_row.iloc[col_idx])
            cell.set_text_props(ha='left', va='center', weight='bold'); cell.PAD = 0.05
        
        if style.get('level') == 3: cell.get_text().set_style('italic')
    
    ax_text.text(0.05, 0.9, summary_text, ha='left', va='top', fontsize=10, family='monospace')
    
    plt.tight_layout()
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=200); image_buffer.seek(0); plt.close(fig)
    return image_buffer

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
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.axis('off')
    fig.suptitle(f"REPORT DAILY ENDSTATE JAKPUS - {report_timestamp.strftime('%d %B %Y %H:%M:%S').upper()}", fontsize=16, weight='bold')
    ax.text(0.5, 0.5, "Tidak ada data untuk status yang relevan pada tanggal ini.", ha='center', va='center', fontsize=12, wrap=True)
    plt.tight_layout()
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=150); image_buffer.seek(0); plt.close(fig)
    return image_buffer

# --- Setup Bot Handlers & FastAPI (Tidak Berubah) ---
def setup_handlers(application: Application):
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_excel_file))

KPRO_TARGET_SHEET_URL = "https://docs.google.com/spreadsheets/d/1wPeYLmInP7JlPCLZ1XYR-A75l9oHQfZ_U2R4Pc6ohVY/"
KPRO_TARGET_SHEET_NAME = "REPORT PS INDIHOME"
# 'PACKAGE_NAME' is no longer essential for the file to be considered valid.
KPRO_ESSENTIAL_HEADERS = [
    'WONUM', 'DATECREATED', 'STO', 'STATUS', 'TGL_MANJA',
    'STATUSDATE', 'ERRORCODE'
]

# Static mapping of STO name to its target row number
KPRO_STO_ROW_MAP = {
    'CID': 9,
    'CPP': 10,
    'GBC': 11,
    'GBI': 12,
    'KMY': 13,
}

# Static mapping of KPI names to their target column INDEX (1-based)
KPRO_COLUMN_INDEX_MAP = {
    'RE HI': 9,
    'LEWAT MANJA': 10,
    'MANJA HI': 11,
    'MANJA H+': 12,
    'MANJA H++': 13,
    'KENDALA PELANGGAN': 15,
    'KENDALA JARINGAN': 16,
    'POTENSI PI ORBIT': 18,
    'VALSTART ENDSATATE': 23,
    'PS ENDSTATE': 24,
    'EST PS H-1': 28,
    'EST PS W-1': 29,
    'PS ORBIT HI': 30,
}

# New constants for the "UPDATE PER 2JAM" micro-report
KPRO_MICRO_UPDATE_SHEET_NAME = "UPDATE PER 2JAM"

# Static mapping of STO name to its target row number for the micro-update sheet
KPRO_MICRO_STO_ROW_MAP = {
    'CID': 10,
    'CPP': 11,
    'GBC': 12,
    'GBI': 13,
    'KMY': 14,
}

# Static mapping of micro-update KPI names to their target column INDEX (1-based)
KPRO_MICRO_COLUMN_INDEX_MAP = {
    'STARTWORK': 4,   # D
    'PENDWORK': 5,    # E
    'CONTWORK': 6,    # F
    'INSTCOMP': 7,    # G
    'ACTCOMP': 8,     # H
    'VALSTART': 9,    # I
    'VALCOMP': 10,    # J
    'WORKFAIL': 11,   # K
    'CANCLWORK': 12,  # L
    'COMPWORK': 13,   # M
    'TOTAL WO': 14    # N
}

# New constants for the Kendala table in the micro-update sheet
KPRO_KENDALA_STO_ROW_MAP = {
    'CID': 19,
    'CPP': 20,
    'GBC': 21,
    'GBI': 22,
    'KMY': 23,
}
KPRO_KENDALA_COLUMN_INDEX_MAP = {
    'KENDALA PELANGGAN': 4, # Column D
    'KENDALA TEKNIK': 5,    # Column E
}

# --- Conversation states for /kpro ---
AWAITING_KPRO_FILE = range(1000, 1001) # Use a new, non-overlapping range

import xlrd # Pastikan ini ada di bagian atas file Anda

def _find_and_load_kpro_data(file_bytes: BytesIO, filename: str) -> pd.DataFrame | None:
    """
    Memindai file Excel untuk data yang valid. Jika pembacaan Excel biner gagal 
    karena file tersebut sebenarnya adalah HTML (umum pada ekspor web), 
    fungsi ini akan beralih ke pembacaan HTML sebagai fallback.
    """
    global logger
    all_dfs = []
    
    try:
        # --- Upaya 1 & 2: Coba baca sebagai file Excel biner (.xlsx atau .xls) ---
        engine = 'xlrd' if filename.lower().endswith('.xls') else 'openpyxl'
        xls = pd.ExcelFile(file_bytes, engine=engine)
        
        for sheet_name in xls.sheet_names:
            try:
                # Baca beberapa baris pertama untuk menemukan header dengan efisien
                header_scan_df = pd.read_excel(xls, sheet_name=sheet_name, header=None, nrows=10)
                header_row_index = -1
                for i, row in header_scan_df.iterrows():
                    row_values = set(str(val).strip() for val in row.dropna())
                    if all(header in row_values for header in KPRO_ESSENTIAL_HEADERS):
                        header_row_index = i
                        break
                
                if header_row_index != -1:
                    # Header valid ditemukan, baca seluruh sheet dari baris tersebut
                    sheet_df = pd.read_excel(xls, sheet_name=sheet_name, header=header_row_index)
                    sheet_df.columns = [str(col).strip() for col in sheet_df.columns]
                    all_dfs.append(sheet_df)
                    logger.info(f"Berhasil memuat data Excel dari sheet: '{sheet_name}'")
            except Exception as e_sheet:
                logger.warning(f"Tidak dapat memproses sheet '{sheet_name}'. Melanjutkan. Error: {e_sheet}")
                continue

    except (ValueError, xlrd.biffh.XLRDError) as e:
        # --- Upaya 3: Fallback ke pembacaan HTML jika Upaya 1 & 2 gagal ---
        error_message = str(e)
        if "Expected BOF record" in error_message or "Excel file format cannot be determined" in error_message:
            logger.warning(f"Gagal membaca sebagai file Excel biner. Ini kemungkinan file HTML. Mencoba fallback pembacaan HTML...")
            try:
                # Penting: Kembalikan pointer file ke awal sebelum membaca lagi
                file_bytes.seek(0)
                
                # pd.read_html mengembalikan daftar DataFrame (satu untuk setiap <table> di HTML)
                list_of_html_dfs = pd.read_html(file_bytes, header=0, encoding='utf-8')
                
                if not list_of_html_dfs:
                    logger.error("Fallback HTML: Tidak ada tabel yang ditemukan di dalam file.")
                    return None

                # Cari tabel yang benar dengan memeriksa header esensial
                for df_from_html in list_of_html_dfs:
                    # Bersihkan dan normalkan header dari HTML
                    df_from_html.columns = [str(col).strip() for col in df_from_html.columns]
                    if all(header in df_from_html.columns for header in KPRO_ESSENTIAL_HEADERS):
                        all_dfs.append(df_from_html)
                        logger.info("Berhasil memuat data dari tabel HTML di dalam file.")

            except Exception as e_html:
                logger.error(f"Fallback pembacaan HTML juga gagal: {e_html}", exc_info=True)
                return None
        else:
            # Jika ini adalah error Excel biner lainnya (misalnya, file benar-benar rusak)
            logger.error(f"Gagal membaca file Excel dengan error non-HTML: {e}", exc_info=True)
            return None
    
    except Exception as e_general:
        logger.error(f"Error tidak terduga saat memuat file: {e_general}", exc_info=True)
        return None

    if not all_dfs:
        logger.error("Tidak ada data yang valid ditemukan di dalam file setelah semua upaya.")
        return None

    # Gabungkan semua DataFrame yang valid menjadi satu
    return pd.concat(all_dfs, ignore_index=True)

def _process_kpro_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes the raw DataFrame to calculate all checkpoint KPIs.
    - CORRECTED: Manja rule now counts ONLY tickets with STATUS = 'STARTWORK'.
    - CORRECTED: VALSTART ENDSATATE now correctly counts INSTCOMP, ACTCOMP, VALCOMP, VALSTART
      based on STATUSDATE being today, ignoring case.
    """
    # 1. Data Cleaning and Timezone Preparation
    df['DATECREATED'] = pd.to_datetime(df['DATECREATED'], errors='coerce')
    df['STATUSDATE'] = pd.to_datetime(df['STATUSDATE'], errors='coerce')
    df['TGL_MANJA'] = pd.to_datetime(df['TGL_MANJA'], errors='coerce')

    # Standardize the STATUS and ERRORCODE columns to uppercase to prevent case-sensitivity issues
    df['STATUS'] = df['STATUS'].astype(str).str.strip().str.upper()
    df['ERRORCODE'] = df['ERRORCODE'].astype(str).str.strip().str.upper()

    wib_tz = timezone(timedelta(hours=7))
    today = datetime.now(wib_tz).date()
    yesterday = today - timedelta(days=1)
    seven_days_ago = today - timedelta(days=6)

    # 2. Pre-calculate boolean masks for efficiency (now using standardized uppercase statuses)
    is_created_today = df['DATECREATED'].dt.date == today
    is_statusdate_today = df['STATUSDATE'].dt.date == today
    is_statusdate_yesterday = df['STATUSDATE'].dt.date == yesterday
    is_statusdate_last_7_days = (df['STATUSDATE'].dt.date >= seven_days_ago) & (df['STATUSDATE'].dt.date <= today)
    
    is_compwork = df['STATUS'] == 'COMPWORK'
    is_startwork = df['STATUS'] == 'STARTWORK'
    is_workfail = df['STATUS'] == 'WORKFAIL'

    # Define the correct list of statuses for VALSTART ENDSATATE
    statuses_for_valstart_endstate = ['INSTCOMP', 'ACTCOMP', 'VALCOMP', 'VALSTART']
    
    # 3. Group by STO and calculate KPIs
    grouped = df.groupby('STO')
    kpi_results = {}
    
    default_sto_data = {k: 0 for k in KPRO_COLUMN_INDEX_MAP.keys()}
    all_sto_names = list(KPRO_STO_ROW_MAP.keys())
    for sto in all_sto_names:
        kpi_results[sto] = default_sto_data.copy()

    for sto, group in grouped:
        if sto not in all_sto_names:
            continue

        # --- Orbit Logic (No changes) ---
        ps_orbit_hi_count = 0
        potensi_pi_orbit_count = 0
        has_package = 'PACKAGE_NAME' in group.columns
        has_product = 'PRODUCT' in group.columns
        has_jenis_product = 'JENIS_PRODUCT' in group.columns
        for index, row in group.iterrows():
            is_orbit_product = False
            if has_package and pd.notna(row['PACKAGE_NAME']) and "EZNET" in str(row['PACKAGE_NAME']).upper(): is_orbit_product = True
            elif has_product and pd.notna(row['PRODUCT']) and "EZNET" in str(row['PRODUCT']).upper(): is_orbit_product = True
            elif has_jenis_product and pd.notna(row['JENIS_PRODUCT']) and str(row['JENIS_PRODUCT']).strip().upper() == "EZ NET": is_orbit_product = True
            if is_orbit_product:
                if row['STATUS'] == 'COMPWORK' and row['STATUSDATE'].date() == today: ps_orbit_hi_count += 1
                elif not pd.isna(row['STATUS']) and row['STATUS'] not in ['WORKFAIL', 'COMPWORK', 'COMPLETED']: potensi_pi_orbit_count += 1

        # --- KPI Calculations (with corrections) ---
        re_hi = len(group[is_created_today.loc[group.index]])
        lewat_manja = len(group[is_startwork.loc[group.index] & (group['TGL_MANJA'].dt.date < today)])
        manja_hi = len(group[is_startwork.loc[group.index] & (group['TGL_MANJA'].dt.date == today)])
        manja_h_plus = len(group[is_startwork.loc[group.index] & (group['TGL_MANJA'].dt.date == (today + timedelta(days=1)))])
        manja_h_plus2 = len(group[is_startwork.loc[group.index] & (group['TGL_MANJA'].dt.date >= (today + timedelta(days=2)))])
        kendala_pelanggan = len(group[is_workfail.loc[group.index] & (group['ERRORCODE'] == 'KENDALA PELANGGAN')])
        kendala_jaringan = len(group[is_workfail.loc[group.index] & (group['ERRORCODE'] == 'KENDALA TEKNIK')])
        
        # Use the new, correct list for the VALSTART ENDSATATE calculation
        valstart_endsatate = len(group[is_statusdate_today.loc[group.index] & group['STATUS'].isin(statuses_for_valstart_endstate)])
        
        ps_endsatate = len(group[is_statusdate_today.loc[group.index] & is_compwork.loc[group.index]])
        est_ps_h1 = len(group[is_statusdate_yesterday.loc[group.index] & is_compwork.loc[group.index]])
        est_ps_w1 = len(group[is_statusdate_last_7_days.loc[group.index] & is_compwork.loc[group.index]])

        kpi_results[sto].update({
            'RE HI': re_hi,
            'LEWAT MANJA': lewat_manja,
            'MANJA HI': manja_hi,
            'MANJA H+': manja_h_plus,
            'MANJA H++': manja_h_plus2,
            'KENDALA PELANGGAN': kendala_pelanggan,
            'KENDALA JARINGAN': kendala_jaringan,
            'POTENSI PI ORBIT': potensi_pi_orbit_count,
            'VALSTART ENDSATATE': valstart_endsatate,
            'PS ENDSTATE': ps_endsatate,
            'PS ORBIT HI': ps_orbit_hi_count,
            'EST PS H-1': est_ps_h1,
            'EST PS W-1': est_ps_w1
        })
        
    return pd.DataFrame.from_dict(kpi_results, orient='index')

async def _update_checkpoint_sheet(kpi_data: dict) -> str:
    """
    Updates the target Google Sheet using a static, hardcoded cell map.
    """
    global logger, client
    try:
        sheet = client.open_by_url(KPRO_TARGET_SHEET_URL).worksheet(KPRO_TARGET_SHEET_NAME)
        cells_to_update = []
        for sto_name, row_num in KPRO_STO_ROW_MAP.items():
            if sto_name in kpi_data:
                for kpi_name, col_index in KPRO_COLUMN_INDEX_MAP.items():
                    value = kpi_data[sto_name].get(kpi_name, 0)
                    cells_to_update.append(gspread.Cell(row_num, col_index, value))

        if cells_to_update:
            sheet.update_cells(cells_to_update, value_input_option='USER_ENTERED')
            return f"✅ Berhasil! {len(cells_to_update)} sel telah diperbarui di '{KPRO_TARGET_SHEET_NAME}'.\n{KPRO_TARGET_SHEET_URL}"
        else:
            return  Tidak ada data baru untuk diperbarui (mungkin STO di file tidak cocok dengan target)."

    except Exception as e:
        logger.error(f"Failed to update checkpoint sheet: {e}", exc_info=True)
        return f"❌ Gagal memperbarui Google Sheet. Error: {e}"

# --- /kpro Command Handlers ---
# python
def _generate_ps_estimation_and_anomaly_report(df: pd.DataFrame) -> str:
    """
    Analyzes the raw KPRO dataframe to generate an "Estimasi PS" report
    and detects data anomalies (missing Manja or Manja date before created date).
    """
    global logger, KPRO_STO_ROW_MAP, KPRO_TARGET_SHEET_URL
    
    # --- 1. Data Preparation and Anomaly Detection ---
    wib_tz = timezone(timedelta(hours=7))
    today = datetime.now(wib_tz).date()

    # Ensure necessary columns are parsed as datetimes, coercing errors to NaT (Not a Time)
    df['TGL_MANJA'] = pd.to_datetime(df['TGL_MANJA'], errors='coerce')
    df['DATECREATED'] = pd.to_datetime(df['DATECREATED'], errors='coerce')
    df['STATUSDATE'] = pd.to_datetime(df['STATUSDATE'], errors='coerce')
    
    # Standardize columns for case-insensitive checks
    df['STATUS'] = df['STATUS'].astype(str).str.strip().str.upper()
    if 'SCORDERNO' not in df.columns:
        df['SCORDERNO'] = '' # Add the column if it's missing to prevent errors
    df['SCORDERNO'] = df['SCORDERNO'].astype(str).str.strip().str.upper()


    # Find anomalies
    anomaly_mask = (df['TGL_MANJA'].isna()) | (df['TGL_MANJA'] < df['DATECREATED'])
    anomaly_df = df[anomaly_mask]

    # --- 2. Calculate "Estimasi PS" ---
    df_completed_today = df[(df['STATUSDATE'].dt.date == today) & (df['STATUS'] == 'COMPWORK')].copy()
    
    # Initialize results dictionary to ensure all STOs are reported
    results = {sto: {'PS TSEL': 0, 'PS INDIBIZ': 0, 'PS PDA': 0} for sto in KPRO_STO_ROW_MAP.keys()}
    
    if not df_completed_today.empty:
        # Calculate PDA counts
        pda_mask = df_completed_today['SCORDERNO'].str.contains("PDA", na=False)
        pda_counts = df_completed_today[pda_mask].groupby('STO').size()
        for sto, count in pda_counts.items():
            if sto in results:
                results[sto]['PS PDA'] = count

        # Calculate TSEL counts (everything else that is not PDA)
        tsel_counts = df_completed_today[~pda_mask].groupby('STO').size()
        for sto, count in tsel_counts.items():
            if sto in results:
                results[sto]['PS TSEL'] = count

    # INDIBIZ is always 0 as requested
    for sto in results:
        results[sto]['PS INDIBIZ'] = 0

    # --- 3. Format the Final Message String ---
    message_lines = [f"Estimasi PS Jakpus , {datetime.now(wib_tz).strftime('%d/%m/%Y')} :\n"]
    
    total_tsel, total_indibiz, total_pda = 0, 0, 0

    # Format STO sections
    for sto, data in results.items():
        message_lines.append(f"{sto}")
        message_lines.append(f"PS TSEL = {data['PS TSEL']}")
        message_lines.append(f"PS INDIBIZ = {data['PS INDIBIZ']}")
        message_lines.append(f"PS PDA = {data['PS PDA']}")
        message_lines.append("")
        total_tsel += data['PS TSEL']
        total_indibiz += data['PS INDIBIZ']
        total_pda += data['PS PDA']
    
    # Format TOTAL section
    message_lines.append("TOTAL")
    message_lines.append(f"PS TSEL = {total_tsel}")
    message_lines.append(f"PS INDIBIZ = {total_indibiz}")
    message_lines.append(f"PS PDA = {total_pda}")
    message_lines.append("\n------------------------------------")

    # Format Anomaly section if anomalies were found
    if not anomaly_df.empty:
        message_lines.append("\nInformasi Anomali Order:\n")
        for index, row in anomaly_df.iterrows():
            wonum = row.get('WONUM', 'WONUM Tidak Ditemukan')
            reason = ""
            if pd.isna(row['TGL_MANJA']):
                reason = "Tgl Manja tidak ditemukan/invalid"
            elif row['TGL_MANJA'] < row['DATECREATED']:
                reason = f"Tgl Manja ({row['TGL_MANJA'].strftime('%d-%b')}) < Tgl Dibuat ({row['DATECREATED'].strftime('%d-%b')})"
            message_lines.append(f"- {wonum} ({reason})")
    else:
        message_lines.append("\n✅ Tidak ditemukan anomali data (Manja).")

    # Add the link at the end
    message_lines.append(f"\nLink Sheet: {KPRO_TARGET_SHEET_URL}")
    
    return "\n".join(message_lines)

def _generate_micro_update_details_report(wonum_data: dict) -> str:
    """
    Formats a detailed report of WONUMs grouped by STO and Status.
    CORRECTED: Now includes WORKFAIL in the output.
    """
    message_lines = ["*Rincian WONUM per Status (Update per 2 Jam)*"]
    
    # <<< CORRECTION: 'WORKFAIL' has been added to the list >>>
    status_order = [
        'STARTWORK', 'PENDWORK', 'CONTWORK', 'INSTCOMP', 'ACTCOMP', 
        'VALSTART', 'VALCOMP', 'WORKFAIL', 'CANCLWORK', 'COMPWORK'
    ]

    # Iterate through STOs in a fixed order for consistency
    for sto_name in sorted(KPRO_MICRO_STO_ROW_MAP.keys()):
        sto_data = wonum_data.get(sto_name)
        if not sto_data:
            continue

        has_data_for_sto = any(sto_data.get(status) for status in status_order)
        if not has_data_for_sto:
            continue

        message_lines.append(f"\n\n--- *{sto_name}* ---")
        
        for status in status_order:
            wonum_list = sto_data.get(status)
            if wonum_list:
                # Join WONUMs, handling potential non-string types
                wonum_str = ', '.join(map(str, wonum_list))
                message_lines.append(f"*{status} ({len(wonum_list)})*:\n`{wonum_str}`")
                
    if len(message_lines) == 1:
        return "Tidak ada aktivitas WONUM yang tercatat untuk dilaporkan hari ini."
        
    return "\n".join(message_lines)
    
def _generate_micro_update_details_report(wonum_data: dict) -> str:
    """
    Formats a detailed report of WONUMs grouped by STO and Status.
    """
    message_lines = ["*Rincian WONUM per Status (Update per 2 Jam)*"]
    
    # Define a consistent order for statuses to be displayed
    status_order = [
        'STARTWORK', 'PENDWORK', 'CONTWORK', 'INSTCOMP', 'ACTCOMP', 
        'VALSTART', 'VALCOMP', 'WORKFAIL', 'CANCLWORK', 'COMPWORK'
    ]

    # Iterate through STOs in a fixed order for consistency
    for sto_name in sorted(KPRO_MICRO_STO_ROW_MAP.keys()):
        sto_data = wonum_data.get(sto_name)
        if not sto_data:
            continue

        has_data_for_sto = any(sto_data.get(status) for status in status_order)
        if not has_data_for_sto:
            continue

        message_lines.append(f"\n\n--- *{sto_name}* ---")
        
        for status in status_order:
            wonum_list = sto_data.get(status)
            if wonum_list:
                # Join WONUMs, handling potential non-string types
                wonum_str = ', '.join(map(str, wonum_list))
                message_lines.append(f"*{status} ({len(wonum_list)})*:\n`{wonum_str}`")
                
    if len(message_lines) == 1:
        return "Tidak ada aktivitas WONUM yang tercatat untuk dilaporkan hari ini."
        
    return "\n".join(message_lines)


async def kpro_command(update: Update, context: CallbackContext) -> int:
    """Starts the /kpro Excel processing flow."""
    keyboard = [[InlineKeyboardButton("Batal", callback_data="kpro_cancel")]]
    sent_msg = await update.message.reply_text(
        "Silakan unggah file Excel (.xls atau .xlsx) yang berisi data 'Fulfilment Endstate'.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    context.user_data['kpro_prompt_msg_id'] = sent_msg.message_id
    return AWAITING_KPRO_FILE

# python
async def handle_kpro_file(update: Update, context: CallbackContext) -> int:
    """
    Processes the Excel file, calculates checkpoint KPIs, updates the target Google Sheet,
    triggers the micro-update, and now sends THREE separate reports:
    1. Confirmation of sheet updates.
    2. Detailed WONUM breakdown for the micro-update.
    3. PS Estimation & Anomaly report.
    """
    message = update.message
    chat_id = message.chat_id
    status_msg = None

    await delete_message_if_exists(context, chat_id, context.user_data.pop('kpro_prompt_msg_id', None))

    if not message.document or not (message.document.file_name.lower().endswith('.xlsx') or message.document.file_name.lower().endswith('.xls')):
        await context.bot.send_message(chat_id, "File tidak valid. Harap unggah file .xlsx atau .xls. Proses dibatalkan.")
        return ConversationHandler.END

    try:
        status_msg = await context.bot.send_message(chat_id, "⏳ File diterima. Memproses data dan memperbarui Google Sheet...")
        
        file_bytes = await message.document.get_file()
        file_in_memory = BytesIO(await file_bytes.download_as_bytearray())
        
        raw_df = _find_and_load_kpro_data(file_in_memory, message.document.file_name)

        if raw_df is None or raw_df.empty:
            await delete_message_if_exists(context, chat_id, status_msg.message_id)
            await context.bot.send_message(chat_id, "❌ Gagal menemukan data yang valid di dalam file Excel.")
            return ConversationHandler.END

        # --- Main Checkpoint Update ---
        kpi_df = _process_kpro_kpis(raw_df)
        if kpi_df.empty:
            await delete_message_if_exists(context, chat_id, status_msg.message_id)
            await context.bot.send_message(chat_id, "❌ Gagal menghitung KPI Checkpoint. Pastikan data di file lengkap.")
            return ConversationHandler.END

        checkpoint_result_message = await _update_checkpoint_sheet(kpi_df.to_dict(orient='index'))
        
        # --- Micro-Update ---
        # <<< THIS IS THE CORRECTED LINE >>>
        # It now correctly unpacks all THREE returned values.
        micro_update_success, micro_update_message, wonum_details = await _update_micro_update_sheet(raw_df)
        
        # Combine the original results into one final message
        final_status_report = f"{checkpoint_result_message}\n\n{micro_update_message}"
        
        await delete_message_if_exists(context, chat_id, status_msg.message_id)
        # MESSAGE 1: Send the confirmation of sheet updates
        await context.bot.send_message(chat_id, final_status_report)

        # --- <<< NEW DETAILED WONUM REPORT >>> ---
        # MESSAGE 2: Generate and send the detailed WONUM breakdown
        details_report_text = _generate_micro_update_details_report(wonum_details)
        if details_report_text:
            # Send in chunks if the message is too long
            if len(details_report_text) > 4096:
                for i in range(0, len(details_report_text), 4096):
                    await context.bot.send_message(chat_id, details_report_text[i:i+4096], parse_mode="Markdown")
            else:
                await context.bot.send_message(chat_id, details_report_text, parse_mode="Markdown")
        
        # --- Estimasi PS and Anomaly Report ---
        # MESSAGE 3: Generate and send the Estimasi PS and Anomaly report
        estimation_and_anomaly_report_text = _generate_ps_estimation_and_anomaly_report(raw_df)
        if estimation_and_anomaly_report_text:
            await context.bot.send_message(chat_id, estimation_and_anomaly_report_text)

    except Exception as e:
        logger.error(f"Error during /kpro file processing: {e}", exc_info=True)
        if status_msg: await delete_message_if_exists(context, chat_id, status_msg.message_id)
        await context.bot.send_message(chat_id, f"❌ Terjadi error tak terduga: {e}")

    return ConversationHandler.END
    
async def kpro_cancel(update: Update, context: CallbackContext) -> int:
    """Cancels the /kpro process."""
    query = update.callback_query
    await query.answer()
    await delete_message_if_exists(context, query.message.chat_id, context.user_data.pop('kpro_prompt_msg_id', None))
    await context.bot.send_message(chat_id=query.message.chat_id, text="Proses /kpro dibatalkan.")
    return ConversationHandler.END

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up FastAPI application...")
    setup_handlers(ptb_application)
    await ptb_application.initialize(); await ptb_application.start()
    full_webhook_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
    await ptb_application.bot.set_webhook(url=full_webhook_url)
    logger.info(f"Webhook set to {full_webhook_url}")
    yield 
    logger.info("Shutting down FastAPI application..."); await ptb_application.stop(); await ptb_application.shutdown()

app = FastAPI(docs_url=None, redoc_url=None, lifespan=lifespan)

@app.get("/")
async def root(): return {"message": "Telegram Bot FastAPI is running!"}
@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    try:
        json_data = await request.json(); update = Update.de_json(json_data, ptb_application.bot)
        await ptb_application.process_update(update); return Response(status_code=200)
    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True); return Response(status_code=500)import logging
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
BOT_TOKEN = "8330450329:AAGEPd2j2a1dZ1PEJ7BrneykiZ-3Kv1T3LI"
WEBHOOK_URL = "https://psbiqbal.onrender.com"
WEBHOOK_PATH = "/telegram"

# --- PENGATURAN GOOGLE SHEET ---
ENABLE_GOOGLE_SHEETS = True

# --- ID SHEET (Gunakan ID, bukan URL) ---
KPRO_SHEET_ID = "1wPeYLmInP7JlPCLZ1XYR-A75l9oHQfZ_U2R4Pc6ohVY"
KPRO_TARGET_SHEET_NAME = "REPORT PS INDIHOME"
KPRO_MICRO_UPDATE_SHEET_NAME = "UPDATE PER 2JAM"

# --- KREDENSIAL (PYTHON DICTIONARY) ---
# Saya sudah memperbaiki format Private Key agar otomatis terbaca sebagai baris baru
PRIVATE_KEY_CLEAN = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC4qN8oPT0p2ovQ\n1KzlX1nvglftOpG7F6gKW72mukaVtYUdx6Vw0FZZk3f0CCGXkdbt2aPtuH5H3JEy\nDlJYDhS1yVl/AyyPrFZHdvvt8J2CWlGEJr4ke+fvWacix4U7/woXRK69TIuOIiKo\nI7Wyj1vhWzxRU6N+L7QRis4Etcc1pbzWwFNuFYaW4vm8YxWrvV8+TOJ76DlZix8Y\nGljabsuq1sVmVggjr2o7+5sRu7BSbPoRbvrTKGmDbEFL1KOcv0OVrkJp1nofFnlb\nIHiG92gYQXubU1qaV5ujTZDs39slcwjyl0JjV6iTAmQX8VsAwFZo46hdJHm/Q93g\nk4Fwldj3AgMBAAECggEAAdOIaCVO57flyRoetE1gnin2WlN0IdsRNOUFDP3ro8sT\nTCDvhcbxtloV23Usdot2ijdW/otkEwGJ8r+fLd1vHqslS53JljQt+EVOJuN0fgJh\nqwJtFVRKY3IfYTctnf3Jk5hWIsDRN9smPO55e2YdPQSCJgsYjgTFGE/8oKGhnOoI\nlUAzrQ3GQvysyxFXCHINGUC2kLxzK3i244ApozUzUZ0z40mBcGfg8DNa4j4SNohD\n0nud+piFI489v5uN4EX9RdmPRr/SEWBChZ/iYLDgXYdU2cDyx+hnXWSiugycWLZi\nPXAEXzhoENrCsp5B1AjhmPG0T7LjAj721lHeEtOLaQKBgQDakx5wic47Q0V7L1fb\n8Fuic39+vCw94DEaPvRJH1LEtf1xmulkse4zFUWQC2AOJFw4ptp+7/zoXPOwkWJi\nX9SFGXm4sNihXhwfewW/Iy6w/RioCAZ3+6CC5eC6z6JxzCQCM2+8cFmxJNii6By6\njjq67Hcregi09pwBpyjPb6OoPwKBgQDYRyPsRRA0UM9HwMv9+u/n2r2pviYAHOku\nbn06LKifWkeFPAez3sUYUCHEnhs07r+26vdODcKdny26+6B4u2xWrJaR5Q6jjdUz\nBHpNu8KBXPlpwh3fDmp/k1yYfkRMdHhRh9MC3DzZgfrMsEa5vb6xV/ffqlp8QZcK\nps5EdRVhSQKBgBkwtWRg7Wy1Dw/oX+bQJ69sQjhX9X1YFjChKsQ2oPJcyw3JvbZG\nL16hx/eW6AYZOKuqxymz/ODGvasOxljyFGsWiYm4j+7hCrqyEfJ6Woo5URskeaJg\nVJphZeoBvgYBcfDy/qCoDh41UeZMe+sgMzKRyBYxpUk91rL2EeT+R80/AoGBAK/7\n2ygy3kejhba+E387xCCmJfRL7EHlRHxqnW1Lz32zCUVJnn7nAvuQoJmLiVnd95PQ\nx6D0o2p8jsp6W45B+5rfXrmiZ/H/w/56Y0aDRHbc/3nl4UaSRWg/sXXIMK0BjLHS\n0omeSck28avCuBoFYniNuv19cZlwCYY6Stb7aoU5AoGBALSPOapJbt4dspw0rLVN\neLF6tQW+Lcx36Bsb7VEmk/Mu28HlKmbtjyJa8dAPbNETXPQp5PpLKoOeTqAwApoM\nGY5RfG/fF+RWiTNEZW4+pLFmCmfOeAMNubRjiMQiFNclssZkeFYPeNsfSAMvYdhH\nsc9LMtN36jTejK+XSCNuERJQ\n-----END PRIVATE KEY-----\n"

CREDENTIALS_DICT = {
  "type": "service_account",
  "project_id": "decisive-router-474406-n1",
  "private_key_id": "d0e4eff33dc9104254c76df65e5fcc17541b7849",
  "private_key": PRIVATE_KEY_CLEAN, # Menggunakan variabel di atas
  "client_email": "provisioning@decisive-router-474406-n1.iam.gserviceaccount.com",
  "client_id": "101251633100420416304",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/provisioning%40decisive-router-474406-n1.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

# ==========================================
# 2. FUNGSI AUTHENTIKASI GOOGLE (FINAL FIX)
# ==========================================
def get_gspread_client():
    if not HAS_GSPREAD: 
        logger.error("❌ Library 'gspread' TIDAK DITEMUKAN. Cek requirements.txt")
        return None
    
    try:
        # Menggunakan metode service_account_from_dict yang paling stabil
        gc = gspread.service_account_from_dict(CREDENTIALS_DICT)
        return gc
    except Exception as e:
        logger.error(f"❌ Google Auth Gagal: {e}", exc_info=True)
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
    img = io.BytesIO(); plt.savefig(img, format='png'); img.seek(0); plt.close(fig); return img

def create_integrated_dashboard(daily_df: pd.DataFrame, report_timestamp: datetime, status_counts: pd.Series) -> io.BytesIO:
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
    if not ENABLE_GOOGLE_SHEETS: return False, "", {}
    
    # 1. Login
    client = get_gspread_client()
    if not client: 
        return False, " Gagal Login Google. Cek log server untuk detail error (Library/Key).", {}

    msg = []
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
