import logging
import pandas as pd
import matplotlib.pyplot as plt
import io
import os
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, Request, Response
from contextlib import asynccontextmanager
from telegram import Update, InputFile
from telegram.ext import Application, MessageHandler, filters, CommandHandler, ContextTypes

# ==========================================
# 1. KONFIGURASI & LOGGING
# ==========================================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

BOT_TOKEN = "8330450329:AAGEPd2j2a1dZ1PEJ7BrneykiZ-3Kv1T3LI"
WEBHOOK_URL = "https://psbiqbal.onrender.com"
WEBHOOK_PATH = "/telegram"

# ==========================================
# 2. FUNGSI PEMBANTU (DASHBOARD & LOGIKA)
# ==========================================

def create_summary_text(status_counts: pd.Series) -> str:
    """Membuat ringkasan teks untuk bagian bawah dashboard."""
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
        f"PI (STARTWORK)                = {pi}\n"
        f"PI PROGRESS (INSTCOMP+PENDWORK+CONTWORK) = {pi_progress}\n"
        f"KENDALA (WORKFAIL)            = {kendala}\n"
        f"EST PS (PS+ACOM)              = {est_ps}"
    )

def create_empty_dashboard(report_timestamp: datetime) -> io.BytesIO:
    """Membuat gambar pesan error jika tidak ada data."""
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.axis('off')
    fig.suptitle(f"REPORT DAILY ENDSTATE JAKPUS - {report_timestamp.strftime('%d %B %Y %H:%M:%S').upper()}", fontsize=16, weight='bold')
    ax.text(0.5, 0.5, "Tidak ada data untuk status yang relevan pada tanggal ini.", ha='center', va='center', fontsize=12, wrap=True)
    plt.tight_layout()
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=150); image_buffer.seek(0); plt.close(fig)
    return image_buffer

def create_integrated_dashboard(daily_df: pd.DataFrame, report_timestamp: datetime, status_counts: pd.Series) -> io.BytesIO:
    """Logika utama pembuatan visualisasi tabel dan summary menggunakan Matplotlib."""
    
    # Persiapan Data
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
                    for sto in stos: sub_error_row[sto] = len(sub_error_group[sub_error_group['STO'] == sub_error_code]) # Fix filter logic here if needed
                    # Recalculate sub_error specifically
                    for sto in stos:
                        sub_error_row[sto] = len(sub_error_group[sub_error_group['STO'] == sto])
                    
                    if sum(list(sub_error_row.values())[1:]) > 0:
                        table_data.append(sub_error_row)
                        row_styles[len(table_data) - 1] = {'level': 3, 'status': status}

    if not table_data: return create_empty_dashboard(report_timestamp)

    # Konversi ke DataFrame untuk Tabel
    display_df = pd.DataFrame(table_data, columns=['KATEGORI'] + stos).fillna(0)
    display_df['Grand Total'] = display_df[stos].sum(axis=1)
    
    # Hitung Grand Total Akhir
    relevant_statuses = [row['KATEGORI'] for row in table_data if row_styles.get(table_data.index(row), {}).get('level') == 1]
    total_source_df = daily_df[daily_df['STATUS'].isin(relevant_statuses)]
    grand_total_row = {'KATEGORI': 'Grand Total'}
    for sto in stos: grand_total_row[sto] = len(total_source_df[total_source_df['STO'] == sto])
    grand_total_row['Grand Total'] = len(total_source_df)
    
    display_df = pd.concat([display_df, pd.DataFrame([grand_total_row])], ignore_index=True)
    row_styles[len(display_df)-1] = {'level': 0, 'status': 'Total'}

    # Pembuatan Figure
    num_rows = len(display_df)
    fig_height = num_rows * 0.5 + 4.5
    fig = plt.figure(figsize=(12, fig_height))
    gs = fig.add_gridspec(3, 1, height_ratios=[1.5, num_rows, 5])
    
    ax_title = fig.add_subplot(gs[0]); ax_title.axis('off')
    ax_table = fig.add_subplot(gs[1]); ax_table.axis('off')
    ax_text = fig.add_subplot(gs[2]); ax_text.axis('off')
    
    # Judul
    ax_title.text(0.05, 0.95, f"REPORT DAILY ENDSTATE JAKPUS - {report_timestamp.strftime('%d %B %Y %H:%M:%S').upper()}", 
                  ha='left', va='top', fontsize=16, weight='bold', color='#2F3E46')
    
    # Tabel
    col_widths = [0.35] + [0.08] * (len(stos) + 1)
    table = ax_table.table(cellText=display_df.values, colLabels=['KATEGORI'] + stos + ['Grand Total'], 
                           loc='center', cellLoc='center', colWidths=col_widths)
    table.auto_set_font_size(False); table.set_fontsize(10); table.scale(1, 2)

    # Styling Warna Tabel
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
        
        style = row_styles.get(row_idx - 1, {})
        data_row = display_df.iloc[row_idx - 1]
        
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
            cell.set_text_props(ha='left', va='center', weight='bold'); cell.PAD = 0.05
    
    # Tambahkan Ringkasan Teks
    ax_text.text(0.05, 0.9, create_summary_text(status_counts), ha='left', va='top', fontsize=10, family='monospace')
    
    plt.tight_layout()
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=200); image_buffer.seek(0); plt.close(fig)
    return image_buffer

# ==========================================
# 3. TELEGRAM HANDLERS
# ==========================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Halo! Silakan kirim file Excel laporan (.xls/.xlsx) Anda.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Kirim file Excel, saya akan memproses dashboard berdasarkan tanggal terbaru di file tersebut.")

async def handle_excel_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Set zona waktu WIB
    wib_tz = timezone(timedelta(hours=7))
    upload_timestamp_wib = update.message.date.astimezone(wib_tz)
    
    document = update.message.document
    if not (document.file_name.endswith(('.xls', '.xlsx'))):
        await update.message.reply_text("Hanya menerima format .xls atau .xlsx.")
        return

    msg = await update.message.reply_text("Memproses data, mohon tunggu...")

    try:
        # Download file
        file_obj = await context.bot.get_file(document.file_id)
        file_buffer = io.BytesIO()
        await file_obj.download_to_memory(file_buffer)
        file_buffer.seek(0)

        # Baca Excel
        df = pd.read_excel(file_buffer)
        
        # Validasi Header
        required = ['SCORDERNO', 'STO', 'STATUSDATE', 'STATUS', 'ERRORCODE', 'SUBERRORCODE']
        if not all(h in df.columns for h in required):
            await update.message.reply_text("Gagal! Pastikan file memiliki kolom: " + ", ".join(required))
            return

        # Preprocessing
        for col in ['STO', 'STATUS', 'ERRORCODE', 'SUBERRORCODE']:
            df[col] = df[col].astype(str).str.strip().str.upper()
        df['STATUSDATE'] = pd.to_datetime(df['STATUSDATE'], errors='coerce')
        df.dropna(subset=['STATUSDATE'], inplace=True)
        
        if df.empty:
            await update.message.reply_text("Data kosong setelah filter tanggal.")
            return
            
        # Ambil data tanggal terbaru
        latest_date = df['STATUSDATE'].dt.date.max()
        daily_df = df[df['STATUSDATE'].dt.date == latest_date].copy()
        status_counts = daily_df['STATUS'].value_counts()
        
        # Buat Dashboard
        image_buffer = create_integrated_dashboard(daily_df, upload_timestamp_wib, status_counts)
        
        # Kirim Hasil
        caption = f"✅ REPORT DAILY JAKPUS\n📅 Data Tanggal: {latest_date.strftime('%d/%m/%Y')}\n⏰ Diproses: {upload_timestamp_wib.strftime('%H:%M:%S')} WIB"
        await update.message.reply_photo(
            photo=InputFile(image_buffer, filename="report.png"), 
            caption=caption
        )
        await msg.delete()

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        await update.message.reply_text(f"❌ Terjadi kesalahan sistem: {str(e)}")

# ==========================================
# 4. SETUP FASTAPI & WEBHOOK
# ==========================================

ptb_app = Application.builder().token(BOT_TOKEN).arbitrary_callback_data(True).build()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup Handlers
    ptb_app.add_handler(CommandHandler("start", start))
    ptb_app.add_handler(CommandHandler("help", help_command))
    ptb_app.add_handler(MessageHandler(filters.Document.ALL, handle_excel_file))
    
    # Start Bot
    await ptb_app.initialize()
    await ptb_app.start()
    await ptb_app.bot.set_webhook(url=f"{WEBHOOK_URL}{WEBHOOK_PATH}")
    logger.info("Bot & Webhook initialized.")
    yield
    # Stop Bot
    await ptb_app.stop()
    await ptb_app.shutdown()

app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root(): return {"status": "running"}

@app.post(WEBHOOK_PATH)
async def webhook_handler(request: Request):
    data = await request.json()
    update = Update.de_json(data, ptb_app.bot)
    await ptb_app.process_update(update)
    return Response(status_code=200)
