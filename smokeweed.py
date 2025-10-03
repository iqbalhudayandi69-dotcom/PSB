import logging
import pandas as pd
import matplotlib.pyplot as plt
from telegram import Update, InputFile
from telegram.ext import Application, MessageHandler, filters, CommandHandler, ContextTypes
import io
import os
from fastapi import FastAPI, Request, Response
from contextlib import asynccontextmanager
from datetime import datetime

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
        "Halo! Kirimkan file Excel (.xls atau .xlsx) Anda, dan saya akan membuatkan dashboard harian."
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Bot ini akan membuat gambar dashboard untuk setiap tanggal yang ada di file Excel Anda.\n\n"
        "Dashboard akan menampilkan ringkasan STATUS vs STO, dengan rincian ERRORCODE untuk WORKFAIL."
    )

async def handle_excel_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    document = update.message.document
    
    if not (document.file_name.endswith('.xls') or document.file_name.endswith('.xlsx')):
        await update.message.reply_text("Mohon unggah file dengan format .xls atau .xlsx.")
        return

    await update.message.reply_text("Menerima file Anda, sedang memproses semua tanggal...")

    try:
        file_id = document.file_id
        new_file = await context.bot.get_file(file_id)
        
        file_buffer = io.BytesIO()
        await new_file.download_to_memory(file_buffer)
        file_buffer.seek(0)

        df = pd.read_excel(file_buffer)

        required_headers = ['SCORDERNO', 'STO', 'STATUSDATE', 'STATUS', 'ERRORCODE']
        
        missing_headers = [header for header in required_headers if header not in df.columns]
        if missing_headers:
            await update.message.reply_text(
                f"File Excel Anda tidak memiliki header yang lengkap. Header yang hilang: {', '.join(missing_headers)}"
            )
            return

        # --- LOGIKA INTI: FLAGGING STATUSDATE ---
        df['STATUSDATE'] = pd.to_datetime(df['STATUSDATE'], errors='coerce')
        df.dropna(subset=['STATUSDATE'], inplace=True)
        df['DATE_ONLY'] = df['STATUSDATE'].dt.date
        
        unique_dates = sorted(df['DATE_ONLY'].unique())

        if not unique_dates:
            await update.message.reply_text("Tidak ditemukan data dengan tanggal yang valid di kolom STATUSDATE.")
            return

        await update.message.reply_text(f"Ditemukan data untuk {len(unique_dates)} tanggal. Membuat dashboard untuk masing-masing...")

        for report_date in unique_dates:
            daily_df = df[df['DATE_ONLY'] == report_date].copy()
            image_buffer = create_dashboard(daily_df, report_date) 
            
            # Mengirim caption dengan tanggal laporan
            caption = f"Dashboard untuk tanggal {report_date.strftime('%d %B %Y')}"
            await update.message.reply_photo(photo=InputFile(image_buffer, filename=f"dashboard_{report_date}.png"), caption=caption)

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        await update.message.reply_text(f"Terjadi kesalahan saat memproses file Anda: {e}. Mohon coba lagi atau periksa format file.")


# --- FUNGSI create_dashboard BARU DENGAN DESAIN YANG DIPERBAIKI ---
def create_dashboard(daily_df: pd.DataFrame, report_date: datetime.date) -> io.BytesIO:
    """
    Membuat dashboard visual hierarkis yang dipercantik untuk satu hari.
    """
    
    # Persiapan Data
    for col in ['STO', 'STATUS', 'ERRORCODE']:
        if col not in daily_df.columns: daily_df[col] = 'N/A'
        daily_df[col] = daily_df[col].fillna('N/A').astype(str)

    stos = sorted(daily_df['STO'].unique())
    
    # Urutan status yang diinginkan
    status_order = ['ACOMP', 'STARTWORK', 'INTSCOMP', 'WORKFAIL', 'VALCOMP', 'VALSTART', 'CANCELWORK', 'COMPWORK']

    table_data = []
    row_styles = {} # Menyimpan info styling per baris
    
    for status in status_order:
        status_df = daily_df[daily_df['STATUS'] == status]
        if status_df.empty: continue # Lewati status jika tidak ada datanya

        # Baris STATUS utama
        row_data = {'KATEGORI': status}
        for sto in stos:
            row_data[sto] = len(status_df[status_df['STO'] == sto])
        table_data.append(row_data)
        row_styles[len(table_data) - 1] = {'level': 1, 'status': status}

        # Logika Kondisional: Rincian untuk WORKFAIL
        if status == 'WORKFAIL':
            error_counts = status_df.groupby('ERRORCODE').size()
            for error_code, count in error_counts.items():
                error_row = {'KATEGORI': f"  ↳ {error_code}"}
                error_df = status_df[status_df['ERRORCODE'] == error_code]
                for sto in stos:
                    error_row[sto] = len(error_df[error_df['STO'] == sto])
                table_data.append(error_row)
                row_styles[len(table_data) - 1] = {'level': 2, 'status': status}

    if not table_data: # Jika tidak ada data sama sekali untuk status yang dicari
        return create_empty_dashboard(report_date)

    display_df = pd.DataFrame(table_data)
    display_df['Grand Total'] = display_df[stos].sum(axis=1)

    grand_total_row = display_df[stos + ['Grand Total']].sum().to_dict()
    grand_total_row['KATEGORI'] = 'Grand Total'
    display_df = pd.concat([display_df, pd.DataFrame([grand_total_row])], ignore_index=True)
    row_styles[len(display_df)-1] = {'level': 0, 'status': 'Total'}

    # Ganti 0 dengan string kosong untuk kebersihan visual
    for sto in stos:
        display_df[sto] = display_df[sto].apply(lambda x: '' if x == 0 else x)

    # Visualisasi
    fig_height = len(display_df) * 0.45 + 2.5
    fig, ax = plt.subplots(figsize=(12, fig_height))
    ax.axis('off')
    
    fig.suptitle(f"LAPORAN HARIAN - {report_date.strftime('%d %B %Y').upper()}", fontsize=16, weight='bold')

    table = ax.table(cellText=display_df.values, colLabels=['KATEGORI'] + stos + ['Grand Total'],
                      loc='center', cellLoc='center')
    table.auto_set_font_size(False); table.set_fontsize(10); table.scale(1, 1.8)

    # Styling Lanjutan
    color_map = {'COMPWORK': '#E8F5E9', 'WORKFAIL': '#FFF9C4', 'Total': '#F5F5F5'}
    for (row_idx, col_idx), cell in table.get_celld().items():
        cell.set_edgecolor('#E0E0E0')
        cell.set_height(0.05)

        # Header
        if row_idx == 0:
            cell.set_facecolor('#424242'); cell.set_text_props(color='white', weight='bold')
            continue

        style = row_styles.get(row_idx - 1, {})
        
        # Warna Latar
        bg_color = color_map.get(style.get('status'))
        if bg_color: cell.set_facecolor(bg_color)
            
        # Teks
        if col_idx == 0: # Kolom Kategori
            cell.set_text_props(ha='left', va='center')
            cell.PAD = 0.05 # Padding kiri
        else:
            cell.set_text_props(ha='center', va='center')

        # Bold untuk Level 1 (STATUS) dan Level 0 (Total)
        if style.get('level') in [0, 1]:
            cell.set_text_props(weight='bold')

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=200)
    image_buffer.seek(0)
    plt.close(fig)
    return image_buffer

def create_empty_dashboard(report_date: datetime.date) -> io.BytesIO:
    """Membuat gambar placeholder jika tidak ada data."""
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.axis('off')
    fig.suptitle(f"LAPORAN HARIAN - {report_date.strftime('%d %B %Y').upper()}", fontsize=16, weight='bold')
    ax.text(0.5, 0.5, "Tidak ada data untuk status yang relevan pada tanggal ini.", 
            ha='center', va='center', fontsize=12, wrap=True)
    plt.tight_layout(rect=[0, 0, 1, 0.9])
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=150)
    image_buffer.seek(0)
    plt.close(fig)
    return image_buffer

# --- Setup Bot Handlers & FastAPI (Tidak Berubah) ---
def setup_handlers(application: Application):
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_excel_file))

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
        logger.error(f"Error processing webhook update: {e}", exc_info=True); return Response(status_code=500)
