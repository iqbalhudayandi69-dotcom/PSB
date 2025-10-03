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
import numpy as np

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
        "Halo! Kirimkan file Excel (.xls atau .xlsx) Anda, dan saya akan membuatkan dashboard."
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Saya adalah bot pembuat dashboard Excel. "
        "Cukup kirimkan saya file Excel (.xls atau .xlsx) dengan header utama seperti 'SCORDERNO', 'STATUS', dll., "
        "dan saya akan membuatkan dashboard visual untuk Anda."
        "\n\nPerintah yang tersedia:\n/start - Memulai interaksi\n/help - Menampilkan pesan bantuan ini"
    )

async def handle_excel_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    document = update.message.document
    
    if not (document.file_name.endswith('.xls') or document.file_name.endswith('.xlsx')):
        await update.message.reply_text("Mohon unggah file dengan format .xls atau .xlsx.")
        return

    await update.message.reply_text("Menerima file Anda, sedang memproses...")

    try:
        file_id = document.file_id
        new_file = await context.bot.get_file(file_id)
        
        file_buffer = io.BytesIO()
        await new_file.download_to_memory(file_buffer)
        file_buffer.seek(0)

        df = pd.read_excel(file_buffer)

        required_headers = ['SCORDERNO', 'DATEL LAMA', 'STO', 'DATECREATED', 'STATUSDATE', 
                            'STATUS', 'ERRORCODE', 'SUBERRORCODE', 'TGL_MANJA', 'WORKFAIL']
        
        missing_headers = [header for header in required_headers if header not in df.columns]
        if missing_headers:
            await update.message.reply_text(
                f"File Excel Anda tidak memiliki header yang lengkap. Header yang hilang: {', '.join(missing_headers)}"
            )
            return

        image_buffer = create_dashboard(df) 
        
        await update.message.reply_photo(photo=InputFile(image_buffer, filename="dashboard.png"))
        await update.message.reply_text("Dashboard Anda siap!")

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        await update.message.reply_text(f"Terjadi kesalahan saat memproses file Anda: {e}. Mohon coba lagi atau periksa format file.")


# --- FUNGSI create_dashboard BARU UNTUK FORMAT GAMBAR SPESIFIK ---
def create_dashboard(df: pd.DataFrame) -> io.BytesIO:
    """
    Membuat dashboard visual yang meniru format gambar yang diberikan.
    """
    
    # --- 1. Persiapan Data ---
    for col in ['STO', 'STATUS', 'ERRORCODE']:
        if col not in df.columns: df[col] = 'N/A'
        df[col] = df[col].fillna('N/A').astype(str)

    # Dapatkan daftar STO unik dari data, diurutkan
    stos = sorted(df['STO'].unique())
    
    # Definisikan struktur hierarki KET_ORDER
    ket_order_structure = {
        'CANCLWORK': [], 'COMPWORK': [], 'INSTCOMP': [], 'STARTWORK': [],
        'WORKFAIL': {
            'KENDALA PELANGGAN': [
                "ALAMAT TIDAK DITEMUKAN", "BATAL", "INDIKASI CABUT PASANG",
                "KENDALA IZIN", "PENDING", "SALAH TAGGING",
            ],
            'KENDALA TEKNIK': [
                "KENDALA IKR/IKG", "ODP JAUH", "ODP RETI"
            ]
        }
    }
    
    # --- 2. Bangun DataFrame untuk Ditampilkan ---
    table_data = []
    row_styles = {} # Untuk menyimpan info styling
    
    for ket, sub_ket in ket_order_structure.items():
        # Baris level 1 (STATUS)
        row_data = {'KET_ORDER': ket}
        
        if not sub_ket: # Jika tidak punya sub-kategori
            filtered_df = df[df['STATUS'] == ket]
            for sto in stos:
                row_data[sto] = len(filtered_df[filtered_df['STO'] == sto])
            table_data.append(row_data)
            row_styles[len(table_data)-1] = {'level': 1, 'color': ket}
        else: # Jika punya sub-kategori (kasus WORKFAIL)
            # Baris header grup
            workfail_df = df[df['STATUS'] == ket]
            for sto in stos:
                row_data[sto] = len(workfail_df[workfail_df['STO'] == sto])
            table_data.append(row_data)
            row_styles[len(table_data)-1] = {'level': 1, 'color': ket}

            # Sub-grup (KENDALA PELANGGAN, KENDALA TEKNIK)
            for sub_group_name, error_codes in sub_ket.items():
                sub_group_df = workfail_df[workfail_df['ERRORCODE'].isin(error_codes)]
                sub_group_row = {'KET_ORDER': f"  {sub_group_name}"}
                for sto in stos:
                    sub_group_row[sto] = len(sub_group_df[sub_group_df['STO'] == sto])
                table_data.append(sub_group_row)
                row_styles[len(table_data)-1] = {'level': 2, 'color': ket}
                
                # Rincian error code
                for error_code in error_codes:
                    error_df = workfail_df[workfail_df['ERRORCODE'] == error_code]
                    error_row = {'KET_ORDER': f"    {error_code}"}
                    for sto in stos:
                        error_row[sto] = len(error_df[error_df['STO'] == sto])
                    if sum(list(error_row.values())[1:]) > 0: # Hanya tampilkan jika ada data
                        table_data.append(error_row)
                        row_styles[len(table_data)-1] = {'level': 3, 'color': ket}

    display_df = pd.DataFrame(table_data).replace(0, '') # Ganti 0 dengan string kosong
    display_df['Grand Total'] = display_df[stos].replace('', 0).sum(axis=1)

    # Tambah baris Grand Total
    grand_total_row = display_df[stos + ['Grand Total']].replace('', 0).sum().to_dict()
    grand_total_row['KET_ORDER'] = 'Grand Total'
    display_df = pd.concat([display_df, pd.DataFrame([grand_total_row])], ignore_index=True)
    row_styles[len(display_df)-1] = {'level': 0, 'color': 'Grand Total'}

    # --- 3. Hitung Metrik Ringkasan ---
    status_counts = df['STATUS'].value_counts()
    def get_count(s): return status_counts.get(s, 0)
    
    ps = get_count('COMPWORK')
    acom = get_count('ACTCOMP') + get_count('VALSTART') + get_count('VALCOMP')
    pi = get_count('STARTWORK')
    pi_progress = get_count('INSTCOMP') + get_count('CONTWORK') + get_count('PENDWORK')
    kendala = get_count('WORKFAIL')
    est_ps = ps + acom

    summary_text = (
        f"PS (COMPWORK) = {ps}\n"
        f"ACOM (ACTCOMP+VALSTART+VALCOMP) = {acom}\n"
        f"PI (STARTWORK) = {pi}\n"
        f"PI PROGRESS (INSTCOMP+CONTWORK+PENDWORK) = {pi_progress}\n"
        f"KENDALA (WORKFAIL) = {kendala}\n"
        f"EST PS (PS+ACOM) = {est_ps}\n\n"
        f"Source : KPRO Fulfillment Endstate"
    )

    # --- 4. Visualisasi dengan Matplotlib ---
    fig_height = len(display_df) * 0.4 + 4 # Tinggi dinamis + ruang untuk teks
    fig = plt.figure(figsize=(10, fig_height))
    gs = fig.add_gridspec(2, 1, height_ratios=[len(display_df), 7])
    
    ax_table = fig.add_subplot(gs[0])
    ax_text = fig.add_subplot(gs[1])
    ax_table.axis('off'); ax_text.axis('off')

    # Judul Utama
    fig.suptitle(f"REPORT DAILY ENDSTATE {datetime.now().strftime('%d/%m/%Y %H.%M')}", fontsize=14, y=0.98)
    
    # Render Tabel
    table = ax_table.table(
        cellText=display_df.values, colLabels=['KET_ORDER'] + stos + ['Grand Total'],
        loc='center', cellLoc='center'
    )
    table.auto_set_font_size(False); table.set_fontsize(9); table.scale(1, 1.5)

    # Styling Tabel
    color_map = {'COMPWORK': '#dff0d8', 'WORKFAIL': '#fcf8e3', 'Grand Total': '#d9edf7'}
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor('black')
        # Header
        if row == 0:
            cell.set_facecolor('#d9edf7'); cell.set_text_props(weight='bold')
            if col == 0: cell.set_text_props(ha='left', va='center')
            else: cell.set_text_props(ha='center', va='center')
            continue
            
        # Styling baris berdasarkan info yang disimpan
        style_info = row_styles.get(row - 1, {})
        bg_color = color_map.get(style_info.get('color'))
        if bg_color: cell.set_facecolor(bg_color)

        # Alignment & Bold
        if col == 0: cell.set_text_props(ha='left', va='center')
        if style_info.get('level') in [0, 2]: cell.set_text_props(weight='bold')
        
        # Grand Total Column
        if col == len(display_df.columns) - 1: cell.set_text_props(weight='bold')

    # Render Teks Ringkasan
    ax_text.text(0.01, 0.9, summary_text, ha='left', va='top', fontsize=10, linespacing=1.5)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=200)
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
