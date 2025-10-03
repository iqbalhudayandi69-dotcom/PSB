import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from telegram import Update, Bot, InputFile
from telegram.ext import Application, MessageHandler, filters, CommandHandler, ContextTypes
import io
import os
from fastapi import FastAPI, Request, Response
from contextlib import asynccontextmanager # Import ini

# Konfigurasi Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Konfigurasi Bot ---
BOT_TOKEN = "8330450329:AAGEPd2j2a1dZ1PEJ7BrneykiZ-3Kv1T3LI"
WEBHOOK_URL = "https://psbiqbal.onrender.com"
WEBHOOK_PATH = "/telegram" # Path di mana FastAPI akan menerima update

# Inisialisasi Application (python-telegram-bot)
ptb_application = (
    Application.builder().token(BOT_TOKEN).arbitrary_callback_data(True).build()
)

# --- Fungsi Utility Bot (Tidak berubah) ---
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

        required_headers = ['SCORDERNO', 'DATE LAMA', 'STO', 'DATECREATED', 'STATUSDATE', 
                            'STATUS', 'ERRORCODE', 'SUBERRORCODE', 'TGL_MANJA']
        
        missing_headers = [header for header in required_headers if header not in df.columns]
        if missing_headers:
            await update.message.reply_text(
                f"File Excel Anda tidak memiliki header yang lengkap. Header yang hilang: {', '.join(missing_headers)}"
            )
            return

        df['STATUSDATE'] = pd.to_datetime(df['STATUSDATE'], errors='coerce')
        df.dropna(subset=['STATUSDATE'], inplace=True)

        image_buffer = create_dashboard(df)
        
        await update.message.reply_photo(photo=InputFile(image_buffer, filename="dashboard.png"))
        await update.message.reply_text("Dashboard Anda siap!")

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        await update.message.reply_text(f"Terjadi kesalahan saat memproses file Anda: {e}. Mohon coba lagi atau periksa format file.")

def create_dashboard(df: pd.DataFrame) -> io.BytesIO:
    target_statuses = ['ACOMP', 'STARTWORK', 'INTSCOMP', 'WORKFAIL', 
                       'VALCOMP', 'VALSTART', 'CANCELWORK', 'COMPWORK']

    df_filtered = df[df['STATUS'].isin(target_statuses)].copy()
    df_filtered['STATUS_DATE_ONLY'] = df_filtered['STATUSDATE'].dt.date
    status_counts = df_filtered.groupby(['STATUS_DATE_ONLY', 'STATUS']).size().reset_index(name='Jumlah SCORDERNO')

    pivot_table = status_counts.pivot_table(index='STATUS_DATE_ONLY', columns='STATUS', values='Jumlah SCORDERNO').fillna(0)
    
    for status in target_statuses:
        if status not in pivot_table.columns:
            pivot_table[status] = 0

    pivot_table = pivot_table[target_statuses]

    fig_height = 4 * len(target_statuses)
    fig, axes = plt.subplots(nrows=len(target_statuses), ncols=1, figsize=(15, fig_height), sharex=True)
    fig.suptitle('Dashboard SCORDERNO Status Tren Harian', fontsize=16)

    if len(target_statuses) == 1:
        axes = [axes]

    for i, status in enumerate(target_statuses):
        if not pivot_table[status].empty:
            sns.lineplot(data=pivot_table, x=pivot_table.index, y=status, ax=axes[i], marker='o')
        else:
            axes[i].text(0.5, 0.5, 'Tidak ada data untuk status ini', horizontalalignment='center', verticalalignment='center', transform=axes[i].transAxes)
        
        axes[i].set_title(f'Jumlah SCORDERNO untuk STATUS: {status}')
        axes[i].set_ylabel('Jumlah SCORDERNO')
        axes[i].grid(True, linestyle='--', alpha=0.7)
        axes[i].tick_params(axis='x', rotation=45)
    
    plt.xlabel('Tanggal')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])

    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=150)
    image_buffer.seek(0)
    plt.close(fig)
    
    return image_buffer

# --- Setup Bot Handlers (Tidak berubah) ---
def setup_handlers(application: Application):
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_excel_file))

# --- FastAPI Lifespan Events ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Menangani event startup dan shutdown aplikasi FastAPI,
    termasuk inisialisasi dan pembersihan bot Telegram.
    """
    logger.info("Starting up FastAPI application...")
    setup_handlers(ptb_application)
    
    ptb_application.updater = None 
    await ptb_application.initialize()
    await ptb_application.start()
    
    full_webhook_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
    await ptb_application.bot.set_webhook(url=full_webhook_url)
    logger.info(f"Webhook set to {full_webhook_url}")

    yield # Di sini aplikasi Anda akan berjalan

    logger.info("Shutting down FastAPI application...")
    await ptb_application.stop()
    await ptb_application.shutdown()
    # await ptb_application.bot.delete_webhook() # Opsional
    logger.info("Application stopped.")

# Inisialisasi FastAPI dengan lifespan
app = FastAPI(docs_url=None, redoc_url=None, lifespan=lifespan)

# --- FastAPI Endpoints (Tidak berubah) ---
@app.get("/")
async def root():
    return {"message": "Telegram Bot FastAPI is running!"}

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    try:
        json_data = await request.json()
        update = Update.de_json(json_data, ptb_application.bot)
        await ptb_application.process_update(update)
        return Response(status_code=200)
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}", exc_info=True)
        return Response(status_code=500)
