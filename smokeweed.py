import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from telegram import Update, Bot, InputFile
from telegram.ext import Application, MessageHandler, filters, CommandHandler, ContextTypes
import io
import os
from fastapi import FastAPI, Request, Response

# Konfigurasi Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Konfigurasi Bot ---
BOT_TOKEN = "8330450329:AAGEPd2j2a1dZ1PEJ7BrneykiZ-3Kv1T3LI"
WEBHOOK_URL = "https://psbiqbal.onrender.com"
WEBHOOK_PATH = "/telegram" # Path di mana FastAPI akan menerima update

# Inisialisasi FastAPI
app = FastAPI(docs_url=None, redoc_url=None) # Menonaktifkan docs default FastAPI

# Inisialisasi Application (python-telegram-bot)
# Kita akan membuatnya sebagai global atau singleton
ptb_application = (
    Application.builder().token(BOT_TOKEN).arbitrary_callback_data(True).build()
)

# --- Fungsi Utility Bot ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Mengirim pesan sambutan saat perintah /start dipanggil."""
    await update.message.reply_text(
        "Halo! Kirimkan file Excel (.xls atau .xlsx) Anda, dan saya akan membuatkan dashboard."
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Mengirim pesan bantuan saat perintah /help dipanggil."""
    await update.message.reply_text(
        "Saya adalah bot pembuat dashboard Excel. "
        "Cukup kirimkan saya file Excel (.xls atau .xlsx) dengan header utama seperti 'SCORDERNO', 'STATUS', dll., "
        "dan saya akan membuatkan dashboard visual untuk Anda."
        "\n\nPerintah yang tersedia:\n/start - Memulai interaksi\n/help - Menampilkan pesan bantuan ini"
    )

async def handle_excel_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Menangani file Excel yang diunggah pengguna."""
    document = update.message.document
    
    # Memeriksa ekstensi file
    if not (document.file_name.endswith('.xls') or document.file_name.endswith('.xlsx')):
        await update.message.reply_text("Mohon unggah file dengan format .xls atau .xlsx.")
        return

    await update.message.reply_text("Menerima file Anda, sedang memproses...")

    try:
        # Mengunduh file
        file_id = document.file_id
        new_file = await context.bot.get_file(file_id)
        
        # Membaca file ke dalam memory (buffer)
        file_buffer = io.BytesIO()
        await new_file.download_to_memory(file_buffer)
        file_buffer.seek(0) # Kembali ke awal buffer

        # Membaca data menggunakan pandas
        df = pd.read_excel(file_buffer)

        # Memastikan header yang dibutuhkan ada
        required_headers = ['SCORDERNO', 'DATE LAMA', 'STO', 'DATECREATED', 'STATUSDATE', 
                            'STATUS', 'ERRORCODE', 'SUBERRORCODE', 'TGL_MANJA']
        
        missing_headers = [header for header in required_headers if header not in df.columns]
        if missing_headers:
            await update.message.reply_text(
                f"File Excel Anda tidak memiliki header yang lengkap. Header yang hilang: {', '.join(missing_headers)}"
            )
            return

        # Mengkonversi 'STATUSDATE' ke format datetime
        df['STATUSDATE'] = pd.to_datetime(df['STATUSDATE'], errors='coerce')
        # Menghapus baris yang memiliki nilai NaN setelah konversi (jika ada)
        df.dropna(subset=['STATUSDATE'], inplace=True)

        # --- Proses Pembuatan Dashboard ---
        image_buffer = create_dashboard(df)
        
        # Mengirim gambar dashboard
        await update.message.reply_photo(photo=InputFile(image_buffer, filename="dashboard.png"))
        await update.message.reply_text("Dashboard Anda siap!")

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True) # Baris ini sudah lengkap
        await update.message.reply_text(f"Terjadi kesalahan saat memproses file Anda: {e}. Mohon coba lagi atau periksa format file.")

def create_dashboard(df: pd.DataFrame) -> io.BytesIO:
    """
    Membuat dashboard visual berdasarkan data dari DataFrame.
    Mengembalikan buffer gambar.
    """
    
    # Status yang ingin dipantau
    target_statuses = ['ACOMP', 'STARTWORK', 'INTSCOMP', 'WORKFAIL', 
                       'VALCOMP', 'VALSTART', 'CANCELWORK', 'COMPWORK']

    # Filter DataFrame hanya untuk status yang diinginkan
    df_filtered = df[df['STATUS'].isin(target_statuses)].copy()

    # Agregasi data: Hitung jumlah SCORDERNO per STATUS per STATUSDATE (per hari)
    df_filtered['STATUS_DATE_ONLY'] = df_filtered['STATUSDATE'].dt.date
    
    # Menghitung jumlah SCORDERNO unik per STATUS dan per tanggal
    # Ini menghitung jumlah baris untuk setiap kombinasi status dan tanggal
    status_counts = df_filtered.groupby(['STATUS_DATE_ONLY', 'STATUS']).size().reset_index(name='Jumlah SCORDERNO')

    # Membuat pivot table untuk visualisasi yang lebih mudah
    pivot_table = status_counts.pivot_table(index='STATUS_DATE_ONLY', columns='STATUS', values='Jumlah SCORDERNO').fillna(0)
    
    # Mengisi status yang mungkin tidak ada di data untuk konsistensi
    for status in target_statuses:
        if status not in pivot_table.columns:
            pivot_table[status] = 0

    # Mengurutkan kolom status sesuai urutan target_statuses
    pivot_table = pivot_table[target_statuses]

    # --- Visualisasi Dashboard ---
    # Menentukan ukuran figure secara dinamis agar sesuai dengan jumlah status
    fig_height = 4 * len(target_statuses) # Setiap plot status sekitar 4 inci tinggi
    fig, axes = plt.subplots(nrows=len(target_statuses), ncols=1, figsize=(15, fig_height), sharex=True)
    fig.suptitle('Dashboard SCORDERNO Status Tren Harian', fontsize=16)

    # Pastikan 'axes' adalah list/array bahkan jika hanya ada 1 subplot
    if len(target_statuses) == 1:
        axes = [axes]

    for i, status in enumerate(target_statuses):
        # Pastikan ada data untuk diplot, jika tidak, plot kosong atau skip
        if not pivot_table[status].empty:
            sns.lineplot(data=pivot_table, x=pivot_table.index, y=status, ax=axes[i], marker='o')
        else:
            axes[i].text(0.5, 0.5, 'Tidak ada data untuk status ini', horizontalalignment='center', verticalalignment='center', transform=axes[i].transAxes)
        
        axes[i].set_title(f'Jumlah SCORDERNO untuk STATUS: {status}')
        axes[i].set_ylabel('Jumlah SCORDERNO')
        axes[i].grid(True, linestyle='--', alpha=0.7)
        axes[i].tick_params(axis='x', rotation=45)
    
    plt.xlabel('Tanggal')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Menyesuaikan layout agar judul tidak terpotong

    # Menyimpan plot ke buffer gambar
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=150) # Menambah dpi untuk kualitas gambar lebih baik
    image_buffer.seek(0) # Kembali ke awal buffer
    plt.close(fig) # Penting untuk menutup figure agar tidak menghabiskan memori
    
    return image_buffer

# --- Setup Bot Handlers ---
def setup_handlers(application: Application):
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command)) # Menambahkan perintah /help
    application.add_handler(MessageHandler(filters.Document.ALL, handle_excel_file))

# --- FastAPI Endpoints ---

@app.on_event("startup")
async def startup_event():
    logger.info("Starting up...")
    setup_handlers(ptb_application)
    
    # Menghentikan polling yang mungkin aktif dan memulai Application secara async
    # Ini memastikan tidak ada konflik dengan mode webhook
    ptb_application.updater = None 
    await ptb_application.start()
    
    # Menyiapkan webhook di Telegram
    full_webhook_url = f"{WEBHOOK_URL}{WEBHOOK_PATH}"
    await ptb_application.bot.set_webhook(url=full_webhook_url)
    logger.info(f"Webhook set to {full_webhook_url}")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down...")
    await ptb_application.stop()
    await ptb_application.shutdown()
    # Opsional: Hapus webhook saat shutdown agar tidak ada sisa konfigurasi yang mengarah ke server yang mati
    # await ptb_application.bot.delete_webhook() 
    logger.info("Application stopped.")

@app.get("/")
async def root():
    return {"message": "Telegram Bot FastAPI is running!"}

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    """Menerima update dari Telegram dan meneruskannya ke python-telegram-bot."""
    try:
        # Mendapatkan body request sebagai JSON
        json_data = await request.json()
        
        # Meneruskan update ke Application python-telegram-bot
        update = Update.de_json(json_data, ptb_application.bot)
        await ptb_application.process_update(update)

        return Response(status_code=200) # Telegram mengharapkan 200 OK
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}", exc_info=True)
        return Response(status_code=500) # Beri tahu Telegram ada kesalahan
