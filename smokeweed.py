import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from telegram import Update, InputFile
from telegram.ext import Application, MessageHandler, filters, CommandHandler, ContextTypes
import io # Untuk menyimpan gambar ke memori

# Konfigurasi Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Fungsi Utility ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Mengirim pesan sambutan saat perintah /start dipanggil."""
    await update.message.reply_text(
        "Halo! Kirimkan file Excel (.xls atau .xlsx) Anda, dan saya akan membuatkan dashboard."
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
        logger.error(f"Error processing file: {e}", exc_info=True)
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
    # Anda menyebutkan "per STATUSDATE", jadi kita akan mengelompokkan per hari.
    df_filtered['STATUS_DATE_ONLY'] = df_filtered['STATUSDATE'].dt.date
    
    # Menghitung jumlah SCORDERNO unik per STATUS dan per tanggal
    # Jika SCORDERNO bisa duplikat dan ingin dihitung sebagai satu entitas, gunakan .nunique()
    # Jika setiap baris adalah entitas unik yang ingin dihitung, gunakan .size()
    # Untuk kasus ini, saya asumsikan setiap baris SCORDERNO adalah unik yang dihitung.
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
    fig, axes = plt.subplots(nrows=len(target_statuses), ncols=1, figsize=(15, 5 * len(target_statuses)), sharex=True)
    fig.suptitle('Dashboard SCORDERNO Status Tren Harian', fontsize=16)

    if len(target_statuses) == 1: # Handle case with single status
        axes = [axes]

    for i, status in enumerate(target_statuses):
        sns.lineplot(data=pivot_table, x=pivot_table.index, y=status, ax=axes[i], marker='o')
        axes[i].set_title(f'Jumlah SCORDERNO untuk STATUS: {status}')
        axes[i].set_ylabel('Jumlah SCORDERNO')
        axes[i].grid(True, linestyle='--', alpha=0.7)
        axes[i].tick_params(axis='x', rotation=45)
    
    plt.xlabel('Tanggal')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Menyesuaikan layout agar judul tidak terpotong

    # Menyimpan plot ke buffer gambar
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png')
    image_buffer.seek(0) # Kembali ke awal buffer
    plt.close(fig) # Penting untuk menutup figure agar tidak menghabiskan memori
    
    return image_buffer

# --- Fungsi Main (untuk menjalankan bot) ---

async def main() -> None:
    """Fungsi utama untuk menjalankan bot."""
    # Ganti 'YOUR_BOT_TOKEN' dengan token bot Telegram Anda
    application = Application.builder().token("YOUR_BOT_TOKEN").build()

    # Handler perintah
    application.add_handler(CommandHandler("start", start))

    # Handler pesan file (filter hanya untuk dokumen)
    application.add_handler(MessageHandler(filters.Document.ALL, handle_excel_file))

    # Jalankan bot sampai Ctrl-C ditekan
    logger.info("Bot dimulai...")
    await application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())