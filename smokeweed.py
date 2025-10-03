import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from matplotlib.patches import Rectangle
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
        "Halo! Kirimkan file Excel (.xls atau .xlsx) Anda untuk mendapatkan dashboard harian terupdate yang canggih."
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Bot ini akan membuat gambar dashboard untuk tanggal paling akhir (terupdate) yang ada di file Excel Anda.\n\n"
        "Dashboard akan menampilkan ringkasan STATUS vs STO, dengan rincian ERRORCODE dan SUBERRORCODE untuk WORKFAIL."
    )

async def handle_excel_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    document = update.message.document
    
    if not (document.file_name.endswith('.xls') or document.file_name.endswith('.xlsx')):
        await update.message.reply_text("Mohon unggah file dengan format .xls atau .xlsx.")
        return

    await update.message.reply_text("Menerima file Anda, mencari tanggal terupdate dan membuat dashboard...")

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
            await update.message.reply_text(
                f"File Excel Anda tidak memiliki header yang lengkap. Header yang hilang: {', '.join(missing_headers)}"
            )
            return

        # --- LOGIKA INTI: HANYA AMBIL TANGGAL TERUPDATE ---
        df['STATUSDATE'] = pd.to_datetime(df['STATUSDATE'], errors='coerce')
        df.dropna(subset=['STATUSDATE'], inplace=True)
        
        if df.empty:
            await update.message.reply_text("Tidak ditemukan data dengan tanggal yang valid di kolom STATUSDATE.")
            return
            
        latest_date = df['STATUSDATE'].dt.date.max()
        daily_df = df[df['STATUSDATE'].dt.date == latest_date].copy()
        
        image_buffer = create_dashboard(daily_df, latest_date) 
        caption = f"Dashboard untuk tanggal terupdate {latest_date.strftime('%d %B %Y')}"
        await update.message.reply_photo(photo=InputFile(image_buffer, filename=f"dashboard_{latest_date}.png"), caption=caption)

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        await update.message.reply_text(f"Terjadi kesalahan saat memproses file Anda: {e}. Mohon coba lagi atau periksa format file.")


# --- FUNGSI create_dashboard BARU DENGAN DESAIN PREMIUM ---
def create_dashboard(daily_df: pd.DataFrame, report_date: datetime.date) -> io.BytesIO:
    """
    Membuat dashboard visual hierarkis yang dipercantik dengan data bars.
    """
    
    # --- 1. Persiapan Data ---
    for col in ['STO', 'STATUS', 'ERRORCODE', 'SUBERRORCODE']:
        if col not in daily_df.columns: daily_df[col] = 'N/A'
        daily_df[col] = daily_df[col].fillna('N/A').astype(str)

    stos = sorted(daily_df['STO'].unique())
    status_order = ['COMPWORK', 'ACOMP', 'STARTWORK', 'INTSCOMP', 'VALCOMP', 'VALSTART', 'CANCELWORK', 'PENDWORK', 'CONTWORK', 'WORKFAIL']

    table_data = []
    row_styles = {} # Menyimpan info styling per baris
    
    # --- 2. Konstruksi DataFrame Hierarkis ---
    for status in status_order:
        status_df = daily_df[daily_df['STATUS'] == status]
        if status_df.empty: continue

        row_data = {'KATEGORI': status}
        for sto in stos: row_data[sto] = len(status_df[status_df['STO'] == sto])
        table_data.append(row_data)
        row_styles[len(table_data) - 1] = {'level': 1, 'status': status}

        if status == 'WORKFAIL':
            for error_code, error_group in status_df.groupby('ERRORCODE'):
                if error_code == 'N/A': continue
                error_row = {'KATEGORI': f"  ↳ {error_code}"}
                for sto in stos: error_row[sto] = len(error_group[error_group['STO'] == sto])
                table_data.append(error_row)
                row_styles[len(table_data) - 1] = {'level': 2, 'status': status}

                for sub_error_code, sub_error_group in error_group.groupby('SUBERRORCODE'):
                    if sub_error_code == 'N/A': continue
                    sub_error_row = {'KATEGORI': f"    → {sub_error_code}"}
                    for sto in stos: sub_error_row[sto] = len(sub_error_group[sub_error_group['STO'] == sto])
                    table_data.append(sub_error_row)
                    row_styles[len(table_data) - 1] = {'level': 3, 'status': status}

    if not table_data: return create_empty_dashboard(report_date)

    display_df = pd.DataFrame(table_data)
    display_df['Grand Total'] = display_df[stos].sum(axis=1)
    
    grand_total_row = display_df[stos + ['Grand Total']].sum().to_dict()
    grand_total_row['KATEGORI'] = 'Grand Total'
    display_df = pd.concat([display_df, pd.DataFrame([grand_total_row])], ignore_index=True)
    row_styles[len(display_df)-1] = {'level': 0, 'status': 'Total'}

    # --- 3. Visualisasi ---
    fig_height = len(display_df) * 0.4 + 2.5
    fig, ax = plt.subplots(figsize=(12, fig_height))
    ax.axis('off')
    
    # Header
    fig.text(0.05, 0.95, "Laporan Harian (Terupdate)", ha='left', va='bottom', fontsize=18, weight='bold', color='#2F3E46')
    fig.text(0.05, 0.92, report_date.strftime('%d %B %Y').upper(), ha='left', va='bottom', fontsize=12, color='#588157')
    
    # Tabel
    table = ax.table(cellText=[['']*len(display_df.columns)]*len(display_df), 
                     colLabels=['KATEGORI'] + stos + ['Grand Total'],
                     loc='center', cellLoc='center')
    table.auto_set_font_size(False); table.set_fontsize(10); table.scale(1, 1.8)

    # --- 4. Styling & Data Bars (Cell-by-cell rendering) ---
    color_map = {'COMPWORK': '#2E7D32', 'ACOMP': '#2E7D32', 'WORKFAIL': '#C6A700', 'Total': '#2F3E46'}
    bg_color_map = {'WORKFAIL_L1': '#FFF9C4', 'WORKFAIL_L2': '#FFFDE7'}

    for (row_idx, col_idx), cell in table.get_celld().items():
        cell.set_edgecolor('none') # Hapus semua garis awal
        
        # Header
        if row_idx == 0:
            cell.set_facecolor('#2F3E46'); cell.set_text_props(color='white', weight='bold')
            continue
        
        # Garis Horizontal
        cell.add_patch(Rectangle((0, 0), 1, 0.01, facecolor='#E0E0E0', transform=cell.get_transform(), zorder=10))

        style = row_styles.get(row_idx - 1, {})
        data_row = display_df.iloc[row_idx-1]
        
        # Background Color
        if style.get('status') == 'WORKFAIL':
            cell.set_facecolor(bg_color_map[f"WORKFAIL_L{style.get('level', 3)}"])
        elif style.get('status') == 'Total':
            cell.set_facecolor('#FAFAFA')
        
        # Data & Data Bars
        if col_idx > 0: # Kolom numerik
            text = str(data_row.iloc[col_idx])
            numeric_value = pd.to_numeric(text, errors='coerce')
            if pd.notna(numeric_value) and numeric_value > 0:
                # Data Bar
                bar_width = numeric_value / data_row['Grand Total'] if data_row['Grand Total'] > 0 else 0
                cell.add_patch(Rectangle((0, 0.1), bar_width, 0.8, facecolor='#BDBDBD', alpha=0.3, transform=cell.get_transform()))
                cell.get_text().set_text(f"{numeric_value:,.0f}")
            else:
                cell.get_text().set_text("")
            cell.set_text_props(ha='right', va='center', family='sans-serif'); cell.PAD = 0.05
        else: # Kolom Kategori
            cell.get_text().set_text(data_row.iloc[col_idx])
            cell.set_text_props(ha='left', va='center', family='sans-serif'); cell.PAD = 0.05

        # Font Styling
        if style.get('level') == 1: cell.get_text().set_weight('bold')
        if style.get('level') == 3: cell.get_text().set_style('italic')
        if style.get('level') == 0: cell.get_text().set_weight('bold')
        
        status_color = color_map.get(style.get('status'))
        if status_color and col_idx == 0: cell.get_text().set_color(status_color)

    # Footer
    fig.text(0.95, 0.05, "Source: KPRO Fulfillment Endstate", ha='right', va='bottom', fontsize=8, color='gray', style='italic')

    plt.tight_layout(rect=[0, 0.05, 1, 0.9])
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=250)
    image_buffer.seek(0)
    plt.close(fig)
    return image_buffer

def create_empty_dashboard(report_date: datetime.date) -> io.BytesIO:
    # (Fungsi ini tetap sama)
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.axis('off')
    fig.suptitle(f"LAPORAN HARIAN (TERUPDATE) - {report_date.strftime('%d %B %Y').upper()}", fontsize=16, weight='bold')
    ax.text(0.5, 0.5, "Tidak ada data untuk status yang relevan pada tanggal ini.", ha='center', va='center', fontsize=12, wrap=True)
    plt.tight_layout(rect=[0, 0, 1, 0.9])
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=150); image_buffer.seek(0); plt.close(fig)
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
