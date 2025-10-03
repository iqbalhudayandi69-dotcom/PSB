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
        "Halo! Kirimkan file Excel (.xls atau .xlsx) untuk mendapatkan laporan harian terupdate."
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Bot ini akan membuat satu gambar dashboard terintegrasi untuk tanggal paling akhir di file Excel Anda."
    )

async def handle_excel_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    document = update.message.document
    
    if not (document.file_name.endswith('.xls') or document.file_name.endswith('.xlsx')):
        await update.message.reply_text("Mohon unggah file dengan format .xls atau .xlsx.")
        return

    await update.message.reply_text("Menerima file Anda, memproses laporan terintegrasi...")

    try:
        file_id = document.file_id
        new_file = await context.bot.get_file(file_id)
        
        file_buffer = io.BytesIO()
        await new_file.download_to_memory(file_buffer)
        file_buffer.seek(0)

        df = pd.read_excel(file_buffer)

        # Persiapan & Validasi Data
        required_headers = ['SCORDERNO', 'STO', 'STATUSDATE', 'STATUS', 'ERRORCODE', 'SUBERRORCODE']
        missing_headers = [header for header in required_headers if header not in df.columns]
        if missing_headers:
            await update.message.reply_text(f"File Excel lengkap. Header hilang: {', '.join(missing_headers)}")
            return
            
        for col in ['STO', 'STATUS', 'ERRORCODE', 'SUBERRORCODE']:
            df[col] = df[col].astype(str).str.strip().str.upper()

        df['STATUSDATE'] = pd.to_datetime(df['STATUSDATE'], errors='coerce')
        df.dropna(subset=['STATUSDATE'], inplace=True)
        
        if df.empty:
            await update.message.reply_text("Tidak ditemukan data dengan tanggal valid di kolom STATUSDATE.")
            return
            
        # Logika Inti: Fokus pada Tanggal Terupdate
        latest_date = df['STATUSDATE'].dt.date.max()
        daily_df = df[df['STATUSDATE'].dt.date == latest_date].copy()
        
        # Pembuatan & Pengiriman Laporan Tunggal
        image_buffer, summary_text = create_integrated_dashboard(daily_df, latest_date) 
        caption = f"Dashboard & Ringkasan Laporan - {latest_date.strftime('%d %B %Y')}"
        await update.message.reply_photo(photo=InputFile(image_buffer, filename=f"dashboard_{latest_date}.png"), caption=caption)

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        await update.message.reply_text(f"Terjadi kesalahan saat memproses file Anda: {e}. Mohon coba lagi atau periksa format file.")


# --- FUNGSI PEMBUATAN DASHBOARD TERINTEGRASI ---
def create_integrated_dashboard(daily_df: pd.DataFrame, report_date: datetime.date) -> (io.BytesIO, str):
    
    # --- 1. Persiapan Data & Konstruksi Tabel ---
    stos = sorted(daily_df['STO'].unique())
    status_order = ['CANCLWORK', 'COMPWORK', 'ACOMP', 'VALCOMP', 'VALSTART', 'STARTWORK', 'INTSCOMP', 'PENDWORK', 'CONTWORK', 'WORKFAIL']
    
    table_data, row_styles = [], {}
    
    for status in status_order:
        status_df = daily_df[daily_df['STATUS'] == status]
        if status_df.empty: continue

        row_data = {'KATEGORI': status}; [row_data.update({sto: len(status_df[status_df['STO'] == sto])}) for sto in stos]
        table_data.append(row_data); row_styles[len(table_data) - 1] = {'level': 1, 'status': status}

        if status == 'WORKFAIL':
            for error_code, error_group in status_df.groupby('ERRORCODE'):
                if error_code in ['NAN', 'N/A']: continue
                error_row = {'KATEGORI': f"  ↳ {error_code}"}; [error_row.update({sto: len(error_group[error_group['STO'] == sto])}) for sto in stos]
                table_data.append(error_row); row_styles[len(table_data) - 1] = {'level': 2, 'status': status, 'error': error_code}

                for sub_error_code, sub_error_group in error_group.groupby('SUBERRORCODE'):
                    if sub_error_code in ['NAN', 'N/A']: continue
                    sub_error_row = {'KATEGORI': f"    → {sub_error_code}"}; [sub_error_row.update({sto: len(sub_error_group[sub_error_group['STO'] == sto])}) for sto in stos]
                    table_data.append(sub_error_row); row_styles[len(table_data) - 1] = {'level': 3, 'status': status}

    if not table_data: 
        img_buffer, _ = create_empty_dashboard(report_date)
        return img_buffer, "Tidak ada data untuk ditampilkan."

    display_df = pd.DataFrame(table_data).fillna(0)
    display_df['Grand Total'] = display_df[stos].sum(axis=1)
    
    grand_total_row = display_df[stos + ['Grand Total']].sum().to_dict(); grand_total_row['KATEGORI'] = 'Grand Total'
    display_df = pd.concat([display_df, pd.DataFrame([grand_total_row])], ignore_index=True)
    row_styles[len(display_df)-1] = {'level': 0, 'status': 'Total'}

    # --- 2. Perhitungan Teks Ringkasan ---
    summary_text = create_summary_text(daily_df)

    # --- 3. Visualisasi dengan GridSpec ---
    num_rows = len(display_df)
    fig_height = num_rows * 0.4 + 4.5 # Menambah ruang untuk header dan footer
    
    fig = plt.figure(figsize=(12, fig_height))
    gs = fig.add_gridspec(3, 1, height_ratios=[1.5, num_rows, 4])
    
    ax_title = fig.add_subplot(gs[0]); ax_title.axis('off')
    ax_table = fig.add_subplot(gs[1]); ax_table.axis('off')
    ax_text = fig.add_subplot(gs[2]); ax_text.axis('off')
    
    # Render Judul
    ax_title.text(0.01, 0.8, "Laporan Harian (Terupdate)", ha='left', va='top', fontsize=18, weight='bold', color='#2F3E46')
    ax_title.text(0.01, 0.3, report_date.strftime('%d %B %Y').upper(), ha='left', va='top', fontsize=12, color='#588157')
    
    # Render Tabel
    table = ax_table.table(cellText=display_df.values, colLabels=['KATEGORI'] + stos + ['Grand Total'], loc='center', cellLoc='center')
    table.auto_set_font_size(False); table.set_fontsize(10); table.scale(1, 1.8)

    # Styling Tabel
    color_map = {
        'COMPWORK': ('#4CAF50', 'white'), 'ACOMP': ('#C8E6C9', 'black'), 'VALCOMP': ('#C8E6C9', 'black'), 'VALSTART': ('#C8E6C9', 'black'),
        'WORKFAIL': ('#FF9800', 'white'), 'KENDALA_ERROR': ('#FFE0B2', 'black'), 'STARTWORK': ('#FFFDE7', 'black'),
        'CANCLWORK': ('#F44336', 'white'), 'Total': ('#F5F5F5', 'black')
    }
    
    for (row_idx, col_idx), cell in table.get_celld().items():
        cell.set_linewidth(0)
        if row_idx == 0:
            cell.set_facecolor('#2F3E46'); cell.set_text_props(color='white', weight='bold'); cell.visible_edges = 'B'; cell.set_edgecolor('white'); cell.set_linewidth(1.5)
            continue
        cell.visible_edges = 'T'; cell.set_edgecolor('#E0E0E0'); cell.set_linewidth(0.8)
        
        style = row_styles.get(row_idx - 1, {})
        data_row = display_df.iloc[row_idx - 1]
        
        bg_color, text_color = 'white', 'black'
        status_key = style.get('status')
        if style.get('level') == 2 and status_key == 'WORKFAIL': status_key = 'KENDALA_ERROR'
        if status_key in color_map: bg_color, text_color = color_map[status_key]
        
        cell.set_facecolor(bg_color); cell.get_text().set_color(text_color)

        if col_idx > 0:
            numeric_value = pd.to_numeric(data_row.iloc[col_idx], errors='coerce')
            cell.get_text().set_text(f"{numeric_value:,.0f}" if pd.notna(numeric_value) and numeric_value > 0 else "")
            cell.set_text_props(ha='center', va='center')
        else:
            cell.get_text().set_text(data_row.iloc[col_idx])
            cell.set_text_props(ha='left', va='center'); cell.PAD = 0.05
        
        if style.get('level') in [0, 1, 2]: cell.get_text().set_weight('bold')
        if style.get('level') == 3: cell.get_text().set_style('italic')
        
    # Render Teks Ringkasan di Footer
    ax_text.text(0.01, 0.9, summary_text, ha='left', va='top', fontsize=10, family='monospace')

    plt.tight_layout()
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=250); image_buffer.seek(0); plt.close(fig)
    return image_buffer, summary_text

def create_summary_text(daily_df: pd.DataFrame) -> str:
    status_counts = daily_df['STATUS'].value_counts()
    def get_count(s): return status_counts.get(s, 0)

    ps = get_count('COMPWORK')
    acom = get_count('ACOMP') + get_count('VALSTART') + get_count('VALCOMP')
    pi = get_count('STARTWORK')
    pi_progress = get_count('INTSCOMP') + get_count('CONTWORK') + get_count('PENDWORK')
    kendala = get_count('WORKFAIL')
    est_ps = ps + acom

    return (
        f"Ringkasan Metrik Harian:\n"
        f"--------------------------------------\n"
        f"PS (COMPWORK)                 = {ps}\n"
        f"ACOM (ACOMP+VALSTART+VALCOMP) = {acom}\n"
        f"PI (STARTWORK)                  = {pi}\n"
        f"PI PROGRESS (INSTCOMP+...)      = {pi_progress}\n"
        f"KENDALA (WORKFAIL)              = {kendala}\n"
        f"EST PS (PS+ACOM)                = {est_ps}"
    )

def create_empty_dashboard(report_date: datetime.date) -> (io.BytesIO, str):
    # (Fungsi ini tetap sama, hanya return tuple)
    fig, ax = plt.subplots(figsize=(10, 3))
    ax.axis('off')
    fig.suptitle(f"LAPORAN HARIAN (TERUPDATE) - {report_date.strftime('%d %B %Y').upper()}", fontsize=16, weight='bold')
    ax.text(0.5, 0.5, "Tidak ada data untuk status yang relevan pada tanggal ini.", ha='center', va='center', fontsize=12, wrap=True)
    plt.tight_layout(rect=[0, 0, 1, 0.9])
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format='png', dpi=150); image_buffer.seek(0); plt.close(fig)
    return image_buffer, "Tidak ada data untuk ditampilkan."

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
