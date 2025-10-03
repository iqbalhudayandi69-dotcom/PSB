# ... (semua bagian import, konfigurasi bot, dan fungsi start/help tetap sama) ...
# ...
# ...

async def handle_excel_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    document = update.message.document
    
    if not (document.file_name.endswith('.xls') or document.file_name.endswith('.xlsx')):
        await update.message.reply_text("Mohon unggah file dengan format .xls atau .xlsx.")
        return

    await update.message.reply_text("Menerima file Anda, melakukan validasi dan analisis...")

    try:
        file_id = document.file_id
        new_file = await context.bot.get_file(file_id)
        
        file_buffer = io.BytesIO()
        await new_file.download_to_memory(file_buffer)
        file_buffer.seek(0)

        df = pd.read_excel(file_buffer)
        
        # --- LAPORAN DIAGNOSTIK BAGIAN 1 ---
        total_rows_awal = len(df)

        required_headers = ['SCORDERNO', 'STO', 'STATUSDATE', 'STATUS', 'ERRORCODE', 'SUBERRORCODE']
        
        missing_headers = [header for header in required_headers if header not in df.columns]
        if missing_headers:
            await update.message.reply_text(
                f"File Excel Anda tidak memiliki header yang lengkap. Header yang hilang: {', '.join(missing_headers)}"
            )
            return

        # --- PENINGKATAN: Pembersihan Spasi Otomatis ---
        for col in ['STO', 'STATUS', 'ERRORCODE', 'SUBERRORCODE']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()


        # --- LOGIKA INTI: HANYA AMBIL TANGGAL TERUPDATE ---
        df['STATUSDATE'] = pd.to_datetime(df['STATUSDATE'], errors='coerce')
        
        rows_sebelum_drop_tanggal = len(df)
        df.dropna(subset=['STATUSDATE'], inplace=True)
        rows_setelah_drop_tanggal = len(df)
        
        if df.empty:
            await update.message.reply_text("Tidak ditemukan data dengan tanggal yang valid di kolom STATUSDATE.")
            return
            
        latest_date = df['STATUSDATE'].dt.date.max()
        daily_df = df[df['STATUSDATE'].dt.date == latest_date].copy()
        
        # --- LAPORAN DIAGNOSTIK BAGIAN 2 ---
        diagnostic_message = (
            f"✅ **Laporan Diagnostik Data:**\n"
            f"------------------------------------\n"
            f"Total Baris Awal di File: `{total_rows_awal}`\n"
            f"Baris Dihapus (STATUSDATE kosong): `{rows_sebelum_drop_tanggal - rows_setelah_drop_tanggal}`\n"
            f"------------------------------------\n"
            f"Tanggal Terupdate Ditemukan: `{latest_date.strftime('%d %B %Y')}`\n"
            f"Total Baris untuk Tanggal Ini: `{len(daily_df)}`\n"
            f"------------------------------------"
        )
        await update.message.reply_text(diagnostic_message, parse_mode='Markdown')
        
        # Buat dan kirim satu dashboard
        image_buffer = create_dashboard(daily_df, latest_date) 
        caption = f"Dashboard untuk tanggal terupdate {latest_date.strftime('%d %B %Y')}"
        await update.message.reply_photo(photo=InputFile(image_buffer, filename=f"dashboard_{latest_date}.png"), caption=caption)

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        await update.message.reply_text(f"Terjadi kesalahan saat memproses file Anda: {e}. Mohon coba lagi atau periksa format file.")


# ... (Fungsi create_dashboard dan sisa kode FastAPI tetap sama persis seperti versi sebelumnya) ...
# ...
# ...
