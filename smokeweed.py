import logging
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from telegram import Update, InputFile
from telegram.ext import Application, MessageHandler, filters, CommandHandler, ContextTypes
from telegram.request import HTTPXRequest
import io
import os
import json # Wajib import json
from fastapi import FastAPI, Request, Response
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta

# --- LIBRARY GOOGLE SHEETS ---
try:
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
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

BOT_TOKEN = "8330450329:AAGEPd2j2a1dZ1PEJ7BrneykiZ-3Kv1T3LI"
WEBHOOK_URL = "https://psbiqbal.onrender.com"
WEBHOOK_PATH = "/telegram"

# --- PENGATURAN GOOGLE SHEET ---
ENABLE_GOOGLE_SHEETS = True  # Set True agar aktif

# ---------------------------------------------------------
# PASTE ISI FILE CREDENTIALS.JSON ANDA DI ANTARA TANDA KUTIP 3 DI BAWAH INI
# ---------------------------------------------------------
GOOGLE_JSON_CREDENTIALS = """
{
  "type": "service_account",
  "project_id": "CONTOH-PROJECT-ID",
  "private_key_id": "b83...",
  "private_key": "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n",
  "client_email": "bot-name@contoh-project.iam.gserviceaccount.com",
  "client_id": "123...",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/..."
}
"""
# ---------------------------------------------------------

KPRO_TARGET_SHEET_URL = "https://docs.google.com/spreadsheets/d/1wPeYLmInP7JlPCLZ1XYR-A75l9oHQfZ_U2R4Pc6ohVY/"
KPRO_TARGET_SHEET_NAME = "REPORT PS INDIHOME"
KPRO_MICRO_UPDATE_SHEET_NAME = "UPDATE PER 2JAM"

# ==========================================
# 2. FUNGSI GOOGLE SHEETS (DIPERBARUI)
# ==========================================
def get_gspread_client():
    if not HAS_GSPREAD: return None
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    try:
        # Prioritas: Baca dari variabel String di atas (Lebih mudah untuk Render)
        if GOOGLE_JSON_CREDENTIALS and "private_key" in GOOGLE_JSON_CREDENTIALS:
            creds_dict = json.loads(GOOGLE_JSON_CREDENTIALS)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            return gspread.authorize(creds)
            
        # Fallback: Baca dari file jika ada
        elif os.path.exists("credentials.json"):
            creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
            return gspread.authorize(creds)
            
        else:
            logger.warning("Kredensial Google tidak ditemukan (Variable Kosong & File Tidak Ada).")
            return None
    except Exception as e:
        logger.error(f"Google Auth Error: {e}")
        return None

# ... (SISA KODE KE BAWAH SAMA PERSIS DENGAN SEBELUMNYA) ...
