import os
import logging
from dotenv import load_dotenv

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    # Mengurangi 'cerewet' dari aiohttp
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
    logging.getLogger("aiohttp.client").setLevel(logging.WARNING)
    logging.getLogger("aiohttp.web").setLevel(logging.WARNING)

def load_config():
    # Path ke file .env bisa dispesifikan saat memanggil load_dotenv()
    # Jika tidak, ia akan mencari .env di root
    pass

# Panggil setup_logging saat modul ini diimpor
setup_logging()

# --- Variabel Konfigurasi Global ---
# Variabel ini akan diisi setelah load_dotenv() dipanggil di main.py

NODE_ID = os.getenv("NODE_ID", "default_node_id")

NODE_PORT = int(os.getenv("NODE_PORT", 8000))

NODE_HOST = os.getenv("NODE_HOST", "0.0.0.0")

# Membaca daftar peer address (node lain)
peer_str = os.getenv("PEER_ADDRESSES", "")
PEER_ADDRESSES = [addr.strip() for addr in peer_str.split(',') if addr.strip()]
"""Daftar alamat HTTP node lain (peers), misal: ['http://localhost:8001', 'http://localhost:8002']"""

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

logging.info(f"Konfigurasi dimuat (awal): NODE_ID={NODE_ID}, PORT={NODE_PORT}")