import asyncio
import argparse
import logging
from dotenv import load_dotenv
import sys

from src.utils import config

logger = logging.getLogger(__name__)
node_instance = None

async def main_async(env_file, node_type): 
    """Fungsi async utama untuk setup dan menjalankan node."""
    
    if not load_dotenv(dotenv_path=env_file):
        logger.error(f"Error: Tidak dapat menemukan file .env di {env_file}")
        return
    logger.info(f"File konfigurasi {env_file} berhasil dimuat.")

    import importlib
    importlib.reload(config)
    
    NodeClass = None # Inisialisasi
    if node_type == 'lock':
        from src.nodes.raft_lock_node import BaseRaftLockNode # Ganti nama impor
        NodeClass = BaseRaftLockNode 
        logger.info("Tipe node: Lock Manager (Raft)")
    elif node_type == 'queue':
        from src.nodes.queue_node import QueueNode 
        NodeClass = QueueNode 
        logger.info("Tipe node: Queue Node")
    # --- [ BARU ] ---
    elif node_type == 'cache':
        from src.nodes.cache_node import CacheNode # Impor CacheNode
        NodeClass = CacheNode
        logger.info("Tipe node: Cache Node (MESI)")
    # ---------------
    else:
        logger.error(f"Tipe node tidak dikenal: {node_type}. Gunakan 'lock', 'queue', atau 'cache'.")
        return

    global node_instance
    
    try:
        node_instance = NodeClass() 
        await node_instance.start_server() 
        
        logger.info(f"Node {config.NODE_ID} (tipe: {node_type}) berjalan. Tekan Ctrl+C untuk berhenti.")
        await asyncio.Event().wait() 
        
    except asyncio.CancelledError:
        logger.info(f"Node {config.NODE_ID}: Diterima sinyal pembatalan.")
    except Exception as e:
        logger.error(f"Terjadi error saat menjalankan main_async: {e}", exc_info=True)
    finally:
        logger.info("Membersihkan sebelum keluar...")
        if node_instance:
            await node_instance.stop_server()

def main():
    parser = argparse.ArgumentParser(description="Menjalankan node sistem terdistribusi.")
    parser.add_argument(
        "--env-file", type=str, required=True,
        help="Path ke file .env."
    )
    # --- [ UBAH choices ] ---
    parser.add_argument(
        "--node-type", type=str, required=True, choices=['lock', 'queue', 'cache'], # Tambah 'cache'
        help="Tipe node ('lock', 'queue', atau 'cache')."
    )
    # -----------------------
    args = parser.parse_args()

    try:
        asyncio.run(main_async(args.env_file, args.node_type)) 
    except KeyboardInterrupt:
        logger.info("Diterima KeyboardInterrupt, keluar...")
    finally:
        logger.info("Program utama selesai.")

if __name__ == "__main__":
    main()