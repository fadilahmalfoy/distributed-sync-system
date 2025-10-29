import asyncio
import aiohttp
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - TEST - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def run_test():
    test_url = "http://localhost:8001/broadcast"
    test_payload = {
        "message": "Halo semua! Ini tes broadcast dari node-1."
    }
    
    logger.info(f"Memulai tes: Mengirim POST ke {test_url} dengan {test_payload}")
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(test_url, json=test_payload, timeout=10) as response:
                response.raise_for_status()
                result = await response.json()
                logger.info(f"Respon sukses dari node-1: {result}")
                
    except Exception as e:
        logger.error(f"Tes Gagal: Error saat menghubungi {test_url}. {e}")
        logger.error("Pastikan node-1 (port 8001) berjalan.")
        return

    logger.info("Tes selesai.")
    logger.info("--- PERIKSA LOG DI TERMINAL NODE-2 & NODE-3 ---")
    logger.info("Anda seharusnya melihat 'Pesan diterima: ...' di kedua terminal tersebut.")

if __name__ == "__main__":
    try:
        asyncio.run(run_test())
    except KeyboardInterrupt:
        logger.info("Tes dihentikan.")

