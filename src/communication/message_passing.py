import logging
import aiohttp

logger = logging.getLogger(__name__)

async def send_message_async(session: aiohttp.ClientSession, url: str, payload: dict) -> dict | None:
    target_url = f"{url}/message"
    logger.info(f"Mengirim pesan ke {target_url} dengan payload: {payload}")
    
    try:
        async with session.post(target_url, json=payload, timeout=5) as response:
            response.raise_for_status()  # Error jika status >= 400
            resp_json = await response.json()
            logger.info(f"Respon diterima dari {target_url}: {resp_json}")
            return resp_json
    except aiohttp.ClientConnectorError:
        logger.error(f"Gagal terhubung ke {target_url}. Node mungkin mati.")
        return None
    except aiohttp.ClientResponseError as e:
        logger.error(f"Error HTTP dari {target_url}: {e.status} - {e.message}")
        return None
    except Exception as e:
        logger.error(f"Error saat mengirim pesan ke {target_url}: {e}")
        return None