import asyncio
import logging
from aiohttp import web, ClientSession, ClientConnectorError
import json
import redis.asyncio as redis 

from src.utils import config
from src.utils.hashing import ConsistentHashRing 

logger = logging.getLogger(__name__)

class BaseNode: 
    def __init__(self):
        try:
            self.node_id_str = config.NODE_ID 
            self.host = config.NODE_HOST 
            self.port = int(config.NODE_PORT)
            self.peer_urls = config.PEER_ADDRESSES if config.PEER_ADDRESSES else []
            self.peer_nodes_config = {} 
            self.all_node_ids = {self.port} 
            for url in self.peer_urls:
                 try:
                      peer_port = int(url.split(':')[-1])
                      if peer_port != self.port:
                           self.peer_nodes_config[url] = peer_port
                           self.all_node_ids.add(peer_port)
                 except (ValueError, IndexError):
                      logger.error(f"Format PEER_ADDRESSES tidak valid: {url}")
            self.redis_host = config.REDIS_HOST
            self.redis_port = config.REDIS_PORT
            self.redis_client = None 
            self.hash_ring = ConsistentHashRing(nodes=list(self.all_node_ids))
            logger.info(f"BaseNode '{self.node_id_str}' (Port: {self.port}) diinisialisasi. Peer URLs: {self.peer_urls}. All Node IDs: {self.all_node_ids}")
            self.app = web.Application()
            self.http_session = None
            self.runner = None
            self.site = None 
            self._setup_base_routes()
        except Exception as e:
            logger.critical(f"Gagal mem-parsing konfigurasi node: {e}", exc_info=True)
            raise

    def _setup_base_routes(self):
        """Menyiapkan rute dasar. Subclass akan menambah rute lain."""
        self.app.router.add_get('/', self.handle_status)
        self.app.router.add_post('/internal_forward', self.handle_internal_forward)

    async def start_server(self):
        """Memulai server aiohttp, koneksi Redis, dan berjalan selamanya."""
        try:
            logger.info(f"Menghubungkan ke Redis di {self.redis_host}:{self.redis_port}")
            self.redis_client = redis.Redis(host=self.redis_host, port=self.redis_port, decode_responses=True)
            await self.redis_client.ping() 
            logger.info("Terhubung ke Redis.")
            self.http_session = ClientSession()
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            self.site = web.TCPSite(self.runner, self.host, self.port) 
            await self.site.start()
            logger.info(f"Server HTTP berjalan di http://{self.host}:{self.port}")
            await self.start_node_specific_tasks()
            
            logger.info(f"Node {self.node_id_str} berjalan. Tekan Ctrl+C untuk berhenti.")
            self.main_loop_future = asyncio.Future() 
            await self.main_loop_future 
        except redis.exceptions.ConnectionError as e:
             logger.critical(f"Gagal terhubung ke Redis: {e}")
             if self.runner: await self.runner.cleanup()
             if self.http_session: await self.http_session.close()
             raise
        except asyncio.CancelledError:
            logger.info(f"Node {self.node_id_str}: start_server dibatalkan.")
        except Exception as e:
            logger.critical(f"Gagal memulai/menjalankan server: {e}", exc_info=True)
        finally:
            logger.info(f"Node {self.node_id_str}: Memulai cleanup start_server...")
            if self.site: await self.site.stop(); self.site = None
            if self.runner: await self.runner.cleanup(); self.runner = None
            if self.http_session and not self.http_session.closed : await self.http_session.close(); self.http_session = None
            if self.redis_client: await self.redis_client.close(); self.redis_client = None
            logger.info(f"Node {self.node_id_str}: Cleanup start_server selesai.")

    async def start_node_specific_tasks(self):
        """Akan di-override oleh subclass (misal: RaftNode, QueueNode)."""
        logger.debug("BaseNode: Tidak ada task spesifik untuk dimulai.")
        pass # BaseNode tidak melakukan apa-apa

    async def stop_server(self):
        """Menghentikan main loop dan task spesifik node."""
        logger.info(f"Node {self.node_id_str}: Menghentikan node...")
        
        # Panggil hook stop spesifik node
        await self.stop_node_specific_tasks()
        
        if hasattr(self, 'main_loop_future') and not self.main_loop_future.done():
             self.main_loop_future.cancel()

    async def stop_node_specific_tasks(self):
        logger.debug("BaseNode: Tidak ada task spesifik untuk dihentikan.")
        pass # BaseNode tidak melakukan apa-apa

    async def handle_status(self, request):
        status = {
            'node_id_str': self.node_id_str,
            'port': self.port,
            'peers': self.peer_urls,
            'hash_ring_nodes': list(self.all_node_ids)
        }
        try:
             await self.redis_client.ping()
             status['redis_status'] = 'connected'
        except Exception:
             status['redis_status'] = 'disconnected'
        return web.json_response(status)

    async def forward_request(self, target_node_id, endpoint, payload):
        if not self.http_session or self.http_session.closed:
            logger.error("Sesi HTTP tidak tersedia untuk forwarding.")
            return None, 503 
        target_url = None
        for url, port in self.peer_nodes_config.items():
             if port == target_node_id:
                  target_url = url
                  break
        if not target_url:
             logger.error(f"Tidak tahu URL untuk node ID {target_node_id}")
             return None, 404 
        
        # [PERBAIKAN] Hapus / di depan endpoint jika ada
        clean_endpoint = endpoint.lstrip('/')
        forward_url = f"{target_url}/internal_forward"
        forward_payload = {
             'original_endpoint': clean_endpoint, # Kirim nama endpoint bersih
             'original_payload': payload
        }
        logger.info(f"Meneruskan request untuk '{clean_endpoint}' ke node {target_node_id} ({forward_url})")
        try:
            async with self.http_session.post(forward_url, json=forward_payload, timeout=5.0) as response:
                resp_data = await response.json()
                return resp_data, response.status
        except ClientConnectorError: logger.warning(f"Gagal terhubung ke node {target_node_id} untuk forwarding."); return None, 503
        except asyncio.TimeoutError: logger.warning(f"Timeout saat forwarding ke node {target_node_id}."); return None, 504 
        except Exception as e: logger.error(f"Error saat forwarding ke node {target_node_id}: {e}"); return None, 500

    async def handle_internal_forward(self, request):
        """Menangani request yang diforward dari node lain."""
        try:
             data = await request.json()
             original_endpoint = data.get('original_endpoint')
             original_payload = data.get('original_payload')
             
             if not original_endpoint or original_payload is None:
                  return web.json_response({'error': 'Bad forward request'}, status=400)
                  
             logger.info(f"Menerima forwarded request untuk endpoint '{original_endpoint}'")
             
             # [PERBAIKAN] Panggil handler internal (misal: _internal_publish)
             internal_handler_name = f"_internal_{original_endpoint}"
             if hasattr(self, internal_handler_name):
                 internal_handler = getattr(self, internal_handler_name)
                 # Panggil handler internal dengan payload-nya
                 result = await internal_handler(original_payload)
                 return web.json_response(result)
             else:
                 logger.error(f"Handler internal '{internal_handler_name}' tidak ditemukan untuk forwarded request.")
                 return web.json_response({'error': 'Internal handler not found'}, status=501) 
        except Exception as e:
             logger.error(f"Error di handle_internal_forward: {e}", exc_info=True)
             return web.json_response({'error': 'Internal server error'}, status=500)

