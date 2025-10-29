import asyncio
import logging
from aiohttp import web
import json

# Impor BaseNode umum
from src.nodes.base_node import BaseNode 

logger = logging.getLogger(__name__)

class QueueNode(BaseNode):
    """Implementasi Node Antrian Terdistribusi."""

    def __init__(self):
        # Panggil init dari BaseNode (setup jaringan, redis, hash ring)
        super().__init__() 
        
        # Tambahkan rute spesifik untuk Queue
        self._setup_queue_routes()
        logger.info(f"QueueNode {self.port}: Rute spesifik (/publish, /consume, /ack) ditambahkan.")

    def _setup_queue_routes(self):
        """Menambah route untuk endpoint publish dan consume."""
        self.app.router.add_post('/publish', self.handle_publish)
        self.app.router.add_post('/consume', self.handle_consume)
        self.app.router.add_post('/ack', self.handle_ack)
        # Kita bisa gunakan handle_status dari BaseNode

    async def start_node_specific_tasks(self):
        """Tidak ada task background spesifik untuk queue node."""
        logger.info(f"QueueNode {self.port}: Siap menerima request.")
        pass 

    async def stop_node_specific_tasks(self):
        """Tidak ada task background spesifik untuk dihentikan."""
        pass 

    # --- Handler untuk Producer ---
    
    async def handle_publish(self, request):
        """Menangani request /publish dari klien (producer)."""
        try:
            data = await request.json()
            key = data.get('key')
            message = data.get('message')
            
            if not all([key, message]):
                 logger.warning(f"Node {self.port}: Menerima request publish tidak valid: {data}")
                 return web.json_response({'error': 'key and message required'}, status=400)
            
            # 1. Consistent Hashing: Tentukan node target
            target_node_id = self.hash_ring.get_node(key)
            logger.debug(f"Node {self.port}: Hash ring menunjuk ke node {target_node_id} untuk key '{key}'")

            if target_node_id == self.port:
                # Node ini bertanggung jawab, proses secara internal
                logger.info(f"Node {self.port}: Memproses publish untuk key '{key}' secara lokal.")
                result = await self._internal_publish(data)
                return web.json_response(result)
            else:
                # Forward request ke node yang benar
                logger.info(f"Node {self.port}: Meneruskan publish untuk key '{key}' ke node {target_node_id}.")
                response_data, status_code = await self.forward_request(
                    target_node_id, 'publish', data 
                )
                if response_data:
                    return web.json_response(response_data, status=status_code)
                else:
                    logger.error(f"Node {self.port}: Gagal meneruskan publish ke {target_node_id}, status={status_code}")
                    return web.json_response({'error': 'Failed to forward request'}, status=status_code or 500)

        except json.JSONDecodeError:
            logger.error(f"Node {self.port}: Gagal mem-parsing JSON di handle_publish")
            return web.json_response({'error': 'Invalid JSON'}, status=400)
        except Exception as e:
            logger.error(f"Node {self.port}: Error di handle_publish: {e}", exc_info=True)
            return web.json_response({'error': 'Internal server error'}, status=500)

    async def _internal_publish(self, payload):
        """Logika internal untuk mem-publish pesan ke Redis."""
        key = payload['key']
        message = payload['message']
        list_name = f"queue:{key}" 
        
        if not self.redis_client:
            logger.error(f"Node {self.port}: Koneksi Redis tidak tersedia untuk _internal_publish.")
            return {'success': False, 'message': 'Redis connection not available'}
            
        try:
            # 3. Persistence: Simpan ke Redis List
            await self.redis_client.lpush(list_name, message)
            logger.info(f"Node {self.port}: Pesan dipublish ke antrian '{list_name}': {message[:30]}...")
            return {'success': True, 'message': 'Message published'}
        except Exception as e:
            logger.error(f"Node {self.port}: Gagal publish ke Redis: {e}", exc_info=True)
            return {'success': False, 'message': 'Failed to publish to Redis'}

    # --- Handler untuk Consumer ---
    
    async def handle_consume(self, request):
        """Menangani request /consume dari klien (consumer)."""
        try:
            data = await request.json()
            key = data.get('key')
            consumer_id = data.get('consumer_id')
            
            if not all([key, consumer_id]):
                 logger.warning(f"Node {self.port}: Menerima request consume tidak valid: {data}")
                 return web.json_response({'error': 'key and consumer_id required'}, status=400)

            # 1. Consistent Hashing: Tentukan node target
            target_node_id = self.hash_ring.get_node(key)
            logger.debug(f"Node {self.port}: Hash ring menunjuk ke node {target_node_id} untuk key '{key}'")
            
            if target_node_id == self.port:
                logger.info(f"Node {self.port}: Memproses consume untuk key '{key}' secara lokal.")
                result = await self._internal_consume(data)
                return web.json_response(result)
            else:
                logger.info(f"Node {self.port}: Meneruskan consume untuk key '{key}' ke node {target_node_id}.")
                response_data, status_code = await self.forward_request(
                    target_node_id, 'consume', data
                )
                if response_data:
                    return web.json_response(response_data, status=status_code)
                else:
                    logger.error(f"Node {self.port}: Gagal meneruskan consume ke {target_node_id}, status={status_code}")
                    return web.json_response({'error': 'Failed to forward request'}, status=status_code or 500)
        
        except json.JSONDecodeError:
            logger.error(f"Node {self.port}: Gagal mem-parsing JSON di handle_consume")
            return web.json_response({'error': 'Invalid JSON'}, status=400)
        except Exception as e:
            logger.error(f"Node {self.port}: Error di handle_consume: {e}", exc_info=True)
            return web.json_response({'error': 'Internal server error'}, status=500)

    async def _internal_consume(self, payload):
        """Logika internal untuk consume (at-least-once)."""
        key = payload['key']
        consumer_id = payload['consumer_id']
        
        list_name = f"queue:{key}"
        processing_list_name = f"processing:{key}:{consumer_id}"

        if not self.redis_client:
            logger.error(f"Node {self.port}: Koneksi Redis tidak tersedia untuk _internal_consume.")
            return {'success': False, 'message': 'Redis connection not available'}

        try:
            # 5. At-least-once: Cek processing list dulu
            message = await self.redis_client.lindex(processing_list_name, 0) 
            if message:
                 logger.warning(f"Node {self.port}: Consumer {consumer_id} meng-consume ulang pesan dari '{processing_list_name}': {message[:30]}...")
                 return {'success': True, 'message': message, 'status': 're-delivered'}

            # 5. At-least-once: Jika kosong, ambil dari antrian utama pakai BRPOPLPUSH
            # 3. Persistence/Recovery: Operasi atomik ini memindahkan pesan
            message = await self.redis_client.brpoplpush(list_name, processing_list_name, timeout=5)
            
            if message:
                logger.info(f"Node {self.port}: Mengirim pesan dari '{list_name}' ke consumer {consumer_id} (disimpan di '{processing_list_name}'): {message[:30]}...")
                return {'success': True, 'message': message, 'status': 'delivered'}
            else:
                logger.info(f"Node {self.port}: Antrian '{list_name}' kosong untuk consumer {consumer_id}.")
                return {'success': False, 'message': 'Queue empty'}
        
        except Exception as e:
            logger.error(f"Node {self.port}: Gagal consume dari Redis: {e}", exc_info=True)
            return {'success': False, 'message': 'Failed to consume from Redis'}

    # --- Handler untuk Acknowledgment ---
    
    async def handle_ack(self, request):
        """Menangani 'ack' dari consumer setelah selesai memproses."""
        try:
            data = await request.json()
            key = data.get('key')
            consumer_id = data.get('consumer_id')
            message_to_ack = data.get('message')
            
            if not all([key, consumer_id, message_to_ack]):
                 logger.warning(f"Node {self.port}: Menerima request ack tidak valid: {data}")
                 return web.json_response({'error': 'key, consumer_id, and message required'}, status=400)

            # 1. Consistent Hashing: Tentukan node target
            target_node_id = self.hash_ring.get_node(key)
            logger.debug(f"Node {self.port}: Hash ring menunjuk ke node {target_node_id} untuk key '{key}'")
            
            if target_node_id == self.port:
                logger.info(f"Node {self.port}: Memproses ack untuk key '{key}' secara lokal.")
                result = await self._internal_ack(data)
                return web.json_response(result)
            else:
                logger.info(f"Node {self.port}: Meneruskan ack untuk key '{key}' ke node {target_node_id}.")
                response_data, status_code = await self.forward_request(
                    target_node_id, 'ack', data
                )
                if response_data:
                    return web.json_response(response_data, status=status_code)
                else:
                    logger.error(f"Node {self.port}: Gagal meneruskan ack ke {target_node_id}, status={status_code}")
                    return web.json_response({'error': 'Failed to forward request'}, status=status_code or 500)

        except json.JSONDecodeError:
            logger.error(f"Node {self.port}: Gagal mem-parsing JSON di handle_ack")
            return web.json_response({'error': 'Invalid JSON'}, status=400)
        except Exception as e:
            logger.error(f"Node {self.port}: Error di handle_ack: {e}", exc_info=True)
            return web.json_response({'error': 'Internal server error'}, status=500)

    async def _internal_ack(self, payload):
        """Logika internal untuk menghapus pesan dari 'processing' list."""
        key = payload['key']
        consumer_id = payload['consumer_id']
        message_to_ack = payload['message'] # Kita masih butuh ini untuk logging
        
        processing_list_name = f"processing:{key}:{consumer_id}"

        if not self.redis_client:
            logger.error(f"Node {self.port}: Koneksi Redis tidak tersedia untuk _internal_ack.")
            return {'success': False, 'message': 'Redis connection not available'}
        
        try:
            # --- [ PERUBAHAN LOGIKA ACK ] ---
            # Coba hapus elemen pertama dari kiri (head) list processing
            # LPOP mengembalikan elemen yang dihapus, atau None jika list kosong
            removed_message = await self.redis_client.lpop(processing_list_name)
            # -------------------------------
            
            if removed_message is not None:
                # Idealnya, kita bandingkan removed_message dengan message_to_ack
                # untuk memastikan kita menghapus yang benar, tapi LPOP sudah cukup
                # jika kita asumsikan hanya 1 pesan di processing list per consumer.
                if removed_message == message_to_ack:
                    logger.info(f"Node {self.port}: ACK berhasil (LPOP) untuk {consumer_id} pada '{processing_list_name}': {removed_message[:30]}...")
                    return {'success': True, 'message': 'ACK successful'}
                else:
                    # Ini aneh, pesan yang dihapus berbeda dari yang di-ACK
                    logger.error(f"Node {self.port}: ACK Error - LPOP menghapus '{removed_message[:30]}' tapi diharapkan '{message_to_ack[:30]}' dari {processing_list_name}")
                    # Mungkin masukkan kembali pesan yang salah dihapus? Atau biarkan?
                    # Untuk sekarang, anggap gagal.
                    # Masukkan kembali ke KIRI (head) agar dicoba lagi
                    await self.redis_client.lpush(processing_list_name, removed_message)
                    return {'success': False, 'message': 'ACK failed: Mismatched message popped'}
            else:
                # LPOP mengembalikan None, berarti list sudah kosong
                logger.warning(f"Node {self.port}: ACK GAGAL (LPOP): List {processing_list_name} sudah kosong untuk {consumer_id}. Pesan: {message_to_ack[:30]}...")
                return {'success': False, 'message': 'Processing list was empty or message already ACKed'}

        except Exception as e:
            logger.error(f"Node {self.port}: Gagal ACK (LPOP) di Redis: {e}", exc_info=True)
            return {'success': False, 'message': 'Failed to ACK in Redis'}