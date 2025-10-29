import asyncio
import logging
from aiohttp import web
from aiohttp.client_exceptions import ClientConnectorError
import json
from enum import Enum
from collections import OrderedDict # Untuk LRU Cache

from src.nodes.base_node import BaseNode 

logger = logging.getLogger(__name__)

class MESIState(Enum):
    MODIFIED = 'M'   # Data valid, diubah, hanya ada di cache ini
    EXCLUSIVE = 'E'  # Data valid, tidak diubah, hanya ada di cache ini
    SHARED = 'S'     # Data valid, tidak diubah, mungkin ada di cache lain
    INVALID = 'I'    # Data tidak valid

CACHE_SIZE = 10 # Bisa dipindahkan ke config nanti

class CacheNode(BaseNode):
    """Implementasi Node Cache dengan protokol koherensi MESI dan LRU."""

    def __init__(self):
        # Panggil init dari BaseNode (setup jaringan, redis, hash ring - redis/hash tidak dipakai di sini)
        super().__init__() 
        
        # Inisialisasi Cache Lokal (LRU)
        # Key: Alamat/Key data, Value: {'state': MESIState, 'value': data_value}
        self.cache = OrderedDict() 
        
        # [Poin 5] Inisialisasi Metrik Performa Dasar
        self.metrics = {
            "local_reads": 0,
            "local_writes": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "invalidations_sent": 0,
            "invalidations_received": 0,
            "read_requests_received": 0,
            "write_requests_received": 0,
            "data_responses_sent": 0,
        }
        
        # Tambahkan rute spesifik untuk Cache
        self._setup_cache_routes()
        logger.info(f"CacheNode {self.port}: Rute spesifik (/read, /write, /cache_event) ditambahkan. Cache size: {CACHE_SIZE}")

    def _setup_cache_routes(self):
        """Menambah route untuk endpoint read, write, dan event snooping."""
        self.app.router.add_get('/read', self.handle_local_read)      # Klien membaca dari node ini
        self.app.router.add_post('/write', self.handle_local_write)    # Klien menulis ke node ini
        self.app.router.add_post('/cache_event', self.handle_cache_event) # Node lain mengirim event (snooping)
        # Override status handler untuk menambah info Cache
        self.app.router.add_get('/', self.handle_status) 

    async def start_node_specific_tasks(self):
        """Tidak ada task background spesifik untuk cache node."""
        logger.info(f"CacheNode {self.port}: Siap menerima request.")
        pass 

    async def stop_node_specific_tasks(self):
        """Tidak ada task background spesifik untuk dihentikan."""
        pass 

    async def handle_status(self, request):
        """Endpoint status yang menyertakan info Cache."""
        base_status_resp = await super().handle_status(request)
        status_data = json.loads(base_status_resp.text) 
        
        # Ubah OrderedDict menjadi dict biasa untuk JSON serialization
        cache_dict = {k: v for k, v in self.cache.items()}
        
        status_data.update({
            'node_type': 'CacheNode',
            'cache_size': len(self.cache),
            'cache_capacity': CACHE_SIZE,
            'cache_content': cache_dict, # Tampilkan isi cache (untuk debug)
            'metrics': self.metrics
        })
        return web.json_response(status_data)

    # --- [Poin 4] Helper LRU ---
    def _update_lru(self, key):
        """Memindahkan key yang baru diakses ke akhir OrderedDict (paling baru)."""
        if key in self.cache:
            self.cache.move_to_end(key)
            
    def _evict_lru(self):
        """Mengeluarkan item yang paling lama tidak diakses jika cache penuh."""
        if len(self.cache) >= CACHE_SIZE:
            # last=False mengeluarkan item pertama (paling lama)
            evicted_key, evicted_value = self.cache.popitem(last=False) 
            logger.info(f"Node {self.port}: Cache penuh, mengeluarkan LRU item: '{evicted_key}' (State: {evicted_value['state'].name})")
            # TODO: Tambahan - Jika state M, kita mungkin perlu write-back ke memori utama (tidak disimulasikan di sini)


    # --- [Poin 1, 2, 3] Logika Inti MESI ---
    
    async def handle_local_read(self, request):
        """Handler saat CPU lokal (atau klien via endpoint ini) ingin membaca data."""
        key = request.query.get('key')
        if not key:
            return web.json_response({'error': 'key required'}, status=400)
            
        logger.info(f"Node {self.port}: Menerima Local Read untuk key '{key}'")
        self.metrics["local_reads"] += 1

        if key in self.cache:
            # Cache Hit!
            self.metrics["cache_hits"] += 1
            cache_entry = self.cache[key]
            current_state = cache_entry['state']
            value = cache_entry['value']
            
            # Update LRU
            self._update_lru(key)
            
            logger.info(f"Node {self.port}: Cache Hit untuk '{key}'. State: {current_state.name}. Value: {value}")
            # Tidak perlu transisi state pada cache hit (M->M, E->E, S->S)
            return web.json_response({'key': key, 'value': value, 'state': current_state.name, 'status': 'cache_hit'})
        else:
            # Cache Miss!
            self.metrics["cache_misses"] += 1
            logger.info(f"Node {self.port}: Cache Miss untuk '{key}'. Mengirim Read Request ke bus...")
 
            event_payload = {'type': 'READ_MISS', 'key': key, 'origin_node_id': self.port}
            responses = await self._broadcast_event(event_payload)
            
            # Cek respons dari node lain
            found_in_other_cache = False
            shared_data = None
            for resp in responses:
                # Cek apakah respons valid dan menandakan data ada di cache lain
                if resp and resp.get('shared') and resp.get('key') == key:
                    found_in_other_cache = True
                    shared_data = resp.get('value')
                    logger.info(f"Node {self.port}: Data '{key}' ditemukan di cache node lain (shared). Value: {shared_data}")
                    break # Cukup satu respons shared

            # Evict jika perlu sebelum menambah item baru
            self._evict_lru()
            
            if found_in_other_cache:
                # Data ada di cache lain -> state jadi Shared (S)
                new_state = MESIState.SHARED
                value = shared_data
            else:
                # Data tidak ada di cache lain (hanya di memori) -> state jadi Exclusive (E)
                new_state = MESIState.EXCLUSIVE
                value = f"data_for_{key}_from_memory" # Simulasi data dari memori
            
            # Tambahkan ke cache lokal
            self.cache[key] = {'state': new_state, 'value': value}
            self._update_lru(key) # Pindahkan ke akhir (paling baru)
            
            logger.info(f"Node {self.port}: Data '{key}' dimasukkan ke cache. State: {new_state.name}. Value: {value}")
            return web.json_response({'key': key, 'value': value, 'state': new_state.name, 'status': 'cache_miss_loaded'})


    async def handle_local_write(self, request):
        """Handler saat CPU lokal (atau klien via endpoint ini) ingin menulis data."""
        try:
            data = await request.json()
            key = data.get('key')
            value = data.get('value')
            
            if not all([key, value is not None]):
                 return web.json_response({'error': 'key and value required'}, status=400)

            logger.info(f"Node {self.port}: Menerima Local Write untuk key '{key}' dengan value '{value}'")
            self.metrics["local_writes"] += 1

            if key not in self.cache:
                 self._evict_lru()

            new_state = MESIState.MODIFIED 
            current_state = None
            if key in self.cache:
                current_state = self.cache[key]['state']

            # Update cache lokal
            self.cache[key] = {'state': new_state, 'value': value}
            self._update_lru(key)

            logger.info(f"Node {self.port}: Cache diupdate untuk '{key}'. State baru: {new_state.name}. Value: {value}")

            if current_state in [MESIState.SHARED, MESIState.INVALID] or current_state is None:
                logger.info(f"Node {self.port}: Mengirim INVALIDATE untuk key '{key}' ke bus...")
                event_payload = {'type': 'INVALIDATE', 'key': key, 'origin_node_id': self.port}
                await self._broadcast_event(event_payload)
                self.metrics["invalidations_sent"] += 1
            
            return web.json_response({'key': key, 'value': value, 'state': new_state.name, 'status': 'write_complete'})

        except json.JSONDecodeError:
            return web.json_response({'error': 'Invalid JSON'}, status=400)
        except Exception as e:
            logger.error(f"Node {self.port}: Error di handle_local_write: {e}", exc_info=True)
            return web.json_response({'error': 'Internal server error'}, status=500)


    async def handle_cache_event(self, request):
        """Handler untuk event yang diterima dari node lain (snooping)."""
        try:
            event = await request.json()
            event_type = event.get('type')
            key = event.get('key')
            origin_node_id = event.get('origin_node_id')
            
            if not all([event_type, key, origin_node_id]) or origin_node_id == self.port:
                # Abaikan event tanpa data lengkap atau dari diri sendiri
                return web.json_response({'status': 'ignored'}) 

            logger.debug(f"Node {self.port}: Menerima event '{event_type}' untuk key '{key}' dari node {origin_node_id}")

            response = {'status': 'event_received', 'key': key}

            if key in self.cache:
                cache_entry = self.cache[key]
                current_state = cache_entry['state']
                
                # Update LRU jika kita merespons/mengubah state
                update_lru_needed = False

                if event_type == 'READ_MISS':
                    self.metrics["read_requests_received"] += 1
                    # Node lain miss saat read. Jika kita punya data (M, E, S), kita supply.
                    if current_state in [MESIState.MODIFIED, MESIState.EXCLUSIVE, MESIState.SHARED]:
                        # Supply data dan ubah state kita (dan origin) menjadi Shared (S)
                        if current_state in [MESIState.MODIFIED, MESIState.EXCLUSIVE]:
                            cache_entry['state'] = MESIState.SHARED
                            logger.info(f"Node {self.port}: Merespons READ_MISS '{key}'. Mengirim data & ubah state ke SHARED.")
                        else: # Sudah SHARED
                             logger.info(f"Node {self.port}: Merespons READ_MISS '{key}'. Mengirim data (state tetap SHARED).")
                        
                        response['shared'] = True # Menandakan kita punya data
                        response['value'] = cache_entry['value']
                        self.metrics["data_responses_sent"] += 1
                        update_lru_needed = True

                elif event_type == 'INVALIDATE':
                    self.metrics["invalidations_received"] += 1
                    # Node lain melakukan write. Jika kita punya data (M, E, S), state jadi Invalid (I).
                    if current_state != MESIState.INVALID:
                        logger.info(f"Node {self.port}: Menerima INVALIDATE '{key}'. Mengubah state ke INVALID.")
                        cache_entry['state'] = MESIState.INVALID
                        
                # TODO: Tambahkan penanganan untuk event lain jika diperlukan (misal: Read-With-Intent-To-Modify / RWITM)

                if update_lru_needed:
                     self._update_lru(key)
            else:
                 # Kita tidak punya data untuk key ini, abaikan event (kecuali metrik?)
                 if event_type == 'READ_MISS': self.metrics["read_requests_received"] += 1
                 if event_type == 'INVALIDATE': self.metrics["invalidations_received"] += 1
                 pass 

            return web.json_response(response)

        except Exception as e:
            logger.error(f"Node {self.port}: Error di handle_cache_event: {e}", exc_info=True)
            return web.json_response({'error': 'Internal server error'}, status=500)


    async def _broadcast_event(self, payload):
        """Mengirim event ke semua peer node."""
        if not self.http_session or self.http_session.closed:
            logger.error("Sesi HTTP tidak tersedia untuk broadcast.")
            return []

        tasks = []
        full_url = "/cache_event" # Endpoint tujuan di node lain
        
        logger.debug(f"Node {self.port}: Memulai broadcast event: {payload}")
        
        # Kirim ke semua URL peer yang terkonfigurasi
        for peer_url in self.peer_urls: 
            target_url = f"{peer_url}{full_url}"
            # Buat task untuk setiap pengiriman
            tasks.append(
                asyncio.create_task(self._send_event_to_peer(target_url, payload))
            )
        
        # Jalankan semua task dan kumpulkan hasilnya
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_responses = []
        for i, res in enumerate(results):
            if isinstance(res, Exception):
                logger.warning(f"Node {self.port}: Gagal broadcast ke {self.peer_urls[i]}: {res}")
            elif res: # Jika bukan None
                valid_responses.append(res)
                
        logger.debug(f"Node {self.port}: Broadcast selesai. Menerima {len(valid_responses)} respons valid.")
        return valid_responses

    async def _send_event_to_peer(self, target_url, payload):
        """Helper untuk mengirim satu event ke satu peer."""
        try:
            async with self.http_session.post(target_url, json=payload, timeout=0.5) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"Node {self.port}: Menerima status {response.status} saat broadcast ke {target_url}")
                    return None
        except ClientConnectorError:
            # logger.warning(f"Node {self.port}: Gagal terhubung ke {target_url} saat broadcast.")
            return None # Node mungkin mati, ini normal
        except asyncio.TimeoutError:
            logger.warning(f"Node {self.port}: Timeout saat broadcast ke {target_url}.")
            return None
        except Exception as e:
            logger.error(f"Node {self.port}: Error saat broadcast ke {target_url}: {e}")
            return None