import asyncio
import logging
from aiohttp import web, ClientConnectorError
import json

# Impor BaseNode umum
from src.nodes.base_node import BaseNode 
# Impor Raft dan LockManager lagi
from src.consensus.raft import RaftNode, RaftState 
from src.nodes.lock_manager import LockManager

logger = logging.getLogger(__name__)

class BaseRaftLockNode(BaseNode): # <-- Mewarisi BaseNode
    """Node khusus untuk Lock Manager yang menggunakan Raft."""

    def __init__(self):
        # Panggil init dari BaseNode (setup jaringan, redis, hash ring)
        super().__init__() 

        # --- Inisialisasi spesifik Raft & Lock ---
        self.lock_manager = LockManager()
        # Menyimpan future untuk request klien yg menunggu commit
        self.pending_requests = {} # Format: {log_index: asyncio.Future}

        # Inisialisasi RaftNode, berikan callback apply
        # Gunakan self.port sebagai ID Raft
        self.raft_logic = RaftNode(
            node_id=self.port, 
            peers=list(self.all_node_ids - {self.port}), # Peers = semua node - diri sendiri
            message_sender=self.send_raft_message,
            apply_callback=self.apply_command 
        )
        # ----------------------------------------

        # Tambahkan route spesifik Raft & Lock
        self._setup_lock_routes()

    def _setup_lock_routes(self):
        """Menambah route untuk Raft RPC dan endpoint lock."""
        self.app.router.add_post('/raft_rpc', self.handle_raft_rpc)
        self.app.router.add_post('/acquire_lock', self.handle_acquire_lock)
        self.app.router.add_post('/release_lock', self.handle_release_lock)
        # Override status handler untuk menambah info Raft
        self.app.router.add_get('/', self.handle_status) 

    # --- Override start_server untuk memulai Raft ---
    async def start_server(self):
        # Panggil start_server dari BaseNode (mulai http, redis)
        await super().start_server() 
        # Setelah server dasar jalan, mulai Raft
        # (Loop utama 'run forever' sudah ada di BaseNode.start_server)
        if self.raft_logic: # Pastikan raft_logic ada
             await self.raft_logic.start_raft_node()

    # --- Override stop_server untuk menghentikan Raft ---
    async def stop_server(self):
        # Hentikan Raft dulu
        if self.raft_logic:
             self.raft_logic._stop_raft_node_tasks() 
        # Batalkan request pending
        for index, future in self.pending_requests.items():
            if not future.done():
                 logger.warning(f"Membatalkan request lock di index {index} karena shutdown.")
                 future.set_exception(asyncio.CancelledError("Node is shutting down"))
        self.pending_requests.clear()
        # Panggil stop_server dari BaseNode (trigger cleanup http, redis)
        await super().stop_server() 

    # --- Override status handler ---
    async def handle_status(self, request):
        """Endpoint status yang menyertakan info Raft & Lock."""
        # Ambil status dasar dari BaseNode
        base_status_resp = await super().handle_status(request)
        base_status = json.loads(base_status_resp.text) # Decode JSON

        # Tambahkan info Raft & Lock
        base_status.update({
            'raft_state': self.raft_logic.state.name,
            'raft_term': self.raft_logic.current_term,
            'raft_leader': self.raft_logic.current_leader,
            'log_length': len(self.raft_logic.log),
            'commit_index': self.raft_logic.commit_index,
            'last_applied': self.raft_logic.last_applied,
            'locks': self.lock_manager.locks 
        })
        return web.json_response(base_status)

    # --- Metode Raft & Lock (disalin dari base_node.py sebelumnya) ---

    async def send_raft_message(self, target_id, message_type, payload):
        # (Kode ini SAMA seperti di base_node.py sebelumnya)
        if self.http_session is None or self.http_session.closed: logger.error(f"Node {self.port}: Sesi HTTP tidak tersedia..."); return
        target_url = None # Cari URL dari self.peer_nodes_config atau self.peer_urls_map
        for url, port in self.peer_nodes_config.items():
             if port == int(target_id): target_url = url; break
        if not target_url: logger.error(f"Node {self.port}: Tidak tahu URL untuk peer ID {target_id}"); return
        full_url = f"{target_url}/raft_rpc"
        message_body = {'sender_id': self.port, 'message_type': message_type, 'payload': payload}
        original_message_type = message_type
        try:
            logger.debug(f"Node {self.port}: Mengirim '{message_type}' ke {target_id} di {full_url}")
            response_data = None
            async with self.http_session.post(full_url, json=message_body, timeout=0.5) as response:
                response_data = await response.json()
                logger.debug(f"Node {self.port}: Menerima respons dari {target_id} untuk '{message_type}': {response_data}")
            if response_data and original_message_type in ['request_vote', 'append_entries']:
                response_type = f"{original_message_type}_response"
                asyncio.create_task(self.raft_logic.handle_rpc(target_id, response_type, response_data))
        except ClientConnectorError: logger.warning(f"Node {self.port}: Gagal terhubung ke peer {target_id} di {full_url}")
        except asyncio.TimeoutError: logger.warning(f"Node {self.port}: Timeout saat mengirim ke peer {target_id} di {full_url}")
        except Exception as e: logger.error(f"Node {self.port}: Error saat mengirim ke {target_id}: {e}", exc_info=False)

    async def handle_raft_rpc(self, request):
        # (Kode ini SAMA seperti di base_node.py sebelumnya)
        try:
            data = await request.json()
            sender_id = data.get('sender_id'); message_type = data.get('message_type'); payload = data.get('payload')
            if not all([sender_id, message_type, payload is not None]):
                logger.warning(f"Menerima RPC Raft tidak lengkap: {data}")
                return web.json_response({'error': 'Bad request'}, status=400)
            response_payload = await self.raft_logic.handle_rpc(sender_id, message_type, payload)
            return web.json_response(response_payload) if response_payload else web.json_response({'status': 'ok'})
        except json.JSONDecodeError: logger.error("Gagal mem-parsing JSON RPC Raft"); return web.json_response({'error': 'Invalid JSON'}, status=400)
        except Exception as e: logger.error(f"Error di handle_raft_rpc: {e}", exc_info=True); return web.json_response({'error': 'Internal server error'}, status=500)

    async def apply_command(self, index, command):
        # (Kode ini SAMA seperti di base_node.py sebelumnya)
        logger.info(f"Node {self.port}: [APPLY] Menerapkan command di index {index}: {command}")
        result = self.lock_manager.handle_command(command)
        future = self.pending_requests.pop(index, None) 
        if future and not future.done():
            logger.info(f"Node {self.port}: [APPLY] Memanggil future.set_result() untuk index {index}")
            future.set_result(result) 
        elif future and future.done():
            logger.warning(f"Node {self.port}: [APPLY] Menemukan future untuk index {index} tapi sudah selesai.")
        return result

    async def handle_acquire_lock(self, request):
        # (Kode ini SAMA seperti di base_node.py sebelumnya)
        if self.raft_logic.state != RaftState.LEADER:
             # Cari URL leader dari config
             leader_url = None
             for url, port in self.peer_nodes_config.items():
                  if port == self.raft_logic.current_leader: leader_url = url; break
             return web.json_response({'success': False, 'message': 'Not a leader', 'leader_url': leader_url}, status=400)
        log_index = -1; future = None
        try:
            data = await request.json(); lock_name = data.get('lock_name'); client_id = data.get('client_id'); lock_type = data.get('type', 'exclusive') 
            if not all([lock_name, client_id]): return web.json_response({'success': False, 'message': 'lock_name and client_id required'}, status=400)
            command = {'op': 'acquire', 'lock_name': lock_name, 'client_id': client_id, 'type': lock_type}
            loop = asyncio.get_running_loop(); future = loop.create_future()
            logger.info(f"Node {self.port}: [HANDLER] Memanggil submit_command untuk command: {command}")
            log_index, error_response = await self.raft_logic.submit_command(command)
            logger.info(f"Node {self.port}: [HANDLER] Selesai submit_command, index={log_index}, error={error_response}")
            if error_response: return web.json_response(error_response, status=400)
            self.pending_requests[log_index] = future
            try:
                logger.info(f"Node {self.port}: [HANDLER] Mulai menunggu future untuk index {log_index} (timeout 10s)")
                result = await asyncio.wait_for(future, timeout=10.0) 
                logger.info(f"Node {self.port}: [HANDLER] future SELESAI untuk index {log_index} dengan hasil: {result}")
                return web.json_response(result)
            except asyncio.TimeoutError:
                logger.warning(f"Node {self.port}: [HANDLER] Timeout menunggu future untuk index {log_index}")
                if log_index != -1 and log_index in self.pending_requests: 
                    f = self.pending_requests.pop(log_index); 
                    if not f.done(): f.cancel("Timeout in handler") 
                return web.json_response({'success': False, 'message': 'Request timed out waiting for commit'}, status=508)
        except Exception as e:
            logger.error(f"Error di handle_acquire_lock: {e}", exc_info=True)
            if log_index != -1 and log_index in self.pending_requests: 
                f = self.pending_requests.pop(log_index, None)
                if f and not f.done(): f.cancel("Exception in handler")
            return web.json_response({'error': 'Internal server error'}, status=500)

    async def handle_release_lock(self, request):
        # (Kode ini SAMA seperti di base_node.py sebelumnya)
        if self.raft_logic.state != RaftState.LEADER:
             leader_url = None
             for url, port in self.peer_nodes_config.items():
                  if port == self.raft_logic.current_leader: leader_url = url; break
             return web.json_response({'success': False, 'message': 'Not a leader', 'leader_url': leader_url}, status=400)
        log_index = -1; future = None
        try:
            data = await request.json(); lock_name = data.get('lock_name'); client_id = data.get('client_id')
            if not all([lock_name, client_id]): return web.json_response({'success': False, 'message': 'lock_name and client_id required'}, status=400)
            command = {'op': 'release', 'lock_name': lock_name, 'client_id': client_id}
            loop = asyncio.get_running_loop(); future = loop.create_future()
            logger.info(f"Node {self.port}: [HANDLER] Memanggil submit_command untuk command: {command}")
            log_index, error_response = await self.raft_logic.submit_command(command)
            logger.info(f"Node {self.port}: [HANDLER] Selesai submit_command, index={log_index}, error={error_response}")
            if error_response: return web.json_response(error_response, status=400)
            self.pending_requests[log_index] = future
            try:
                logger.info(f"Node {self.port}: [HANDLER] Mulai menunggu future untuk index {log_index} (timeout 10s)")
                result = await asyncio.wait_for(future, timeout=10.0)
                logger.info(f"Node {self.port}: [HANDLER] future SELESAI untuk index {log_index} dengan hasil: {result}")
                return web.json_response(result)
            except asyncio.TimeoutError:
                logger.warning(f"Node {self.port}: [HANDLER] Timeout menunggu future untuk index {log_index}")
                if log_index != -1 and log_index in self.pending_requests: 
                    f = self.pending_requests.pop(log_index)
                    if not f.done(): f.cancel("Timeout in handler")
                return web.json_response({'success': False, 'message': 'Request timed out waiting for commit'}, status=508)
        except Exception as e:
            logger.error(f"Error di handle_release_lock: {e}", exc_info=True)
            if log_index != -1 and log_index in self.pending_requests: 
                f = self.pending_requests.pop(log_index, None)
                if f and not f.done(): f.cancel("Exception in handler")
            return web.json_response({'error': 'Internal server error'}, status=500)