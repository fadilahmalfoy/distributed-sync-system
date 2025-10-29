import asyncio
import random
import logging
from enum import Enum
import math 

logger = logging.getLogger(__name__)

class RaftState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

ELECTION_TIMEOUT_MIN = 1.5  
ELECTION_TIMEOUT_MAX = 3.0  
HEARTBEAT_INTERVAL = 0.5    

class RaftNode:
    
    def __init__(self, node_id, peers, message_sender, apply_callback): 
        self.node_id = node_id
        self.peers = list(peers) 
        self.message_sender = message_sender 
        self.apply_callback = apply_callback 
        
        self.current_term = 0
        self.voted_for = None
        self.log = [] 

        self.commit_index = 0
        self.last_applied = 0 

        self.next_index = {} 
        self.match_index = {} 

        self.state = RaftState.FOLLOWER
        self.current_leader = None
        self.votes_received = set()

        self.election_timer = None
        self.heartbeat_timer = None
        self.apply_loop_task = None 

        logger.info(f"Node {self.node_id}: Diinisialisasi sebagai FOLLOWER, Term: {self.current_term}")

    def _get_random_election_timeout(self): 
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    async def start_raft_node(self):
        logger.info(f"Node {self.node_id}: Memulai timer Raft dan apply loop.")
        self._reset_election_timer()
        if not self.apply_loop_task or self.apply_loop_task.done():
            self.apply_loop_task = asyncio.create_task(self._apply_log_entries_loop())

    async def _apply_log_entries_loop(self):
        logger.info(f"Node {self.node_id}: Memulai apply loop...")
        try:
            while True:
                apply_batch = []
                
                # Cek jika ada yang perlu di-apply
                while self.commit_index > self.last_applied:
                    apply_idx_0_based = self.last_applied
                    apply_idx_1_based = apply_idx_0_based + 1
                    
                    if apply_idx_0_based >= len(self.log):
                         logger.error(f"Node {self.node_id}: Apply loop error. commit_index={self.commit_index}, last_applied={self.last_applied}, log_len={len(self.log)}")
                         self.last_applied = self.commit_index # Coba pulihkan
                         apply_batch.clear() # Hapus batch
                         break

                    command_to_apply = self.log[apply_idx_0_based]
                    apply_batch.append((apply_idx_1_based, command_to_apply['command']))
                    self.last_applied = apply_idx_1_based # Update last_applied
                
                # Terapkan batch di luar loop 'while'
                if apply_batch:
                    logger.info(f"Node {self.node_id}: Menerapkan batch {len(apply_batch)} command (sampai index {self.last_applied})")
                    for index, command in apply_batch:
                        try:
                            # Panggil callback (misal BaseRaftNode.apply_command)
                            await self.apply_callback(index, command)
                        except Exception as e:
                            logger.error(f"Node {self.node_id}: Error saat menerapkan command di index {index}: {e}", exc_info=True)
                            # TODO: Handle error state machine

                await asyncio.sleep(0.01) # Cek setiap 10ms
        except asyncio.CancelledError:
            logger.info(f"Node {self.node_id}: Apply loop dibatalkan.")
        except Exception as e:
            logger.critical(f"Node {self.node_id}: Apply loop CRASH: {e}", exc_info=True)


    def _stop_raft_node_tasks(self):
        """Menghentikan semua background task Raft."""
        logger.info(f"Node {self.node_id}: Menghentikan semua task Raft...")
        if self.election_timer:
            self.election_timer.cancel()
            self.election_timer = None
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None
        if self.apply_loop_task:
            self.apply_loop_task.cancel()
            self.apply_loop_task = None

    # --- Manajemen Timer (DENGAN LOGGING INFO BARU) ---
    async def _run_election_timer(self):
        """Menjalankan timer pemilihan."""
        timeout = self._get_random_election_timeout()
        # [LOG INFO BARU]
        logger.info(f"Node {self.node_id}: [TIMER] Election timer DIMULAI (timeout {timeout:.2f}s).")
        await asyncio.sleep(timeout)
        
        # [LOG INFO BARU]
        logger.info(f"Node {self.node_id}: [TIMER] Election timer SELESAI.")

        if self.election_timer and not self.election_timer.cancelled():
            if self.state in [RaftState.FOLLOWER, RaftState.CANDIDATE]:
                logger.info(f"Node {self.node_id}: [TIMER] Election timeout tercapai. Memulai pemilihan.")
                asyncio.create_task(self._start_election())
            else:
                logger.info(f"Node {self.node_id}: [TIMER] Election timer selesai tetapi tidak memulai pemilihan (state: {self.state.name})")
        else:
            logger.info(f"Node {self.node_id}: [TIMER] Election timer selesai tetapi dibatalkan atau None.")

    def _reset_election_timer(self):
        """Mereset timer pemilihan."""
        if self.election_timer:
            logger.debug("Membatalkan election timer sebelumnya.")
            self.election_timer.cancel()
        try:
            loop = asyncio.get_running_loop()
            self.election_timer = loop.create_task(self._run_election_timer())
            # [LOG INFO BARU]
            logger.info(f"Node {self.node_id}: [TIMER] Election timer DIRESET (task baru dibuat).")
        except RuntimeError:
             logger.error(f"Node {self.node_id}: Gagal mereset election timer, tidak ada event loop yang berjalan.")
             self.election_timer = None
    
    async def _run_heartbeat_timer(self):
        """Menjalankan timer heartbeat (hanya untuk Leader)."""
        logger.debug(f"Node {self.node_id}: Memulai loop heartbeat.")
        try:
            while self.state == RaftState.LEADER:
                logger.debug(f"Node {self.node_id}: Mengirim heartbeat.")
                asyncio.create_task(self._send_append_entries_to_all()) # Kirim heartbeat/log
                await asyncio.sleep(HEARTBEAT_INTERVAL)
        except asyncio.CancelledError:
             logger.info(f"Node {self.node_id}: Loop heartbeat dibatalkan.")
        finally:
             logger.info(f"Node {self.node_id}: Menghentikan loop heartbeat (bukan Leader lagi).")
             self.heartbeat_timer = None


    def _start_heartbeat_timer(self):
        """Memulai timer heartbeat."""
        logger.debug(f"Node {self.node_id}: Mencoba memulai timer heartbeat.")
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        try:
            loop = asyncio.get_running_loop()
            self.heartbeat_timer = loop.create_task(self._run_heartbeat_timer())
            logger.debug(f"Node {self.node_id}: Timer heartbeat dimulai.")
        except RuntimeError:
             logger.error(f"Node {self.node_id}: Gagal memulai heartbeat timer, tidak ada event loop yang berjalan.")
             self.heartbeat_timer = None

    # --- Transisi State ---
    
    async def _become_follower(self, term, leader_id=None):
        """Transisi ke state FOLLOWER."""
        if self.state != RaftState.FOLLOWER or term > self.current_term:
             logger.info(f"Node {self.node_id}: Menjadi FOLLOWER di Term {term}. Leader saat ini: {leader_id}. State sebelumnya: {self.state.name}")

        was_leader = self.state == RaftState.LEADER
        self.state = RaftState.FOLLOWER
        
        if term > self.current_term:
             self.current_term = term
             self.voted_for = None
             logger.info(f"Node {self.node_id}: Term diupdate ke {term}.")
        
        if term >= self.current_term:
             self.current_leader = leader_id

        if was_leader and self.heartbeat_timer:
             logger.debug(f"Node {self.node_id}: Membatalkan timer heartbeat karena menjadi follower.")
             self.heartbeat_timer.cancel()
             self.heartbeat_timer = None

        self._reset_election_timer()

    async def _become_candidate(self):
        if self.state not in [RaftState.FOLLOWER, RaftState.CANDIDATE]:
             logger.warning(f"Node {self.node_id}: Transisi ilegal ke CANDIDATE dari state {self.state.name}")
             return

        next_term = self.current_term + 1
        logger.info(f"Node {self.node_id}: Menjadi CANDIDATE untuk Term {next_term}")
        self.state = RaftState.CANDIDATE
        self.current_term = next_term
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.current_leader = None
        self._reset_election_timer()


    async def _become_leader(self):
        if self.state != RaftState.CANDIDATE:
             logger.warning(f"Node {self.node_id}: Mencoba menjadi Leader dari state non-CANDIDATE ({self.state.name}). Mengabaikan.")
             return

        logger.info(f"Node {self.node_id}: ------------------ Menjadi LEADER untuk Term {self.current_term} ------------------")
        self.state = RaftState.LEADER
        self.current_leader = self.node_id

        last_log_index = len(self.log)
        self.peers = [int(p) for p in self.peers]
        self.next_index = {} 
        self.match_index = {} 
        for peer_id in self.peers:
             if peer_id != self.node_id:
                  self.next_index[peer_id] = last_log_index + 1
                  self.match_index[peer_id] = 0

        if self.election_timer:
            logger.debug(f"Node {self.node_id}: Membatalkan timer pemilihan karena menjadi leader.")
            self.election_timer.cancel()
            self.election_timer = None

        logger.debug(f"Node {self.node_id}: Mengirim heartbeat awal sebagai leader baru.")
        await self._send_append_entries_to_all(trigger=True)
        self._start_heartbeat_timer()

    async def _start_election(self):
        if self.state not in [RaftState.FOLLOWER, RaftState.CANDIDATE]:
             logger.debug(f"Node {self.node_id}: Tidak memulai pemilihan karena state adalah {self.state.name}")
             return

        await self._become_candidate()
        last_log_index = len(self.log)
        last_log_term = self.log[last_log_index - 1]['term'] if last_log_index > 0 else 0

        logger.info(f"Node {self.node_id}: Mengirim RequestVote ke peers untuk Term {self.current_term}")
        tasks = []
        payload = {
            'term': self.current_term,
            'candidate_id': self.node_id,
            'last_log_index': last_log_index,
            'last_log_term': last_log_term
        }
        self.peers = [int(p) for p in self.peers]
        for peer_id in self.peers:
             if peer_id != self.node_id:
                  tasks.append(
                       asyncio.create_task(self.message_sender(peer_id, 'request_vote', payload))
                  )
        if tasks:
             await asyncio.gather(*tasks, return_exceptions=True) 

    def _check_election_win(self):
        total_nodes = len(self.peers) + 1
        required_votes = (total_nodes // 2) + 1 
        logger.debug(f"Node {self.node_id}: Cek kemenangan pemilihan. Suara diterima: {len(self.votes_received)}, dibutuhkan: {required_votes}")
        if len(self.votes_received) >= required_votes:
            logger.info(f"Node {self.node_id}: Memenangkan pemilihan dengan {len(self.votes_received)}/{total_nodes} suara.")
            if self.state == RaftState.CANDIDATE:
                 asyncio.create_task(self._become_leader())
                 return True
            else:
                 logger.warning(f"Node {self.node_id}: Mendapat cukup suara tapi state bukan CANDIDATE ({self.state.name}). Mengabaikan.")
                 return False
        return False
        
    async def handle_rpc(self, sender_id, message_type, payload):
        """Menangani RPC yang masuk dari peer lain."""
        logger.debug(f"Node {self.node_id} (Term {self.current_term}, State {self.state.name}): Menerima RPC '{message_type}' dari {sender_id} dengan payload {payload}")

        sender_term = payload.get('term', -1)
        
        if sender_term > self.current_term:
            logger.info(f"Node {self.node_id}: Menerima term {sender_term} (lebih tinggi dari {self.current_term}) dari {sender_id}. Menjadi follower.")
            potential_leader_id = payload.get('leader_id') if message_type == 'append_entries' else None
            await self._become_follower(sender_term, leader_id=potential_leader_id)
            self.current_term = sender_term
            self.voted_for = None

        response = None
        if message_type == 'request_vote':
            response = await self._handle_request_vote(sender_id, payload)
        
        elif message_type == 'append_entries':
            response = await self._handle_append_entries(sender_id, payload)

        elif message_type == 'request_vote_response':
            if self.state == RaftState.CANDIDATE and payload.get('term') == self.current_term:
                 await self._handle_request_vote_response(sender_id, payload)
            else:
                 logger.debug(f"Node {self.node_id}: Mengabaikan respons vote dari {sender_id} (state: {self.state.name} atau term tidak cocok {payload.get('term')} vs {self.current_term})")
        
        elif message_type == 'append_entries_response':
             if self.state == RaftState.LEADER and payload.get('term') == self.current_term:
                  await self._handle_append_entries_response(sender_id, payload)
             else:
                  logger.debug(f"Node {self.node_id}: Mengabaikan respons append entries dari {sender_id} (state: {self.state.name} atau term tidak cocok {payload.get('term')} vs {self.current_term})")
        
        else:
            logger.warning(f"Node {self.node_id}: Menerima tipe RPC tidak dikenal '{message_type}'")

        if response and isinstance(response, dict):
             response['term'] = self.current_term
        return response

    async def _handle_request_vote(self, candidate_id, payload):
        candidate_term = payload['term']
        response_payload = {'term': self.current_term, 'vote_granted': False}

        if candidate_term < self.current_term:
            logger.info(f"Node {self.node_id}: Menolak vote untuk {candidate_id} (term {candidate_term} < {self.current_term})")
            return response_payload
        
        can_vote = (self.voted_for is None or self.voted_for == candidate_id)
        if can_vote:
            candidate_last_log_term = payload['last_log_term']
            candidate_last_log_index = payload['last_log_index']
            my_last_log_term = self.log[len(self.log) - 1]['term'] if len(self.log) > 0 else 0
            my_last_log_index = len(self.log)

            candidate_is_up_to_date = (
                candidate_last_log_term > my_last_log_term or
                (candidate_last_log_term == my_last_log_term and
                 candidate_last_log_index >= my_last_log_index)
            )

            if candidate_is_up_to_date:
                logger.info(f"Node {self.node_id}: Memberikan vote untuk {candidate_id} di Term {self.current_term}")
                self.voted_for = candidate_id 
                response_payload['vote_granted'] = True
                self._reset_election_timer() 
            else:
                 logger.info(f"Node {self.node_id}: Menolak vote untuk {candidate_id} (log kandidat tidak up-to-date. Milik kandidat: T{candidate_last_log_term}, I{candidate_last_log_index}. Milik kita: T{my_last_log_term}, I{my_last_log_index})")
        else:
             logger.info(f"Node {self.node_id}: Menolak vote untuk {candidate_id} (sudah vote untuk {self.voted_for} di Term {self.current_term})")

        return response_payload

    async def _handle_append_entries(self, leader_id, payload):
        leader_term = payload['term']
        response_payload = {'term': self.current_term, 'success': False}

        if leader_term < self.current_term:
             logger.info(f"Node {self.node_id}: Menolak AppendEntries dari {leader_id} (term {leader_term} < {self.current_term})")
             return response_payload

        logger.debug(f"Node {self.node_id}: Menerima AppendEntries/heartbeat dari {leader_id} (Term {leader_term}). Mereset timer pemilihan.")
        self._reset_election_timer() 

        if self.state == RaftState.CANDIDATE:
             logger.info(f"Node {self.node_id}: Menerima AppendEntries dari leader {leader_id} di Term {leader_term}. Kembali ke follower.")
             await self._become_follower(leader_term, leader_id)
        
        elif self.state == RaftState.FOLLOWER:
             if self.current_term <= leader_term:
                  self.current_leader = leader_id

        prev_log_index = payload.get('prev_log_index', 0)
        prev_log_term = payload.get('prev_log_term', 0)

        log_ok = False
        if prev_log_index == 0:
             log_ok = True 
        elif prev_log_index > 0 and prev_log_index <= len(self.log):
             if self.log[prev_log_index - 1]['term'] == prev_log_term:
                  log_ok = True

        if not log_ok:
             logger.warning(f"Node {self.node_id}: Menolak AppendEntries dari {leader_id}. Log tidak cocok di index {prev_log_index}.")
             return response_payload 

        entries = payload.get('entries', [])
        if entries:
             logger.info(f"Node {self.node_id}: Menerima {len(entries)} entri dari {leader_id} mulai dari index {prev_log_index + 1}.")
             
             current_log_idx = prev_log_index
             entry_idx_in_payload = 0
             
             while (current_log_idx < len(self.log) and entry_idx_in_payload < len(entries)):
                  if self.log[current_log_idx]['term'] != entries[entry_idx_in_payload]['term']:
                       logger.warning(f"Node {self.node_id}: Menemukan konflik di index log {current_log_idx + 1}. Menghapus log dari index ini.")
                       self.log = self.log[:current_log_idx]
                       break 
                  current_log_idx += 1
                  entry_idx_in_payload += 1
             
             if entry_idx_in_payload < len(entries):
                  self.log.extend(entries[entry_idx_in_payload:])
                  logger.info(f"Node {self.node_id}: Log diupdate. Panjang log baru: {len(self.log)}")

        leader_commit = payload.get('leader_commit', 0)
        if leader_commit > self.commit_index:
             new_commit_index = min(leader_commit, len(self.log)) 
             if new_commit_index > self.commit_index:
                  self.commit_index = new_commit_index
                  logger.info(f"Node {self.node_id}: Commit index diupdate ke {self.commit_index}")
                  # Loop _apply_log_entries_loop akan menangani apply

        response_payload['success'] = True
        return response_payload

    async def _handle_request_vote_response(self, voter_id, payload):
        if self.state != RaftState.CANDIDATE:
            logger.debug(f"Node {self.node_id}: Menerima respons vote dari {voter_id}, tapi bukan lagi candidate.")
            return
        voter_term = payload['term']
        vote_granted = payload['vote_granted']
        if voter_term > self.current_term:
            logger.info(f"Node {self.node_id}: Menerima term {voter_term} (lebih tinggi dari {self.current_term}) dari respons vote {voter_id}. Menjadi follower.")
            await self._become_follower(voter_term)
            return
        if voter_term == self.current_term and vote_granted:
            try:
                 voter_id_int = int(voter_id)
                 self.votes_received.add(voter_id_int)
                 logger.info(f"Node {self.node_id}: Menerima vote dari {voter_id_int} untuk Term {self.current_term}. Total suara: {len(self.votes_received)}")
                 self._check_election_win()
            except ValueError:
                 logger.error(f"Node {self.node_id}: Menerima voter_id tidak valid: {voter_id}")
        elif voter_term == self.current_term and not vote_granted:
             logger.info(f"Node {self.node_id}: Vote ditolak oleh {voter_id} untuk Term {self.current_term}")

    async def _handle_append_entries_response(self, follower_id_str, payload):
          if self.state != RaftState.LEADER:
               logger.debug(f"Node {self.node_id}: Menerima respons append entries dari {follower_id_str}, tapi bukan lagi leader.")
               return
          try:
               follower_id = int(follower_id_str)
          except ValueError:
               logger.error(f"Node {self.node_id}: Menerima follower_id tidak valid: {follower_id_str}")
               return
          follower_term = payload['term']
          success = payload['success']
          if follower_term > self.current_term:
               logger.info(f"Node {self.node_id}: Menerima term {follower_term} (lebih tinggi dari {self.current_term}) dari respons append entries {follower_id}. Menjadi follower.")
               await self._become_follower(follower_term)
               return
          if follower_term == self.current_term:
               next_idx = self.next_index.get(follower_id, len(self.log) + 1)
               if success:
                    # Asumsi kita mengirim semua entry baru, jadi follower
                    # sekarang match sampai len(self.log)
                    new_match_index = len(self.log) 
                    self.match_index[follower_id] = new_match_index
                    self.next_index[follower_id] = new_match_index + 1
                    logger.debug(f"Node {self.node_id}: AppendEntries ke {follower_id} berhasil. match_index[{follower_id}]={self.match_index[follower_id]}, next_index[{follower_id}]={self.next_index[follower_id]}")
                    await self._update_leader_commit_index()
               elif not success:
                    self.next_index[follower_id] = max(1, next_idx - 1)
                    logger.warning(f"Node {self.node_id}: AppendEntries ke {follower_id} gagal (inkonsistensi log). Decrement next_index[{follower_id}] ke {self.next_index[follower_id]}.")
    
    async def submit_command(self, command):
        """Dipanggil oleh BaseRaftNode untuk submit command baru."""
        if self.state != RaftState.LEADER:
            logger.warning(f"Node {self.node_id}: Menerima submit command, tapi bukan leader.")
            return None, {
                'success': False, 
                'leader_id': self.current_leader,
                'leader_url': None 
            }
        
        logger.info(f"Node {self.node_id} (Leader): Menerima command: {command}")
        
        new_entry_index = len(self.log) + 1 # Index 1-based
        
        # --- [ PERUBAHAN UTAMA DI SINI ] ---
        # Tambahkan timestamp (log index) ke command untuk Deadlock Detection
        command_with_index = command.copy()
        command_with_index['request_index'] = new_entry_index
        # ---------------------------------
        
        new_entry = {
            'term': self.current_term,
            'command': command_with_index # Simpan command yang sudah dimodifikasi
        }
        self.log.append(new_entry)
        
        logger.info(f"Node {self.node_id}: Menambahkan command ke log di index {new_entry_index}: {command_with_index}")

        # Panggil _send_append_entries_to_all secara non-blocking
        asyncio.create_task(self._send_append_entries_to_all(trigger=True))
        
        logger.info(f"Node {self.node_id}: selesai submit_command index={new_entry_index}") 
        return new_entry_index, None

    async def _send_append_entries_to_all(self, trigger=False): # Tambahkan 'trigger'
        if self.state != RaftState.LEADER:
            return 
        
        if trigger:
             logger.info(f"Node {self.node_id}: Terpicu untuk mengirim AppendEntries (karena command baru)")
        else:
             logger.debug(f"Node {self.node_id}: Mengirim AppendEntries/heartbeat periodik")

        tasks = []
        self.peers = [int(p) for p in self.peers]
        
        for peer_id in self.peers:
            if peer_id != self.node_id:
                 tasks.append(
                      asyncio.create_task(self._send_append_entries_to_peer(peer_id))
                 )
        
        if tasks:
             await asyncio.gather(*tasks, return_exceptions=True) 

    async def _send_append_entries_to_peer(self, peer_id):
        if self.state != RaftState.LEADER:
            return 
        
        if peer_id not in self.next_index:
             self.next_index[peer_id] = len(self.log) + 1
             self.match_index[peer_id] = 0

        next_idx = self.next_index[peer_id] # Ini 1-based index
        prev_log_index = next_idx - 1 
        prev_log_term = self.log[prev_log_index - 1]['term'] if prev_log_index > 0 else 0

        entries_to_send = []
        if len(self.log) >= next_idx:
            entries_to_send = self.log[next_idx - 1:] 
            logger.debug(f"Node {self.node_id}: Mengirim {len(entries_to_send)} entry ke {peer_id} (mulai dari index {next_idx})")

        payload = {
            'term': self.current_term,
            'leader_id': self.node_id,
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'entries': entries_to_send,
            'leader_commit': self.commit_index
        }
        await self.message_sender(peer_id, 'append_entries', payload)

    async def _update_leader_commit_index(self):
         total_nodes = len(self.peers) + 1
         required_majority = (total_nodes // 2) + 1
         
         for N in range(len(self.log), self.commit_index, -1): # (len(log), ..., commitIndex+1)
              if self.log[N - 1]['term'] == self.current_term:
                   match_count = 1 
                   for follower_id in self.peers:
                        if follower_id != self.node_id:
                             if self.match_index.get(follower_id, 0) >= N:
                                  match_count += 1
                   
                   if match_count >= required_majority:
                        if N > self.commit_index: 
                             logger.info(f"Node {self.node_id} (Leader): Commit index diupdate dari {self.commit_index} ke {N} (mayoritas: {match_count}/{total_nodes})")
                             self.commit_index = N
                        break