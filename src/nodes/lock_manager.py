import logging

logger = logging.getLogger(__name__)

class LockManager:
    def __init__(self):
        self.locks = {}
        logger.info("LockManager diinisialisasi (dengan Shared Locks & Wound-Wait).")

    def _grant_lock(self, lock_name, lock, request):
        lock_type = request['type']
        client_id = request['client_id']
        request_index = request['request_index']

        lock['type'] = lock_type
        if lock_type == 'exclusive':
            lock['holders'] = {client_id: request_index}
            logger.info(f"[LockGrant] Lock '{lock_name}' (Exclusive) diberikan ke {client_id} @ index {request_index}")
        else: # 'shared'
            lock['holders'][client_id] = request_index
            logger.info(f"[LockGrant] Lock '{lock_name}' (Shared) diberikan ke {client_id} @ index {request_index}. Total pemegang: {len(lock['holders'])}")
        return {"success": True, "message": f"Lock '{lock_name}' ({lock_type}) acquired by {client_id}"}

    def _release_lock(self, lock_name, client_id):
        if lock_name not in self.locks:
            return False, "Lock not found"
            
        lock = self.locks[lock_name]
        
        if client_id not in lock['holders']:
            logger.warning(f"LockManager: {client_id} mencoba melepas lock '{lock_name}' yang tidak dipegangnya. Pemegang: {lock['holders']}")
            return False, "You do not hold the lock"

        holder_index = lock['holders'].pop(client_id)
        logger.info(f"LockManager: {client_id} (memegang @ {holder_index}) melepas lock '{lock_name}'.")

        if not lock['holders']:
            lock['type'] = None
            self._process_queue(lock_name)
        
        return True, "Lock released"

    def _process_queue(self, lock_name):
        if lock_name not in self.locks:
            return
            
        lock = self.locks[lock_name]
        if lock['type'] is not None: 
            return

        if not lock['queue']: 
            logger.debug(f"Lock '{lock_name}' bebas dan antrian kosong.")
            return

        request_to_grant = lock['queue'].pop(0)
        
        self._grant_lock(lock_name, lock, request_to_grant)
        
        if lock['type'] == 'shared':
            while lock['queue'] and lock['queue'][0]['type'] == 'shared':
                next_shared_request = lock['queue'].pop(0)
                self._grant_lock(lock_name, lock, next_shared_request)

    def handle_command(self, command):
        op = command.get('op')
        lock_name = command.get('lock_name')
        client_id = command.get('client_id')
        request_index = command.get('request_index') 
        
        logger.info(f"LockManager: Menerapkan command: {command}")
        
        if op == 'acquire':
            lock_type = command.get('type', 'exclusive')
            new_request = {
                'client_id': client_id, 
                'type': lock_type, 
                'request_index': request_index
                # lock_name tidak perlu di sini
            }
            
            if lock_name not in self.locks:
                self.locks[lock_name] = {"type": None, "holders": {}, "queue": []}
            
            lock = self.locks[lock_name]

            if not lock['holders']:
                return self._grant_lock(lock_name, lock, new_request)

            if lock['type'] == 'shared' and new_request['type'] == 'shared':
                return self._grant_lock(lock_name, lock, new_request)
            
            wounded_clients = {} 
            current_holders = list(lock['holders'].items()) 
            for holder_id, holder_index in current_holders:
                if request_index < holder_index: 
                    logger.warning(f"Deadlock Prevention: Request {client_id} (Index {request_index}) melukai pemegang {holder_id} (Index {holder_index}) pada lock '{lock_name}'")
                    wounded_clients[holder_id] = holder_index
                    lock['holders'].pop(holder_id)
                
            if not lock['holders']:
                lock['type'] = None
                
            if wounded_clients:
                wounded_list = sorted(wounded_clients.items(), key=lambda item: item[1]) 
                original_lock_type = 'exclusive' if len(current_holders) == 1 and lock['type'] == 'exclusive' else 'shared'
                
                for i, (wounded_id, wounded_index) in enumerate(wounded_list):
                    lock['queue'].insert(i, {
                        "client_id": wounded_id, 
                        "type": original_lock_type, # Gunakan tipe asli saat dipegang
                        "request_index": wounded_index
                    })
                logger.info(f"{len(wounded_clients)} pemegang terluka dimasukkan kembali ke antrian '{lock_name}'")


            if not lock['holders']:
                return self._grant_lock(lock_name, lock, new_request)
            else:
                remaining_holder_indices = list(lock['holders'].values())
                if remaining_holder_indices and request_index > min(remaining_holder_indices):
                     logger.info(f"LockManager: {client_id} (Index {request_index}) menunggu lock '{lock_name}' yang dipegang oleh pemegang yg lebih tua (misal, index {min(remaining_holder_indices)})")
                     lock['queue'].append(new_request)
                     lock['queue'].sort(key=lambda x: x['request_index'])
                     return {"success": False, "message": f"Lock '{lock_name}' is busy, request queued."}
                else:
                     logger.warning(f"LockManager: Kondisi tak terduga saat acquire '{lock_name}' oleh {client_id}. Lock dipegang: {lock['holders']}. Memasukkan ke antrian.")
                     lock['queue'].append(new_request)
                     lock['queue'].sort(key=lambda x: x['request_index'])
                     return {"success": False, "message": f"Lock '{lock_name}' is busy, request queued (unexpected)."}

                
        elif op == 'release':
            released, message = self._release_lock(lock_name, client_id)
            return {"success": released, "message": message}
            
        else:
            logger.error(f"LockManager: Command tidak dikenal: {op}")
            return {"success": False, "message": f"Unknown command: {op}"}