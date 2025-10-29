import requests
import time
import sys
import logging
import json
import threading
import random
import uuid

from locust import HttpUser, task, between, events

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("LOCUST_CLIENT")

LOCK_NODE_URLS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003"
]
QUEUE_NODE_BASE_URLS = [
    "http://localhost:9001",
    "http://localhost:9002",
    "http://localhost:9003"
]
CACHE_NODE_BASE_URLS = [
    "http://localhost:7001",
    "http://localhost:7002",
    "http://localhost:7003"
]

LOCK_RESOURCES = [f"lock_res_{i}" for i in range(5)] 
QUEUE_KEYS = [f"queue_key_{i}" for i in range(10)] 
CACHE_KEYS = [f"cache_key_{i}" for i in range(20)] 

class LockUser(HttpUser):
    wait_time = between(0.5, 2.0) 
    host = LOCK_NODE_URLS[0] 
    
    def find_leader(self):
        logger.info(f"{self.client_id} mencoba menemukan leader Raft...")
        shuffled_nodes = random.sample(LOCK_NODE_URLS, len(LOCK_NODE_URLS))
        
        for node_url in shuffled_nodes:
            try:
                response = self.client.get(f"{node_url}/", timeout=1.0)
                if response.status_code == 200:
                    status = response.json()
                    if status.get('raft_state') == 'LEADER':
                        logger.info(f"{self.client_id} menemukan LEADER di: {node_url}")
                        self.leader_host = node_url # Simpan leader yang ditemukan
                        return
                else:
                    logger.warning(f"Gagal cek status {node_url}: HTTP {response.status_code}")
            except requests.RequestException as e:
                logger.error(f"Gagal terhubung ke {node_url} saat mencari leader: {e}")
                
        logger.error(f"{self.client_id} GAGAL menemukan leader Raft setelah memeriksa semua node.")
        self.leader_host = self.host 

    def on_start(self):
        self.client_id = f"locust_lock_user_{uuid.uuid4()}"
        self.leader_host = None # Awalnya tidak tahu leader
        self.find_leader() # Cari leader saat start
        if not self.leader_host:
            # Jika gagal total, hentikan user ini
            self.stop() 

    @task(3) 
    def acquire_exclusive_and_release(self):
        if not self.leader_host:
            self.find_leader()
            if not self.leader_host: return

        resource = random.choice(LOCK_RESOURCES)
        acquire_payload = {
            "client_id": self.client_id,
            "lock_name": resource,
            "type": "exclusive"
        }
        
        release_payload = {
            "client_id": self.client_id,
            "lock_name": resource
        }

        with self.client.post(f"{self.leader_host}/acquire_lock", json=acquire_payload, catch_response=True, name="/acquire_lock") as response:
            try:
                if response.status_code == 200:
                    resp_json = response.json()
                    if resp_json.get("success"):
                        response.success()
                        time.sleep(random.uniform(0.1, 0.5))
                        self.client.post(f"{self.leader_host}/release_lock", json=release_payload, name="/release_lock")
                    else:
                        response.failure(f"Acquire failed or queued: {resp_json.get('message')}")

                elif response.status_code == 400:
                    resp_json = response.json()
                    if "Not a leader" in resp_json.get("message",""):
                        response.failure(f"Gagal: Target {self.leader_host} bukan lagi leader. Mencari leader baru...")
                        self.find_leader() # Temukan leader baru untuk task berikutnya
                    else:
                        response.failure(f"Bad Request: {response.text}")
                else:
                    response.failure(f"Unexpected status code: {response.status_code}")
            except json.JSONDecodeError:
                response.failure(f"Non-JSON response: {response.text}")
            except Exception as e:
                response.failure(f"Error saat memproses response: {e}")

    @task(1) 
    def acquire_shared_and_release(self):
        if not self.leader_host:
            self.find_leader()
            if not self.leader_host: return

        resource = random.choice(LOCK_RESOURCES)
        acquire_payload = {
            "client_id": self.client_id,
            "lock_name": resource,
            "type": "shared"
        }
        
        release_payload = {
            "client_id": self.client_id,
            "lock_name": resource
        }

        with self.client.post(f"{self.leader_host}/acquire_lock", json=acquire_payload, catch_response=True, name="/acquire_lock") as response:
            try:
                if response.status_code == 200:
                    resp_json = response.json()
                    if resp_json.get("success"):
                        response.success()
                        time.sleep(random.uniform(0.1, 0.5))
                        self.client.post(f"{self.leader_host}/release_lock", json=release_payload, name="/release_lock")
                    else:
                        response.failure(f"Acquire shared failed: {resp_json.get('message')}")
                elif response.status_code == 400:
                    resp_json = response.json()
                    if "Not a leader" in resp_json.get("message",""):
                        response.failure(f"Gagal: Target {self.leader_host} bukan lagi leader. Mencari leader baru...")
                        self.find_leader()
                    else:
                        response.failure(f"Bad Request: {response.text}")
                else:
                    response.failure(f"Unexpected status code: {response.status_code}")
            except json.JSONDecodeError:
                response.failure(f"Non-JSON response: {response.text}")
            except Exception as e:
                response.failure(f"Error saat memproses response: {e}")


class QueueUser(HttpUser):
    wait_time = between(0.1, 0.5)
    host = QUEUE_NODE_BASE_URLS[0] # Host dummy
    
    def on_start(self):
        self.client_id = f"locust_queue_user_{uuid.uuid4()}"
        self.last_consumed_message = {} 

    @task(5) 
    def publish(self):
        key = random.choice(QUEUE_KEYS)
        message = f"Pesan dari {self.client_id} untuk {key} @ {time.time()}"
        payload = {'key': key, 'message': message}
        target_node = random.choice(QUEUE_NODE_BASE_URLS) 
        self.client.post(f"{target_node}/publish", json=payload, name="/publish [node random]")

    @task(2) 
    def consume_and_ack(self):
        key = random.choice(QUEUE_KEYS)
        payload_consume = {'key': key, 'consumer_id': self.client_id}
        target_node = random.choice(QUEUE_NODE_BASE_URLS) 

        if key in self.last_consumed_message:
            old_msg = self.last_consumed_message.pop(key)
            payload_ack = {'key': key, 'consumer_id': self.client_id, 'message': old_msg}
            ack_node = random.choice(QUEUE_NODE_BASE_URLS)
            self.client.post(f"{ack_node}/ack", json=payload_ack, name="/ack [node random]")
            time.sleep(0.1)

        with self.client.post(f"{target_node}/consume", json=payload_consume, catch_response=True, name="/consume [node random]") as response:
            if response.status_code == 200:
                try:
                    resp_json = response.json()
                    if resp_json.get("success"):
                        message = resp_json.get("message")
                        self.last_consumed_message[key] = message
                        time.sleep(random.uniform(0.05, 0.2))
                        if random.random() < 0.95: 
                            payload_ack = {'key': key, 'consumer_id': self.client_id, 'message': message}
                            ack_node = random.choice(QUEUE_NODE_BASE_URLS)
                            self.client.post(f"{ack_node}/ack", json=payload_ack, name="/ack [node random]")
                            if key in self.last_consumed_message and self.last_consumed_message[key] == message:
                                 del self.last_consumed_message[key] 
                        else:
                            logging.info(f"{self.client_id} TIDAK ACK pesan untuk {key}")
                    elif resp_json.get("message") == "Queue empty":
                        response.success() 
                    else:
                        response.failure(f"Consume returned success=false: {resp_json.get('message')}")
                except json.JSONDecodeError:
                    response.failure("Failed to decode JSON response from consume")
            else:
                response.failure(f"Consume failed with status {response.status_code}")

class CacheUser(HttpUser):
    wait_time = between(0.2, 1.0)
    host = CACHE_NODE_BASE_URLS[0] # Host dummy

    def on_start(self):
        self.client_id = f"locust_cache_user_{uuid.uuid4()}"

    @task(8) 
    def read_cache(self):
        key = random.choice(CACHE_KEYS)
        target_node = random.choice(CACHE_NODE_BASE_URLS)
        self.client.get(f"{target_node}/read?key={key}", name="/read?key=[key]") 

    @task(2) 
    def write_cache(self):
        key = random.choice(CACHE_KEYS)
        value = f"value_from_{self.client_id}_at_{time.time()}"
        payload = {'key': key, 'value': value}
        target_node = random.choice(CACHE_NODE_BASE_URLS)
        self.client.post(f"{target_node}/write", json=payload, name="/write")

#versi 1 node dari masing masing 3 jenis
# import requests
# import time
# import sys
# import logging
# import json
# import threading
# import random
# import uuid

# from locust import HttpUser, task, between, events

# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger("LOCUST_CLIENT")

# # --- Konfigurasi (DIUBAH UNTUK SINGLE-NODE) ---
# # Tentukan URL untuk HANYA node yang berjalan
# LOCK_NODE_URLS = [
#     "http://localhost:8001"
# ]
# QUEUE_NODE_BASE_URLS = [
#     "http://localhost:9001"
# ]
# CACHE_NODE_BASE_URLS = [
#     "http://localhost:7001"
# ]
# # -----------------------------------------------

# LOCK_RESOURCES = [f"lock_res_{i}" for i in range(5)] 
# QUEUE_KEYS = [f"queue_key_{i}" for i in range(10)] 
# CACHE_KEYS = [f"cache_key_{i}" for i in range(20)] 


# class LockUser(HttpUser):
#     wait_time = between(0.5, 2.0) 
#     host = LOCK_NODE_URLS[0] # Targetkan node lock tunggal

#     def on_start(self):
#         self.client_id = f"locust_lock_user_{uuid.uuid4()}"
#         self.leader_host = self.host # Asumsikan 8001 adalah target, meskipun akan gagal (ini WAJAR)

#     @task
#     def acquire_exclusive_and_release(self):
#         resource = random.choice(LOCK_RESOURCES)
#         acquire_payload = {
#             "client_id": self.client_id,
#             "lock_name": resource,
#             "type": "exclusive"
#         }

#         # Request ini HARUS GAGAL (karena 1 node bukan mayoritas)
#         # Kita tandai sebagai "sukses" di Locust agar tidak mencemari statistik
#         with self.client.post(f"{self.leader_host}/acquire_lock", json=acquire_payload, catch_response=True, name="/acquire_lock") as response:
#             try:
#                 if response.status_code == 400:
#                     resp_json = response.json()
#                     if "Not a leader" in resp_json.get("message",""):
#                         response.success() # INI DIHARAPKAN, jadi tandai sukses
#                     else:
#                         response.failure(f"Bad Request: {response.text}")
#                 elif response.status_code == 200 and not response.json().get("success"):
#                      response.failure(f"Acquire failed: {response.json().get('message')}")
#                 elif response.status_code != 200:
#                     response.failure(f"Unexpected status code: {response.status_code}")
#             except json.JSONDecodeError:
#                 response.failure(f"Non-JSON response: {response.text}")

# class QueueUser(HttpUser):
#     wait_time = between(0.1, 0.5)
#     host = QUEUE_NODE_BASE_URLS[0] # Host dummy

#     def on_start(self):
#         self.client_id = f"locust_queue_user_{uuid.uuid4()}"
#         self.last_consumed_message = {} 

#     @task(5) 
#     def publish(self):
#         key = random.choice(QUEUE_KEYS)
#         message = f"Pesan dari {self.client_id} untuk {key} @ {time.time()}"
#         payload = {'key': key, 'message': message}
#         # [PERBAIKAN] Hanya akan memilih 9001
#         target_node = random.choice(QUEUE_NODE_BASE_URLS) 
#         self.client.post(f"{target_node}/publish", json=payload, name="/publish")

#     @task(2) 
#     def consume_and_ack(self):
#         key = random.choice(QUEUE_KEYS)
#         payload_consume = {'key': key, 'consumer_id': self.client_id}
#         target_node = random.choice(QUEUE_NODE_BASE_URLS) 

#         if key in self.last_consumed_message:
#             old_msg = self.last_consumed_message.pop(key)
#             payload_ack = {'key': key, 'consumer_id': self.client_id, 'message': old_msg}
#             ack_node = random.choice(QUEUE_NODE_BASE_URLS)
#             self.client.post(f"{ack_node}/ack", json=payload_ack, name="/ack")
#             time.sleep(0.1)

#         with self.client.post(f"{target_node}/consume", json=payload_consume, catch_response=True, name="/consume") as response:
#             if response.status_code == 200:
#                 try:
#                     resp_json = response.json()
#                     if resp_json.get("success"):
#                         message = resp_json.get("message")
#                         self.last_consumed_message[key] = message
#                         time.sleep(random.uniform(0.05, 0.2))
#                         if random.random() < 0.95: 
#                             payload_ack = {'key': key, 'consumer_id': self.client_id, 'message': message}
#                             ack_node = random.choice(QUEUE_NODE_BASE_URLS)
#                             self.client.post(f"{ack_node}/ack", json=payload_ack, name="/ack")
#                             if key in self.last_consumed_message and self.last_consumed_message[key] == message:
#                                  del self.last_consumed_message[key] 
#                         else:
#                             logging.info(f"{self.client_id} TIDAK ACK pesan untuk {key}")
#                     elif resp_json.get("message") == "Queue empty":
#                         response.success() 
#                     else:
#                         response.failure(f"Consume returned success=false: {resp_json.get('message')}")
#                 except json.JSONDecodeError:
#                     response.failure("Failed to decode JSON response from consume")
#             else:
#                 response.failure(f"Consume failed with status {response.status_code}")

# class CacheUser(HttpUser):
#     wait_time = between(0.2, 1.0)
#     host = CACHE_NODE_BASE_URLS[0] # Host dummy

#     def on_start(self):
#         self.client_id = f"locust_cache_user_{uuid.uuid4()}"

#     @task(8) 
#     def read_cache(self):
#         key = random.choice(CACHE_KEYS)
#         # [PERBAIKAN] Hanya akan memilih 7001
#         target_node = random.choice(CACHE_NODE_BASE_URLS)
#         self.client.get(f"{target_node}/read?key={key}", name="/read?key=[key]") 

#     @task(2) 
#     def write_cache(self):
#         key = random.choice(CACHE_KEYS)
#         value = f"value_from_{self.client_id}_at_{time.time()}"
#         payload = {'key': key, 'value': value}
#         target_node = random.choice(CACHE_NODE_BASE_URLS)
#         self.client.post(f"{target_node}/write", json=payload, name="/write")