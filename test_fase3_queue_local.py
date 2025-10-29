import requests
import time
import sys
import logging
import json
import random
import subprocess 
import signal
import os
import platform

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("QUEUE_CLIENT_LOCAL")

QUEUE_NODE_PORTS = [9001, 9002, 9003]
QUEUE_NODE_URLS = [f"http://localhost:{p}" for p in QUEUE_NODE_PORTS]
NODE_PROCESSES = [] 

def get_random_node_url():
    return random.choice(QUEUE_NODE_URLS)

# --- Fungsi publish, consume, acknowledge (Tetap Sama) ---
def publish_message(key, message):
    node_url = get_random_node_url()
    endpoint = f"{node_url}/publish"
    payload = {'key': key, 'message': message}
    try:
        response = requests.post(endpoint, json=payload, timeout=7)
        logger.info(f"Publish '{key}':'{message[:10]}...' via {node_url}: Status={response.status_code}, Resp={response.json()}")
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Gagal publish via {node_url}: {e}")
        return None

def consume_message(key, consumer_id):
    node_url = get_random_node_url()
    endpoint = f"{node_url}/consume"
    payload = {'key': key, 'consumer_id': consumer_id}
    try:
        response = requests.post(endpoint, json=payload, timeout=7) 
        logger.info(f"Consume '{key}' by '{consumer_id}' via {node_url}: Status={response.status_code}, Resp={response.json()}")
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Gagal consume via {node_url}: {e}")
        return None

def acknowledge_message(key, consumer_id, message):
    node_url = get_random_node_url()
    endpoint = f"{node_url}/ack"
    payload = {'key': key, 'consumer_id': consumer_id, 'message': message}
    try:
        response = requests.post(endpoint, json=payload, timeout=7)
        logger.info(f"ACK '{key}' by '{consumer_id}' for '{message[:10]}...' via {node_url}: Status={response.status_code}, Resp={response.json()}")
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Gagal ACK via {node_url}: {e}")
        return None
# --------------------------------------------------------

def start_local_nodes():
    global NODE_PROCESSES
    logger.info("Memulai 3 QueueNode lokal...")
    python_executable = sys.executable 
    
    # Buat file .env
    for i, port in enumerate(QUEUE_NODE_PORTS):
        env_file = f"queue_node{i+1}.env"
        peers_list = [f"http://localhost:{p}" for p in QUEUE_NODE_PORTS if p != port]
        peers_str = ",".join(peers_list)
        with open(env_file, "w") as f:
            f.write(f"NODE_ID=qnode-{i+1}\nNODE_PORT={port}\nNODE_HOST=localhost\nPEER_ADDRESSES={peers_str}\nREDIS_HOST=localhost\nREDIS_PORT=6379\n")
            
        cmd = [ python_executable, "-m", "src.nodes.main", "--env-file", env_file, "--node-type", "queue" ]
        log_file = open(f"node_queue_{port}.log", "w") 
        process = subprocess.Popen(cmd, stdout=log_file, stderr=log_file) 
        NODE_PROCESSES.append(process)
        logger.info(f"Memulai node di port {port} (PID: {process.pid}), log di node_queue_{port}.log")
        
    logger.info("Menunggu 5 detik agar semua node siap...")
    time.sleep(5)

# --- [ PERBAIKAN STOP NODE DI SINI ] ---
def stop_local_nodes():
    """Menghentikan semua proses node lokal."""
    logger.info("Menghentikan node lokal...")
    for process in NODE_PROCESSES:
        try:
            # --- [ PERBAIKAN STOP NODE WINDOWS ] ---
            if platform.system() == "Windows":
                # Coba terminate() dulu di Windows
                process.terminate()
            else:
                # Coba SIGINT di sistem lain
                process.send_signal(signal.SIGINT) 
            # ------------------------------------
            process.wait(timeout=5) 
            logger.info(f"Node (PID: {process.pid}) dihentikan.")
        except subprocess.TimeoutExpired:
            logger.warning(f"Node (PID: {process.pid}) tidak berhenti setelah 5s, paksa kill.")
            process.kill() # Paksa jika cara halus gagal
        except Exception as e:
            logger.error(f"Error menghentikan node (PID: {process.pid}): {e}")
            # Coba kill sebagai fallback jika ada error lain
            try:
                 process.kill()
            except Exception:
                 pass # Abaikan jika kill juga gagal
                 
    NODE_PROCESSES.clear()
    for i in range(len(QUEUE_NODE_PORTS)):
         try: os.remove(f"queue_node{i+1}.env")
         except OSError: pass
# ------------------------------------

def run_queue_tests():
    logger.info("--- MEMULAI TES ANTRIAN ---")
    topic1 = "news"
    topic2 = "alerts"
    
    logger.info("\n1. Mempublikasikan pesan ke 'news'...")
    publish_message(topic1, "Berita 1: Cuaca Cerah")
    publish_message(topic1, "Berita 2: Harga Saham Naik")
    publish_message(topic1, "Berita 3: Festival Lokal")
    
    logger.info("\n2. Mempublikasikan pesan ke 'alerts'...")
    publish_message(topic2, "ALERT: Server CPU Tinggi!")
    publish_message(topic2, "ALERT: Disk Hampir Penuh!")
    
    time.sleep(1)
    
    logger.info("\n3. Consumer 'reader1' membaca dari 'news'...")
    # --- [ PERBAIKAN LOGIKA TES DI SINI: Consume 3 kali ] ---
    for i in range(3): 
        consumed = consume_message(topic1, "reader1")
        if consumed and consumed.get('success'):
            msg = consumed.get('message')
            logger.info(f"   reader1 mendapat (pesan {i+1}): {msg}")
            acknowledge_message(topic1, "reader1", msg)
        else:
            logger.error(f"   Gagal consume ke-{i+1} dari news. Respons: {consumed}")
            break # Hentikan jika gagal
    # --------------------------------------------------------
            
    logger.info("\n4. Consumer 'monitor1' membaca dari 'alerts'...")
    consumed_alert1 = consume_message(topic2, "monitor1")
    if consumed_alert1 and consumed_alert1.get('success'):
        msg_alert1 = consumed_alert1.get('message')
        logger.info(f"   monitor1 mendapat: {msg_alert1}")
        logger.warning("   monitor1 TIDAK mengirim ACK (simulasi crash)")
    else: 
        logger.error(f"   Gagal consume pertama dari alerts. Respons: {consumed_alert1}")
        
    logger.info("\n5. Consumer 'monitor1' membaca lagi dari 'alerts' (harus re-delivery)...")
    consumed_alert2 = consume_message(topic2, "monitor1")
    if consumed_alert2 and consumed_alert2.get('success') and consumed_alert2.get('status') == 're-delivered':
        msg_alert2 = consumed_alert2.get('message')
        logger.info(f"   monitor1 mendapat (re-delivery): {msg_alert2}")
        acknowledge_message(topic2, "monitor1", msg_alert2)
    elif consumed_alert2 and consumed_alert2.get('success'): 
        logger.error(f"   monitor1 mendapat pesan baru, bukan re-delivery: {consumed_alert2.get('message')}")
    else: 
        logger.error(f"   Gagal consume kedua dari alerts (setelah simulasi crash). Respons: {consumed_alert2}")
        
    logger.info("\n6. Consumer 'monitor1' membaca lagi (harus dapat pesan alert kedua)...")
    consumed_alert3 = consume_message(topic2, "monitor1")
    if consumed_alert3 and consumed_alert3.get('success'):
        msg_alert3 = consumed_alert3.get('message')
        logger.info(f"   monitor1 mendapat: {msg_alert3}")
        acknowledge_message(topic2, "monitor1", msg_alert3)
    else: 
        logger.error(f"   Gagal consume ketiga dari alerts. Respons: {consumed_alert3}")
        
    logger.info("\n7. Mencoba consume dari 'news' yang sudah kosong...")
    consumed_empty = consume_message(topic1, "reader1")
    if consumed_empty and not consumed_empty.get('success') and consumed_empty.get('message') == 'Queue empty':
        logger.info("   Berhasil: Antrian kosong seperti yang diharapkan.")
    else: 
        logger.error(f"   Gagal: Seharusnya antrian kosong, tapi mendapat: {consumed_empty}")
        
    logger.info("\n--- TES ANTRIAN SELESAI ---")

if __name__ == "__main__":
    logger.info("Pastikan Redis server berjalan di localhost:6379.")
    try:
        start_local_nodes()
        run_queue_tests()
    except Exception as e:
        logger.critical(f"Terjadi error saat tes: {e}", exc_info=True)
    finally:
        stop_local_nodes()