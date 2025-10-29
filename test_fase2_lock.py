import requests
import time
import sys
import logging
import json
import threading

# Setup logging sederhana
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CLIENT")

# Daftar node, kita akan coba cari leader
NODE_URLS = [
    "http://localhost:8001",
    "http://localhost:8002",
    "http://localhost:8003",
]
LEADER_URL = None

def find_leader(retry=5):
    """Mencoba menemukan leader Raft dengan query status."""
    global LEADER_URL
    for _ in range(retry):
        for url in NODE_URLS:
            try:
                response = requests.get(f"{url}/", timeout=1)
                if response.status_code == 200:
                    status = response.json()
                    if status.get('state') == 'LEADER':
                        LEADER_URL = url
                        logger.info(f"Menemukan Leader di: {LEADER_URL}")
                        return True
            except requests.RequestException:
                logger.warning(f"Node di {url} tidak merespons saat mencari leader.")
        logger.warning("Gagal menemukan leader, mencoba lagi dalam 1 detik...")
        time.sleep(1)
    
    logger.error("Tidak dapat menemukan leader di antara node yang diketahui.")
    LEADER_URL = None
    return False

def send_lock_request(endpoint, payload):
    """Mengirim request ke endpoint leader yang diketahui."""
    global LEADER_URL
    if not LEADER_URL:
        if not find_leader():
            return None

    url = f"{LEADER_URL}/{endpoint}"
    try:
        # Tambah timeout lebih lama karena wound-wait mungkin butuh replikasi
        response = requests.post(url, json=payload, timeout=15) 
        if response.status_code == 400: # Kemungkinan 'Not a leader'
             logger.warning("Leader mungkin telah berubah, mencari lagi...")
             LEADER_URL = None # Hapus cache leader
             if find_leader(): # Cari lagi
                  url = f"{LEADER_URL}/{endpoint}" # Coba lagi dengan leader baru
                  response = requests.post(url, json=payload, timeout=15)
             else:
                  return None
        
        return response.json()
        
    except requests.RequestException as e:
        logger.error(f"Gagal mengirim request ke {url}: {e}")
        LEADER_URL = None # Leader mungkin telah berubah, reset
        return None

def get_leader_status():
    """Mengambil status / dari leader."""
    if not LEADER_URL:
        if not find_leader():
            return {"error": "No leader found"}
    try:
        res_status = requests.get(f"{LEADER_URL}/", timeout=1).json()
        return res_status
    except Exception as e:
        return {"error": f"Gagal mendapatkan status: {e}"}

def run_all_tests():
    # Beri waktu 3-5 detik agar node-node bisa memilih leader
    logger.info("Menunggu 5 detik agar cluster Raft memilih leader...")
    time.sleep(5)
    
    if not find_leader():
        logger.critical("Tidak bisa memulai tes, tidak ada leader ditemukan.")
        sys.exit(1)
        
    # --- Tes 1: Shared Locks ---
    logger.info("\n--- TES SHARED LOCK ---")
    
    # 1. 'alice' mendapatkan shared lock
    logger.info("1. 'alice' meminta SHARED lock 'shared-doc'")
    res_a1 = send_lock_request('acquire_lock', {'client_id': 'alice', 'lock_name': 'shared-doc', 'type': 'shared'})
    logger.info(f"Respons 'alice': {res_a1}")
    if not res_a1 or not res_a1.get('success'):
        logger.error("TES SHARED GAGAL: alice gagal mendapatkan shared lock")
        return

    # 2. 'bob' mendapatkan shared lock yang sama
    logger.info("\n2. 'bob' meminta SHARED lock 'shared-doc' (seharusnya berhasil)")
    res_b1 = send_lock_request('acquire_lock', {'client_id': 'bob', 'lock_name': 'shared-doc', 'type': 'shared'})
    logger.info(f"Respons 'bob': {res_b1}")
    if not res_b1 or not res_b1.get('success'):
        logger.error("TES SHARED GAGAL: bob gagal mendapatkan shared lock kedua")
        return

    # 3. 'charlie' meminta exclusive lock (seharusnya masuk antrian)
    logger.info("\n3. 'charlie' meminta EXCLUSIVE lock 'shared-doc' (seharusnya masuk antrian)")
    res_c1 = send_lock_request('acquire_lock', {'client_id': 'charlie', 'lock_name': 'shared-doc', 'type': 'exclusive'})
    logger.info(f"Respons 'charlie': {res_c1}")
    if res_c1 and res_c1.get('success'):
        logger.error("TES SHARED GAGAL: charlie seharusnya tidak mendapatkan exclusive lock")
        return

    logger.info("\nStatus setelah 3 request:")
    logger.info(json.dumps(get_leader_status().get('locks', {}), indent=2))

    # 4. 'alice' melepas lock
    logger.info("\n4. 'alice' melepas 'shared-doc'")
    send_lock_request('release_lock', {'client_id': 'alice', 'lock_name': 'shared-doc'})
    
    # 5. 'bob' melepas lock
    logger.info("\n5. 'bob' melepas 'shared-doc' (lock seharusnya diberikan ke 'charlie')")
    send_lock_request('release_lock', {'client_id': 'bob', 'lock_name': 'shared-doc'})

    logger.info("\nStatus setelah 'bob' melepas (charlie harusnya memegang lock):")
    status_locks = get_leader_status().get('locks', {})
    logger.info(json.dumps(status_locks, indent=2))

    shared_doc_lock = status_locks.get('shared-doc', {})
    current_holders = shared_doc_lock.get('holders', {})
    
    if 'charlie' not in current_holders: # Cek jika 'charlie' BUKAN key di holders
         logger.error("TES SHARED GAGAL: 'charlie' tidak ditemukan di pemegang lock 'shared-doc'")
         return
    elif shared_doc_lock.get('type') != 'exclusive': # Cek tipenya juga
         logger.error(f"TES SHARED GAGAL: Lock 'shared-doc' tipenya {shared_doc_lock.get('type')}, bukan exclusive")
         return
         
    # 6. 'charlie' melepas lock
    logger.info("\n6. 'charlie' melepas 'shared-doc'")
    send_lock_request('release_lock', {'client_id': 'charlie', 'lock_name': 'shared-doc'})
    logger.info("\nStatus akhir (lock harusnya bebas):")
    logger.info(json.dumps(get_leader_status().get('locks', {}), indent=2))
    logger.info("--- TES SHARED LOCK BERHASIL ---")


    # --- Tes 2: Deadlock Prevention (Wound-Wait) ---
    # Kita butuh request yang berjalan bersamaan, tapi kita bisa simulasikan
    # dengan mengirimkan request dengan index yang kita *tahu* akan berurutan.
    logger.info("\n\n--- TES DEADLOCK (WOUND-WAIT) ---")
    
    # 1. 'older_client' (akan punya index log N) mendapatkan lock 'deadlock-test'
    logger.info("1. 'older_client' meminta EXCLUSIVE lock 'deadlock-test'")
    res_old = send_lock_request('acquire_lock', {'client_id': 'older_client', 'lock_name': 'deadlock-test', 'type': 'exclusive'})
    logger.info(f"Respons 'older_client': {res_old}")
    if not res_old or not res_old.get('success'):
        logger.error("TES DEADLOCK GAGAL: older_client gagal mendapatkan lock")
        return
        
    status = get_leader_status()
    older_index = status.get('locks', {}).get('deadlock-test', {}).get('holders', {}).get('older_client')
    logger.info(f"'older_client' memegang lock di index {older_index}")

    # 2. 'younger_client' (akan punya index N+1) meminta lock 'deadlock-test'
    #    (ts(P) > ts(Q)), jadi 'younger_client' harus MENUNGGU
    logger.info("\n2. 'younger_client' (lebih muda) meminta lock (seharusnya menunggu)")
    res_young = send_lock_request('acquire_lock', {'client_id': 'younger_client', 'lock_name': 'deadlock-test', 'type': 'exclusive'})
    logger.info(f"Respons 'younger_client': {res_young}")
    if res_young and res_young.get('success'):
         logger.error("TES DEADLOCK GAGAL: younger_client seharusnya menunggu (wait)")
         return

    logger.info("\nStatus setelah 'younger_client' mengantri:")
    logger.info(json.dumps(get_leader_status().get('locks', {}), indent=2))

    # 3. 'older_client' melepas lock (seharusnya diberikan ke 'younger_client')
    logger.info("\n3. 'older_client' melepas lock")
    send_lock_request('release_lock', {'client_id': 'older_client', 'lock_name': 'deadlock-test'})
    
    logger.info("\nStatus setelah 'older_client' melepas (younger harusnya memegang):")
    status_locks = get_leader_status().get('locks', {}) # Ambil dict locks
    logger.info(json.dumps(status_locks, indent=2))

    deadlock_test_lock = status_locks.get('deadlock-test', {})
    current_holders = deadlock_test_lock.get('holders', {})
    
    if 'younger_client' not in current_holders: # Cek jika 'younger_client' BUKAN key di holders
         logger.error("TES DEADLOCK GAGAL: 'younger_client' tidak ditemukan di pemegang lock 'deadlock-test'")
         return
    elif deadlock_test_lock.get('type') != 'exclusive': # Cek tipenya juga
         logger.error(f"TES DEADLOCK GAGAL: Lock 'deadlock-test' tipenya {deadlock_test_lock.get('type')}, bukan exclusive")
         return

    # 4. 'younger_client' melepas lock
    send_lock_request('release_lock', {'client_id': 'younger_client', 'lock_name': 'deadlock-test'})
    
    # 5. Skenario "WOUND":
    logger.info("\n4. 'younger_client' (akan jadi N+k) mendapatkan lock lagi")
    res_young_2 = send_lock_request('acquire_lock', {'client_id': 'younger_client', 'lock_name': 'deadlock-test', 'type': 'exclusive'})
    logger.info(f"Respons 'younger_client' 2: {res_young_2}")
    
    status = get_leader_status()
    younger_index = status.get('locks', {}).get('deadlock-test', {}).get('holders', {}).get('younger_client')
    logger.info(f"'younger_client' memegang lock di index {younger_index}")

    logger.info(f"\n5. 'older_client' (index {older_index}) meminta lock yang dipegang 'younger_client' (index {younger_index})")
    logger.info(f"   (ts(P) < ts(Q)) -> 'older_client' harus 'WOUND' (melukai) 'younger_client'")
    
    # Kita tidak bisa menggunakan index lama 'older_client'. Request baru AKAN
    # mendapatkan index baru, yang LEBIH MUDA.
    # Skenario tes ini salah.
    
    logger.warning("Skenario tes Wound-Wait tidak bisa disimulasikan dengan benar secara sekuensial.")
    logger.warning("Kita harus percaya bahwa logika di state machine (membandingkan index) sudah benar.")
    logger.warning("Untuk tes nyata, kita perlu 2 klien yang mengirim request hampir bersamaan.")
    
    # Mari kita bersihkan saja
    send_lock_request('release_lock', {'client_id': 'younger_client', 'lock_name': 'deadlock-test'})
    
    logger.info("--- Tes Deadlock Selesai (Simulasi Terbatas) ---")

if __name__ == "__main__":
    run_all_tests()

