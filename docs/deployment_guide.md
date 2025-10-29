Deployment Guide dan Troubleshooting

Bagian ini menjelaskan cara menjalankan dan memecahkan masalah Sistem Sinkronisasi Terdistribusi. Teknologi yang digunakan diantaranya adalah Python 3.8+, Pip (Python package installer), Git, Docker dan Docker Compose (untuk deployment via Docker), serta Redis Server (berjalan di localhost:6379 untuk deployment lokal).

1). Deployment Lokal (untuk Pengembangan & Debugging)
    a). Clone Repository:
        git clone https://github.com/fadilahmalfoy/distributed-sync-system.git 
        cd distributed-sync-system
    b). Setup Virtual Environment:
        python -m venv venv
        # Windows PowerShell:
        .\venv\Scripts\Activate.ps1
        # macOS/Linux:
        source venv/bin/activate
    c). Instal Dependensi:
        pip install -r requirements.txt
    d). Siapkan File .env: 
        Buat file .env untuk setiap node yang ingin dijalankan (misalnya, lock_node1.env, queue_node1.env, cache_node1.env, dst.). Pastikan NODE_PORT unik dan PEER_ADDRESSES menunjuk ke alamat localhost dengan port yang benar untuk node lain dengan tipe yang sama. Pastikan REDIS_HOST diset ke localhost.
    e). Jalankan Redis Server
    f). Jalankan Node:
        Buka terminal terpisah untuk setiap node yang ingin Anda jalankan. Aktifkan virtual environment di setiap terminal. Jalankan node dengan perintah:
            # Contoh menjalankan Lock Node 1
            python -m src.nodes.main --env-file lock_node1.env --node-type lock
            # Contoh menjalankan Queue Node 1 
            python -m src.nodes.main --env-file queue_node1.env --node-type queue
            # Contoh menjalankan Cache Node 1 
            python -m src.nodes.main --env-file cache_node1.env --node-type cache Perlu menjalan kan minimal 3 *node* untuk setiap tipe subsistem (Lock, Queue, Cache) agar fitur terdistribusinya berfungsi.
    g). Hentikan Node

2). Deployment dengan Docker Compose
    a). Pastikan Docker Berjalan
    b). Build Image:
        docker-compose -f docker/docker-compose.yml build --no-cache
    c). Jalankan Semua Layanan:
        docker-compose -f docker/docker-compose.yml up -d
    d). Melihat Log:
        # Melihat log semua kontainer
        docker-compose -f docker/docker-compose.yml logs -f 
        # Melihat log kontainer spesifik
        docker-compose -f docker/docker-compose.yml logs node_lock_1
    e). Menghentikan Layanan:
        docker-compose -f docker/docker-compose.yml down
    f). Menghentikan & Menghapus Volume (Data Redis):
        docker-compose -f docker/docker-compose.yml down --volumes
    g). Scaling (Contoh):
        docker-compose -f docker/docker-compose.yml up -d --scale node_queue_scalable=3

3). Troubleshooting Umum
    a). Koneksi Ditolak (Connection refused):
        Pastikan node tujuan berjalan (cek log atau docker ps). Pastikan PEER_ADDRESSES di file .env (lokal) atau docker-compose.yml (docker) sudah benar (hostname/IP dan port). Jika menggunakan Docker, pastikan semua kontainer berada di jaringan Docker Compose yang sama (dist_sys_net).
    b). Error Terhubung ke Redis:
        Pastikan Redis server berjalan (lokal atau kontainer redis_server). Pastikan REDIS_HOST dan REDIS_PORT sudah benar di konfigurasi node. Jika pakai Docker Compose, REDIS_HOST harus redis.
    c). Node Raft Tidak Memilih Leader:
        Pastikan minimal 3 node lock berjalan. Periksa log node lock, apakah ada pesan error Timeout saat mengirim... atau Gagal terhubung…, Ini mungkin masalah jaringan antar node. Pastikan PEER_ADDRESSES antar node lock sudah benar.
    d). Error docker-compose build:
        Periksa Dockerfile.node untuk kesalahan sintaks. Periksa requirements.txt untuk dependensi yang tidak bisa diinstal di Linux (seperti pywin32). Pastikan Docker punya cukup resource (memori, CPU, disk). Cek pengaturan Docker Desktop. Coba docker system prune -a --volumes untuk membersihkan Docker (hati-hati!).
    e). Timeout Menunggu Commit/Apply:
        Ini bisa jadi race condition atau penjadwalan asyncio. Periksa log leader untuk melihat apakah commit dan apply sebenarnya terjadi. Coba tingkatkan nilai timeout di asyncio.wait_for pada handler klien (handle_acquire_lock, handle_release_lock).