import hashlib
import bisect
import logging

logger = logging.getLogger(__name__)

class ConsistentHashRing:
    """Implementasi dasar Consistent Hashing Ring."""

    def __init__(self, nodes=None, replicas=3):
        """
        Args:
            nodes (list): Daftar ID node awal (misal: port integer).
            replicas (int): Jumlah virtual node per node fisik.
        """
        self.replicas = replicas
        # _keys menyimpan posisi sorted dari virtual node
        self._keys = []
        # _nodes map posisi virtual node ke ID node fisik
        self._nodes = {}

        if nodes:
            for node_id in nodes:
                self.add_node(node_id)

        logger.info(f"ConsistentHashRing diinisialisasi. Replicas: {self.replicas}. Nodes awal: {nodes}")

    def _hash(self, key_str):
        """Menghasilkan hash integer dari string."""
        # Gunakan SHA1, ambil 4 byte pertama (32 bit)
        h = hashlib.sha1(key_str.encode('utf-8')).digest()
        # Konversi 4 byte pertama ke integer (big-endian)
        return int.from_bytes(h[:4], 'big') 

    def add_node(self, node_id):
        """Menambahkan node fisik ke ring."""
        node_str = str(node_id) # Pastikan string untuk hashing
        logger.info(f"Menambahkan node {node_id} ke hash ring.")
        for i in range(self.replicas):
            # Buat virtual node key (misal: "8001-0", "8001-1", ...)
            virtual_key_str = f"{node_str}-{i}"
            key_hash = self._hash(virtual_key_str)

            # Masukkan hash ke posisi sorted di _keys
            bisect.insort(self._keys, key_hash)
            # Map hash ini ke node ID asli
            self._nodes[key_hash] = node_id
        logger.debug(f"State ring setelah menambahkan {node_id}: Keys={self._keys}, Nodes={self._nodes}")

    def remove_node(self, node_id):
        """Menghapus node fisik dari ring."""
        node_str = str(node_id)
        logger.info(f"Menghapus node {node_id} dari hash ring.")
        for i in range(self.replicas):
            virtual_key_str = f"{node_str}-{i}"
            key_hash = self._hash(virtual_key_str)

            # Hapus dari _nodes
            if key_hash in self._nodes:
                del self._nodes[key_hash]

            # Hapus dari _keys (cari indexnya dulu)
            # Ini bisa lambat jika _keys besar, tapi cukup untuk demo
            try:
                # Cari semua kemunculan hash (meskipun kecil kemungkinannya sama persis)
                indices_to_remove = [idx for idx, k in enumerate(self._keys) if k == key_hash]
                # Hapus dari belakang agar index tidak bergeser
                for idx in sorted(indices_to_remove, reverse=True):
                     del self._keys[idx]
            except ValueError:
                pass # Hash tidak ditemukan di keys (seharusnya tidak terjadi jika ada di _nodes)
        logger.debug(f"State ring setelah menghapus {node_id}: Keys={self._keys}, Nodes={self._nodes}")


    def get_node(self, key_str):
        """Mendapatkan ID node yang bertanggung jawab untuk key_str."""
        if not self._keys:
            return None

        key_hash = self._hash(key_str)

        # Cari posisi insert point untuk hash key di daftar _keys yang sorted
        # `bisect_left` menemukan index i sedemikian rupa sehingga semua e di a[:i] < x,
        # dan semua e di a[i:] >= x.
        insert_point = bisect.bisect_left(self._keys, key_hash)

        # Jika insert point ada di akhir list, wrap around ke node pertama
        if insert_point == len(self._keys):
            insert_point = 0

        target_hash = self._keys[insert_point]
        return self._nodes[target_hash]