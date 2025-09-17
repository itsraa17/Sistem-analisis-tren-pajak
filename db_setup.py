"""
db_setup.py
----------------------------------
Panduan & script untuk setup database sistem ini.

1. Ubah DB_PARAMS sesuai kredensial PostgreSQL instansi.
2. Jalankan script ini sekali saja untuk membuat tabel 'riwayat'.
"""

import psycopg2

# >>>> EDIT BAGIAN INI SESUAI DB INSTANSI <<<<
DB_PARAMS = {
    'dbname': 'nama_database_anda',   # ganti dengan nama database yang sudah dibuat
    'user': 'username_anda',          # ganti dengan user PostgreSQL instansi
    'password': 'password_anda',      # ganti password PostgreSQL instansi
    'host': 'localhost',              # biasanya localhost, atau isi IP/hostname server DB
    'port': '5432'                    # default PostgreSQL port
}

def create_table():
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS riwayat (
        id SERIAL PRIMARY KEY,
        id_usaha TEXT,
        nama_usaha TEXT,
        bulan TEXT,
        omset_perbulan NUMERIC,
        jumlah_pajak_dibayar NUMERIC,
        tanggal_pembayaran DATE,
        status TEXT,
        growth NUMERIC,
        kondisi TEXT,
        filename TEXT NOT NULL,
        batch_id TEXT NOT NULL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Buat index agar query lebih cepat
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_riwayat_batch_id ON riwayat(batch_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_riwayat_id_usaha ON riwayat(id_usaha);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_riwayat_timestamp ON riwayat(timestamp);")

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Tabel 'riwayat' berhasil dibuat (atau sudah ada).")

if __name__ == "__main__":
    print("=== Setup Database Dimulai ===")
    create_table()
    print("=== Setup Database Selesai ===")