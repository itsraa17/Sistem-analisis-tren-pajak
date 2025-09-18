# Sistem Deteksi Tren & Anomali Pajak

Aplikasi berbasis Flask + PostgreSQL digunakan untuk menganalisis tren pajak daerah dengan fitur unggah data, visualisasi hasil deteksi, serta riwayat pemrosesan.

---

## Fitur Utama
- Upload file Excel untuk analisis data.
- Deteksi tren pajak berdasarkan input data.
- Riwayat pemrosesan data tersimpan di database.
- Tampilan hasil analisis interaktif (copy, print, filter).

---

## Struktur Proyek
```

.
├── app.py              # Main aplikasi Flask
├── config.py           # Konfigurasi umum
├── db.py               # Koneksi database & query
├── db\_setup.py         # Script setup awal database
├── requirements.txt    # Daftar dependency Python
├── static/             # File statis (CSS, gambar, dll.)
├── templates/          # Template HTML (Flask Jinja2)
│   ├── base.html
│   ├── result.html
│   ├── riwayat.html
│   └── upload.html
└── README.md

````

---

## Konfigurasi Database

Sistem ini menggunakan PostgreSQL sebagai basis data.

1. **Pastikan PostgreSQL sudah terpasang di server.**

2. **Buat database dan user:**
   ```bash
   sudo -u postgres psql
   ```
   ```sql
   CREATE DATABASE tren_pajak;
   CREATE USER admin_pajak WITH PASSWORD 'password_kuat_anda';
   GRANT ALL PRIVILEGES ON DATABASE tren_pajak TO admin_pajak;
   \q
   ```

3. **Sesuaikan parameter koneksi di `db.py`:**
   ```python
   DB_PARAMS = {
       'dbname': 'tren_pajak',
       'user': 'admin_pajak',        # sesuaikan dengan user yang dibuat
       'password': 'password_kuat_anda', # sesuaikan dengan password
       'host': 'localhost',
       'port': '5432'
   }
   ```

4. **Buat tabel database:**
   ```bash
   python db_setup.py
   ```

> ⚠️ **Catatan:**
* Pastikan PostgreSQL service berjalan dengan `sudo systemctl status postgresql`
* Pastikan mengganti `user`, `password`, `host`, dan `port` sesuai dengan server database instansi.

---

## Cara Menjalankan Aplikasi

1. Clone repository ini:

   ```bash
   git clone https://github.com/itsraa17/Sistem-analisis-tren-pajak.git
   cd Sistem-analisis-tren-pajak
   ```

2. Buat dan aktifkan virtual environment (opsional):

   ```bash
   python -m venv venv
   venv\Scripts\activate   # Windows
   source venv/bin/activate   # Linux/Mac
   ```

3. Install dependency:

   ```bash
   pip install -r requirements.txt
   ```

4. Setup database (jalankan sekali saja):

   ```bash
   python db_setup.py
   ```

5. Jalankan aplikasi:

   ```bash
   python app.py
   ```

6. Akses aplikasi di browser melalui:

   ```
   http://127.0.0.1:5000
   ```

---

## Catatan Tambahan

* Semua file hasil analisis akan tersimpan di database dan dapat diakses kembali melalui menu **Riwayat**.
* Pastikan environment Python ≥ 3.10 dan PostgreSQL sudah berjalan sebelum menjalankan aplikasi.

---

