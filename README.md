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

1. Pastikan PostgreSQL sudah terpasang di server.
2. Buat database baru, misalnya dengan nama `tren_pajak`.
3. Sesuaikan parameter koneksi database pada file berikut:
   - `db.py` → ubah bagian `DB_PARAMS`
   - atau jalankan `db_setup.py` untuk membuat tabel `riwayat` secara otomatis.

Contoh konfigurasi (`db.py`):

```python
DB_PARAMS = {
    'dbname': 'tren_pajak',
    'user': 'postgres',
    'password': 'password_anda',
    'host': 'localhost',
    'port': '5432'
}
````

> ⚠️ **Catatan:** Pastikan mengganti `user`, `password`, `host`, dan `port` sesuai dengan server database instansi.

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

4. Jalankan aplikasi:

   ```bash
   python app.py
   ```

5. Akses aplikasi di browser melalui:

   ```
   http://127.0.0.1:5000
   ```

---

## Catatan Tambahan

* Semua file hasil analisis akan tersimpan di database dan dapat diakses kembali melalui menu **Riwayat**.
* Pastikan environment Python ≥ 3.10 dan PostgreSQL sudah berjalan sebelum menjalankan aplikasi.

---

