# Sistem Deteksi Tren & Anomali Pajak

Aplikasi berbasis Flask + PostgreSQL untuk mendeteksi tren, anomali, dan kepatuhan pajak dari data laporan bulanan usaha.


## Fitur Utama

* Upload data Excel pajak bulanan
* Deteksi otomatis tren & anomali
* Dashboard interaktif (grafik + tabel)
* Riwayat upload data

---

## Struktur Folder

```
SISTEM/
│
├── app.py            # Main Flask app
├── config.py         # Konfigurasi tambahan processing data
├── db.py             # Fungsi database
├── db_setup.py       # Setup database instansi
├── requirement.txt   # Dependency Python
│
├── static/           # File CSS
    ├── base.html       
├── templates/        # Template HTML
│   ├── base.html
│   ├── result.html
│   ├── riwayat.html
│   └── upload.html
│
└── __pycache__/      # Cache Python
```

---

## Instalasi

1. **Clone repository**
   ```bash
   git clone https://github.com/username/sistem-deteksi-pajak.git
   cd sistem-deteksi-pajak
````

2. **Buat & aktifkan virtual environment** (opsional tapi disarankan)

   ```bash
   python -m venv venv
   source venv/bin/activate   # Linux/Mac
   venv\Scripts\activate      # Windows
   ```

3. **Install dependency**

   ```bash
   pip install -r requirement.txt
   ```

---

##  Setup Database

1. **Buat database baru di PostgreSQL**
   Masuk ke PostgreSQL lalu jalankan:

   ```sql
   CREATE DATABASE tren_pajak;
   ```

2. **Edit file `db_setup.py`** → sesuaikan dengan kredensial PostgreSQL instansi:

   ```python
   DB_PARAMS = {
       'dbname': 'tren_pajak',
       'user': 'postgres',
       'password': 'password_anda',
       'host': 'localhost',
       'port': '5432'
   }
   ```

3. **Jalankan script setup** untuk membuat tabel `riwayat`

   ```bash
   python db_setup.py
   ```

4. **Cek tabel di PostgreSQL**

   ```sql
   \dt
   ```

Jika tabel `riwayat` sudah muncul, database siap digunakan.

---

## Menjalankan Aplikasi

1. Jalankan Flask:

   ```bash
   python app.py
   ```

2. Buka aplikasi di browser:
   [http://localhost:5000](http://localhost:5000)


---

## Requirement Utama

* Python 3.10+
* PostgreSQL 13+
* Paket Python (sudah ada di `requirement.txt`):

  * Flask
  * pandas
  * psycopg2

---
