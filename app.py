#app.py

from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
import os
import re
import calendar
from db import insert_history_flexible, fetch_file_list, fetch_by_batch_flexible, get_connection
from db import delete_all_history, delete_batch
from config import DataAttributeConfig, validate_required_columns, map_optional_columns

app = Flask(__name__)

def color_kondisi(val):
    if val == 'NORMAL':
        return '''
            background-color: lightgreen !important; 
            font-weight: bold !important;
            print-color-adjust: exact !important;
            -webkit-print-color-adjust: exact !important;
            color: black !important;
        '''
    elif val == 'ANOMALI':
        return '''
            background-color: orange !important; 
            font-weight: bold !important;
            print-color-adjust: exact !important;
            -webkit-print-color-adjust: exact !important;
            color: black !important;
        '''
    elif val == 'TIDAK TAAT PAJAK':
        return '''
            background-color: red !important; 
            color: white !important; 
            font-weight: bold !important;
            print-color-adjust: exact !important;
            -webkit-print-color-adjust: exact !important;
        '''
    return ''

def clean_text_data(series):

    return (
        series
        .astype(str)
        .str.strip()  
        .str.replace(r'\s+', ' ', regex=True)  
        .str.replace(r'[^\w\s\-\:\/]', '', regex=True) 
        .str.replace(r'[^\x00-\x7F]+', '', regex=True)  
        .str.replace(r'[\n\r\t\f\v\u00A0]', ' ', regex=True) 
        .str.strip() 
    )

def format_currency(value):
    """
    Format angka untuk tampilan yang lebih bersih
    """
    if pd.isna(value) or value == '' or value == '-':
        return '-'
    
    try:
        num_value = float(value)
        if num_value == 0:
            return '-'
        
        # Jika angka adalah bilangan bulat, tampilkan tanpa desimal
        if num_value.is_integer():
            return f"{int(num_value):,}"
        else:
            # Jika ada desimal, tampilkan dengan 2 digit desimal
            return f"{num_value:,.2f}"
            
    except (ValueError, TypeError):
        return str(value)

def parse_bulan_col(series):
    """
    Robust parsing of the 'bulan' column: try multiple formats
    """
    s1 = pd.to_datetime(series, format='%Y%m', errors='coerce')  
    s2 = pd.to_datetime(series, format='%y-%m', errors='coerce')  
    s3 = pd.to_datetime(series, errors='coerce')                 
    return s1.fillna(s2).fillna(s3)

def preprocess_excel(file):
    """
    Preprocess file Excel dengan header bulan di row 0
    REVISI: Generate complete data matrix - semua usaha dari bulan pertama sampai terakhir yang ada data
    FIXED: Handle optional columns yang mungkin tidak ada
    """
    # Baca excel
    df = pd.read_excel(file)
    
    # SPECIAL HANDLING: Header bulan ada di row 0
    if len(df) > 0:
        header_row = df.iloc[0]  # Row pertama berisi nama bulan
        print(f"DEBUG: Header row content: {header_row.tolist()}")
        
        # Buat mapping nama kolom baru
        new_column_names = []
        for i, col in enumerate(df.columns):
            # Cek apakah header row ada nama bulan yang valid
            if not pd.isna(header_row.iloc[i]) and str(header_row.iloc[i]).strip():
                header_value = str(header_row.iloc[i]).strip()
                
                # Jika header berisi nama bulan, gunakan sebagai nama kolom
                month_keywords = ['JANUARI', 'FEBRUARI', 'MARET', 'APRIL', 'MEI', 'JUNI', 
                                'JULI', 'AGUSTUS', 'SEPTEMBER', 'OKTOBER', 'NOVEMBER', 'DESEMBER',
                                'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
                
                if any(header_value.upper().startswith(month.upper()) for month in month_keywords):
                    month_name = header_value.strip().lower()
                    new_column_names.append(month_name)
                    print(f"DEBUG: Mapped column '{col}' → '{month_name}'")
                else:
                    # Pertahankan nama asli untuk non-month
                    new_column_names.append(col)
            else:
                # Pertahankan nama asli untuk kolom identitas
                new_column_names.append(col)
        
        # Rename kolom
        df.columns = new_column_names
        print(f"DEBUG: After renaming with header row: {list(df.columns)}")
        
        # Hapus row 0 karena sudah dipakai sebagai header
        df = df.drop(0).reset_index(drop=True)
        print(f"DEBUG: After dropping header row: {df.shape}")
    
    # Mapping kolom identitas - FIXED: Handle optional columns
    column_mapping = {
        'JENIS PAJAK USAHA': 'jenis_pajak_usaha',
        'NPWPD': 'npwpd',
        'NOPD': 'nopd', 
        'NAMA USAHA': 'nama_usaha'
    }
    
    df = df.rename(columns=column_mapping)
    print(f"DEBUG: After identity column mapping: {list(df.columns)}")
    
    # FIXED: Identifikasi kolom identitas yang benar-benar ada
    # Hanya gunakan kolom required + optional yang tersedia
    potential_identity_cols = ['jenis_pajak_usaha', 'npwpd', 'nopd', 'nama_usaha']
    identity_cols = [col for col in potential_identity_cols if col in df.columns]
    
    print(f"DEBUG: Available identity columns: {identity_cols}")
    
    # Validasi minimal: harus ada nama_usaha dan salah satu ID (nopd/npwpd)
    if 'nama_usaha' not in identity_cols:
        raise ValueError("Kolom 'NAMA USAHA' tidak ditemukan dalam file Excel")
    
    if not any(col in identity_cols for col in ['nopd', 'npwpd']):
        raise ValueError("Tidak ditemukan kolom ID usaha (NOPD atau NPWPD) dalam file Excel")
    
    # Kolom bulan = semua kolom selain identitas dan yang mengandung "PEMBAYARAN" atau "Unnamed"
    month_cols = []
    for col in df.columns:
        if (col not in identity_cols and 
            not col.startswith('PEMBAYARAN') and 
            not col.startswith('Unnamed') and
            col.strip() != ''):
            month_cols.append(col)
    
    print(f"DEBUG: Identified month columns: {month_cols}")
    
    if not month_cols:
        raise ValueError("Tidak dapat mengidentifikasi kolom bulan dalam file Excel")
    
    # Drop rows yang tidak memiliki data identitas minimal (nama_usaha)
    df = df.dropna(subset=['nama_usaha'])
    
    # Jika tidak ada npwpd tapi ada nopd, gunakan nopd untuk identifikasi unique
    if 'npwpd' not in df.columns and 'nopd' in df.columns:
        df = df.dropna(subset=['nopd'])
        unique_id_col = 'nopd'
    elif 'npwpd' in df.columns:
        df = df.dropna(subset=['npwpd'])  
        unique_id_col = 'npwpd'
    else:
        # Fallback: buat ID dari nama_usaha
        df['temp_id'] = df['nama_usaha'].astype(str).str.strip()
        unique_id_col = 'temp_id'
    
    print(f"DEBUG: Using {unique_id_col} as unique identifier")
    print(f"DEBUG: After dropping empty identity rows: {df.shape}")
    
    # Melt: ubah wide format ke long format - FIXED: gunakan identity_cols yang dinamis
    df_long = df.melt(
        id_vars=identity_cols,
        value_vars=month_cols,
        var_name="bulan",
        value_name="jumlah_pajak_dibayar"
    )
    
    print(f"DEBUG: After initial melt shape: {df_long.shape}")
    
    # ===== REVISI UTAMA: GENERATE COMPLETE DATA MATRIX DENGAN RANGE DETECTION =====
    
    # Convert jumlah_pajak_dibayar ke numeric untuk deteksi data
    df_long['jumlah_pajak_dibayar'] = pd.to_numeric(df_long['jumlah_pajak_dibayar'], errors='coerce')
    
    # Identifikasi bulan-bulan yang benar-benar ada pembayaran
    months_with_payment = df_long[
        (df_long['jumlah_pajak_dibayar'].notna()) & 
        (df_long['jumlah_pajak_dibayar'] > 0)
    ]['bulan'].unique()
    
    print(f"DEBUG: Months with actual payments: {sorted(months_with_payment)}")
    
    if len(months_with_payment) == 0:
        raise ValueError("Tidak ada data pembayaran yang valid ditemukan dalam file")
    
    # Definisi urutan bulan untuk sorting kronologis
    month_order = ['januari', 'februari', 'maret', 'april', 'mei', 'juni', 
                   'juli', 'agustus', 'september', 'oktober', 'november', 'desember']
    
    # Tentukan range dari bulan pertama sampai terakhir yang ada pembayaran
    month_indices = []
    for month in months_with_payment:
        month_clean = month.strip().lower()
        if month_clean in month_order:
            month_indices.append(month_order.index(month_clean))
        else:
            print(f"WARNING: Unknown month name '{month}', skipping...")
    
    if not month_indices:
        # Fallback: gunakan semua bulan yang ada di data
        all_months = sorted(df_long['bulan'].unique())
        print(f"DEBUG: Fallback - using all months found: {all_months}")
    else:
        # Buat range dari bulan pertama sampai terakhir
        first_month_idx = min(month_indices)
        last_month_idx = max(month_indices)
        
        # Generate range bulan
        month_range = [month_order[i] for i in range(first_month_idx, last_month_idx + 1)]
        
        # Filter hanya bulan yang ada di data asli (untuk handle case nama bulan beda)
        all_months = []
        for month in month_range:
            matching_months = [m for m in df_long['bulan'].unique() if m.strip().lower() == month]
            if matching_months:
                all_months.append(matching_months[0])  # Ambil yang pertama dengan formatting asli
        
        print(f"DEBUG: Detected month range: {month_order[first_month_idx]} to {month_order[last_month_idx]}")
        print(f"DEBUG: Final month list for complete matrix: {all_months}")
    
    # Identifikasi semua usaha unik - FIXED: gunakan unique_id_col yang dinamis
    all_businesses = df_long[identity_cols].drop_duplicates()
    print(f"DEBUG: Found {len(all_businesses)} unique businesses")
    print(f"DEBUG: Will create complete matrix: {len(all_businesses)} businesses × {len(all_months)} months = {len(all_businesses) * len(all_months)} records")
    
    # Buat kombinasi lengkap semua usaha × range bulan - FIXED: handle optional columns
    complete_combinations = []
    for _, business in all_businesses.iterrows():
        for month in all_months:
            combination = {'bulan': month, 'jumlah_pajak_dibayar': None}
            
            # Tambahkan kolom identitas yang tersedia
            for col in identity_cols:
                combination[col] = business[col]
            
            complete_combinations.append(combination)
    
    df_complete = pd.DataFrame(complete_combinations)
    print(f"DEBUG: Complete combinations shape: {df_complete.shape}")
    
    # Merge dengan data asli, prioritaskan data asli jika ada
    # FIXED: gunakan unique_id_col untuk merge
    df_merged = df_complete.merge(
        df_long[[unique_id_col, 'bulan', 'jumlah_pajak_dibayar']], 
        on=[unique_id_col, 'bulan'], 
        how='left',
        suffixes=('', '_actual')
    )
    
    # Update dengan data aktual jika ada, sisanya tetap None
    df_merged['jumlah_pajak_dibayar'] = df_merged['jumlah_pajak_dibayar_actual'].fillna(df_merged['jumlah_pajak_dibayar'])
    
    # Drop kolom helper
    df_merged = df_merged.drop(columns=['jumlah_pajak_dibayar_actual'], errors='ignore')
    
    df_long = df_merged
    
    print(f"DEBUG: After merging complete combinations: {df_long.shape}")
    
    # Hitung omset berdasarkan pajak (hanya untuk yang ada pajak)
    df_long['omset_perbulan'] = df_long['jumlah_pajak_dibayar'].apply(
        lambda x: x * 10 if pd.notna(x) and x > 0 else None
    )
    
    # Convert nama bulan ke format ISO (YYYY-MM)
    tahun = 2025  # Sesuai dengan "PEMBAYARAN TAHUN 2025"
    
    bulan_mapping = {
        'januari': '01', 'jan': '01',
        'februari': '02', 'feb': '02', 
        'maret': '03', 'mar': '03',
        'april': '04', 'apr': '04',
        'mei': '05', 'may': '05',
        'juni': '06', 'jun': '06',
        'juli': '07', 'jul': '07',
        'agustus': '08', 'agu': '08', 'aug': '08',
        'september': '09', 'sep': '09',
        'oktober': '10', 'okt': '10', 'oct': '10',
        'november': '11', 'nov': '11',
        'desember': '12', 'des': '12', 'dec': '12'
    }
    
    def convert_month_to_iso(month_name):
        if pd.isna(month_name):
            return None
        
        month_clean = str(month_name).strip().lower()
        month_num = bulan_mapping.get(month_clean, None)
        
        if month_num:
            return f"{tahun}-{month_num}"
        else:
            print(f"DEBUG: Unknown month name: '{month_name}'")
            return f"{tahun}-01"  # Default ke Januari
    
    df_long['bulan_iso'] = df_long['bulan'].apply(convert_month_to_iso)
    
    # Buat tanggal pembayaran HANYA untuk yang benar-benar bayar pajak
    def create_payment_date(row):
        if pd.notna(row['jumlah_pajak_dibayar']) and row['jumlah_pajak_dibayar'] > 0 and row['bulan_iso']:
            return pd.to_datetime(row['bulan_iso'] + '-15', errors='coerce')
        else:
            return pd.NaT  # No payment date for non-payments
    
    df_long['tanggal_pembayaran'] = df_long.apply(create_payment_date, axis=1)
    
    # Buat id_usaha - FIXED: gunakan unique_id_col atau fallback
    if 'nopd' in df_long.columns:
        df_long['id_usaha'] = df_long['nopd'].astype(str)
    elif 'npwpd' in df_long.columns:
        df_long['id_usaha'] = df_long['npwpd'].astype(str)
    else:
        # Fallback: gunakan nama_usaha sebagai ID
        df_long['id_usaha'] = df_long['nama_usaha'].astype(str)
    
    # Reorder kolom - FIXED: hanya ambil kolom yang ada
    base_columns = [
        "nama_usaha",
        "bulan",
        "bulan_iso",
        "omset_perbulan",
        "jumlah_pajak_dibayar",
        "tanggal_pembayaran",
        "id_usaha"
    ]
    
    # Tambahkan kolom optional yang tersedia
    optional_columns = ["jenis_pajak_usaha", "npwpd", "nopd"]
    
    final_columns = []
    for col in base_columns + optional_columns:
        if col in df_long.columns:
            final_columns.append(col)
    
    df_long = df_long[final_columns]
    
    return df_long

# Revisi fungsi process_data dengan sistem atribut fleksibel

def process_data_flexible(data):
    """
    Fungsi processing data yang fleksibel dengan kategorisasi atribut
    Input: DataFrame atau file
    Output: DataFrame dengan kolom yang sudah divalidasi dan diperkaya
    """
    # Load data
    if isinstance(data, str) or hasattr(data, "filename"):
        ext = os.path.splitext(data.filename)[1].lower()
        if ext == '.csv':
            df = pd.read_csv(data, encoding='latin1')
        elif ext in ['.xls', '.xlsx']:
            df = pd.read_excel(data, engine='openpyxl') 
        else:
            raise ValueError("Format file tidak didukung.")
    else:
        df = data.copy()
    
    config = DataAttributeConfig()
    
    print(f"DEBUG: Input data shape: {df.shape}")
    print(f"DEBUG: Input columns: {list(df.columns)}")
    
    # ===== STEP 1: VALIDASI KOLOM REQUIRED =====
    df_validated, missing_required, mapped_columns = validate_required_columns(df, config)
    
    if missing_required:
        # Coba buat kolom yang hilang dari kolom lain jika memungkinkan
        for missing_col in missing_required.copy():
            if missing_col == 'nopd':
                # Buat nopd dari npwpd jika ada
                if 'npwpd' in df_validated.columns:
                    df_validated['nopd'] = df_validated['npwpd'].astype(str)
                    missing_required.remove(missing_col)
                    print(f"DEBUG: Created {missing_col} from npwpd")
                elif 'id_usaha' in df_validated.columns:
                    df_validated['nopd'] = df_validated['id_usaha'].astype(str)
                    missing_required.remove(missing_col)
                    print(f"DEBUG: Created {missing_col} from id_usaha")
        
        # Jika masih ada yang hilang, raise error
        if missing_required:
            raise ValueError(f"Kolom required tidak ditemukan: {missing_required}. "
                           f"Pastikan file memiliki kolom: {list(config.REQUIRED_COLUMNS.keys())}")
    
    print(f"DEBUG: Column mapping applied: {mapped_columns}")
    
    # ===== STEP 2: MAPPING KOLOM OPSIONAL =====
    df_processed, found_optional = map_optional_columns(df_validated, config)
    
    print(f"DEBUG: Found optional columns - Hidden: {found_optional['hidden']}, Display: {found_optional['display']}")
    
    # ===== STEP 3: VALIDASI DAN CLEANING DATA =====
    
    # Clean required text columns
    for col in ['nopd', 'nama_usaha', 'bulan']:
        if col in df_processed.columns:
            df_processed[col] = clean_text_data(df_processed[col])
    
    # Validasi dan konversi pajak ke numeric
    if 'jumlah_pajak_dibayar' in df_processed.columns:
        df_processed['jumlah_pajak_dibayar'] = pd.to_numeric(
            df_processed['jumlah_pajak_dibayar'], errors='coerce'
        )
    
    # Handle tanggal pembayaran jika ada
    if 'tanggal_pembayaran' in df_processed.columns:
        df_processed['tanggal_pembayaran'] = pd.to_datetime(
            df_processed['tanggal_pembayaran'], errors='coerce'
        )
    
    # ===== STEP 4: GENERATE KOLOM TAMBAHAN =====
    
    # 1. Hitung Omset (pajak × 10)
    def calculate_omset(pajak):
        return config.ADDITIONAL_COLUMNS['omset_perbulan']['calculation'](pajak)
    
    df_processed['omset_perbulan'] = df_processed['jumlah_pajak_dibayar'].apply(calculate_omset)
    
    # 2. Generate Status
    def validate_payment_status(row):
        """
        REVISI: Status berdasarkan kelengkapan data required dan pembayaran
        """
        # Cek kelengkapan data required
        nama_valid = row.get('nama_usaha') and str(row.get('nama_usaha')).strip() not in ['', 'nan', 'None']
        nopd_valid = row.get('nopd') and str(row.get('nopd')).strip() not in ['', 'nan', 'None'] 
        bulan_valid = row.get('bulan') and str(row.get('bulan')).strip() not in ['', 'nan', 'None']
        
        # Cek pembayaran
        pajak = row.get('jumlah_pajak_dibayar')
        pajak_valid = pajak is not None and pd.notna(pajak) and float(pajak) > 0
        
        # VALID jika semua required lengkap DAN ada pembayaran
        if nama_valid and nopd_valid and bulan_valid and pajak_valid:
            return 'VALID'
        else:
            return 'TIDAK VALID'
    
    df_processed['status'] = df_processed.apply(validate_payment_status, axis=1)
    
    # 3. Generate Growth (hanya untuk data VALID)
    df_processed['growth'] = pd.NA
    
    # Group by nopd untuk hitung growth per usaha
    for nopd in df_processed['nopd'].unique():
        usaha_data = df_processed[df_processed['nopd'] == nopd].copy()
        
        # Sort by bulan 
        if 'bulan_iso' in usaha_data.columns:
            usaha_data = usaha_data.sort_values('bulan_iso')
        else:
            usaha_data = usaha_data.sort_values('bulan')
        
        # Hitung growth hanya untuk data VALID berturut-turut
        valid_data = usaha_data[usaha_data['status'] == 'VALID'].copy()
        
        if len(valid_data) >= 2:
            prev_pajak = None
            
            for idx, row in valid_data.iterrows():
                if prev_pajak is None:
                    df_processed.loc[idx, 'growth'] = pd.NA  # First record
                else:
                    cur_pajak = row['jumlah_pajak_dibayar']
                    
                    if prev_pajak == 0:
                        growth = 1.0 if cur_pajak > 0 else 0.0
                    else:
                        growth = (cur_pajak - prev_pajak) / prev_pajak
                    
                    # Clamp to reasonable bounds
                    growth = min(max(growth, -1.0), 10.0)
                    df_processed.loc[idx, 'growth'] = growth
                
                prev_pajak = row['jumlah_pajak_dibayar']
    
    # 4. Generate Kondisi
    def detect_condition(row):
        """
        REVISI: Kondisi berdasarkan status dan growth
        """
        if row['status'] != 'VALID':
            return 'TIDAK TAAT PAJAK'
        
        if pd.isna(row['growth']):
            return 'NORMAL'  # First valid record
            
        # Deteksi anomali berdasarkan growth ekstrem
        if abs(row['growth']) >= 0.5:  # Growth >= 50%
            return 'ANOMALI'
            
        return 'NORMAL'
    
    df_processed['kondisi'] = df_processed.apply(detect_condition, axis=1)
    
    # ===== STEP 5: ARRANGE FINAL COLUMNS =====
    
    # Susun kolom sesuai OUTPUT_COLUMN_ORDER, hanya ambil yang ada
    available_cols = []
    for col in config.OUTPUT_COLUMN_ORDER:
        if col in df_processed.columns:
            available_cols.append(col)
        elif col in found_optional['display']:  # Optional display columns
            if col in df_processed.columns:
                available_cols.append(col)
    
    # Tambahkan kolom hidden optional jika diperlukan untuk debugging (tapi tidak ditampilkan)
    debug_cols = []
    for col in found_optional['hidden']:
        if col in df_processed.columns:
            debug_cols.append(col)
    
    final_columns = available_cols + debug_cols
    
    # Tambahkan kolom yang mungkin terlewat
    remaining_cols = [c for c in df_processed.columns if c not in final_columns]
    final_columns.extend(remaining_cols)
    
    df_final = df_processed[final_columns]
    
    # ===== STEP 6: LOGGING =====
    print(f"DEBUG: Final data shape: {df_final.shape}")
    print(f"DEBUG: Final columns: {list(df_final.columns)}")
    
    status_counts = df_final['status'].value_counts()
    kondisi_counts = df_final['kondisi'].value_counts()
    
    print(f"DEBUG: Status distribution: {status_counts.to_dict()}")
    print(f"DEBUG: Kondisi distribution: {kondisi_counts.to_dict()}")
    
    return df_final


def get_display_columns(df, config=None):
    """
    Helper function untuk mendapatkan kolom yang akan ditampilkan di UI
    """
    if config is None:
        config = DataAttributeConfig()
    
    display_cols = []
    
    # Ambil kolom dari OUTPUT_COLUMN_ORDER yang ada di df
    for col in config.OUTPUT_COLUMN_ORDER:
        if col in df.columns:
            display_cols.append(col)
    
    return display_cols


def get_column_display_mapping(config=None):
    """
    Helper function untuk mendapatkan mapping nama kolom untuk display
    """
    if config is None:
        config = DataAttributeConfig()
    
    return config.DISPLAY_NAMES.copy()


def calculate_dashboard_metrics(df):
    """
    Calculate metrics for dashboard - FIXED untuk konsistensi upload & riwayat
    """
    try:
        print(f"DEBUG Dashboard: Input shape {df.shape}, columns: {list(df.columns)}")
        
        # FIXED: Total unique businesses - handle both numeric and string data
        total_usaha = 0
        
        # Try different ID columns in order of preference
        id_columns = ['id_usaha', 'nopd', 'npwpd', 'nama_usaha']
        for col in id_columns:
            if col in df.columns:
                # Clean the data first - remove NaN, empty strings, 'nan' strings
                valid_ids = df[col].dropna()
                valid_ids = valid_ids[valid_ids.astype(str).str.strip() != '']
                valid_ids = valid_ids[valid_ids.astype(str).str.strip().str.lower() != 'nan']
                
                if len(valid_ids) > 0:
                    total_usaha = valid_ids.nunique()
                    print(f"DEBUG: Used column '{col}' for total_usaha: {total_usaha}")
                    break
        
        # FIXED: Percentage of compliant records - handle string kondisi values
        persentase_patuh = 0
        if 'kondisi' in df.columns:
            # Clean kondisi column
            kondisi_clean = df['kondisi'].dropna().astype(str).str.strip().str.upper()
            normal_count = len(kondisi_clean[kondisi_clean == 'NORMAL'])
            total_records = len(kondisi_clean)
            persentase_patuh = round((normal_count / total_records) * 100) if total_records > 0 else 0
            print(f"DEBUG: Normal: {normal_count}, Total: {total_records}, Percentage: {persentase_patuh}%")
        
        # FIXED: Total omset - handle string values from database  
        total_omset = 0
        if 'omset_perbulan' in df.columns:
            def safe_convert_to_numeric(val):
                if pd.isna(val) or val in ['', '-', 'nan', None]:
                    return 0
                try:
                    # Handle string numbers with commas
                    if isinstance(val, str):
                        val = val.replace(',', '').replace('Rp', '').replace(' ', '')
                    return float(val)
                except (ValueError, TypeError):
                    return 0
            
            omset_numeric = df['omset_perbulan'].apply(safe_convert_to_numeric)
            total_omset = omset_numeric.sum()
            print(f"DEBUG: Total omset calculated: {total_omset}")
        
        # FIXED: Jumlah anomali - handle string kondisi values
        anomali_count = 0
        if 'kondisi' in df.columns:
            kondisi_clean = df['kondisi'].dropna().astype(str).str.strip().str.upper()
            anomali_count = len(kondisi_clean[kondisi_clean == 'ANOMALI'])
        
        # FIXED: Status distribution - handle string values
        status_counts = {}
        if 'kondisi' in df.columns:
            kondisi_clean = df['kondisi'].dropna().astype(str).str.strip().str.upper()
            status_counts = kondisi_clean.value_counts().to_dict()
        
        # FIXED: Monthly trend data dengan normalisasi bulan yang konsisten
        monthly_data = []
        
        # Prioritas kolom bulan
        bulan_col = None
        if 'bulan_iso' in df.columns and df['bulan_iso'].notna().sum() > 0:
            bulan_col = 'bulan_iso'
        elif 'bulan' in df.columns and df['bulan'].notna().sum() > 0:
            bulan_col = 'bulan'
        
        if bulan_col and bulan_col in df.columns:
            try:
                # Prepare data for trend
                df_for_trend = df.copy()
                
                # FIXED: Convert string values to numeric safely
                def safe_numeric_convert(val):
                    if pd.isna(val) or val in ['', '-', 'nan', None]:
                        return 0
                    try:
                        if isinstance(val, str):
                            val = val.replace(',', '').replace('Rp', '').replace(' ', '')
                        return float(val)
                    except (ValueError, TypeError):
                        return 0
                
                df_for_trend['omset_numeric'] = df_for_trend['omset_perbulan'].apply(safe_numeric_convert)
                df_for_trend['pajak_numeric'] = df_for_trend['jumlah_pajak_dibayar'].apply(safe_numeric_convert)
                
                # Filter data yang valid (ada omset atau pajak)
                df_valid = df_for_trend[
                    (df_for_trend['omset_numeric'] > 0) | (df_for_trend['pajak_numeric'] > 0)
                ].copy()
                
                if len(df_valid) > 0:
                    # Clean bulan data
                    df_clean = df_valid.dropna(subset=[bulan_col])
                    df_clean = df_clean[df_clean[bulan_col].astype(str).str.strip() != '']
                    
                    if len(df_clean) > 0:
                        # FIXED: Normalisasi bulan ke format yang konsisten
                        def normalize_month(bulan_val):
                            bulan_str = str(bulan_val).strip()
                            
                            # Jika format YYYY-MM, convert ke MM atau nama bulan
                            if re.match(r'^\d{4}-\d{2}$', bulan_str):  # 2025-04 format
                                year, month_num = bulan_str.split('-')
                                month_names = ['', 'januari', 'februari', 'maret', 'april', 'mei', 'juni',
                                             'juli', 'agustus', 'september', 'oktober', 'november', 'desember']
                                if int(month_num) <= 12:
                                    return month_names[int(month_num)], int(month_num)
                                    
                            # Jika format YY-MM, convert juga
                            elif re.match(r'^\d{2}-\d{2}$', bulan_str):  # 25-04 format
                                year, month_num = bulan_str.split('-')
                                month_names = ['', 'januari', 'februari', 'maret', 'april', 'mei', 'juni',
                                             'juli', 'agustus', 'september', 'oktober', 'november', 'desember']
                                if int(month_num) <= 12:
                                    return month_names[int(month_num)], int(month_num)
                            
                            # Jika sudah nama bulan, mapping ke nomor
                            elif bulan_str.lower() in ['januari', 'jan']:
                                return 'januari', 1
                            elif bulan_str.lower() in ['februari', 'feb']:
                                return 'februari', 2
                            elif bulan_str.lower() in ['maret', 'mar']:
                                return 'maret', 3
                            elif bulan_str.lower() in ['april', 'apr']:
                                return 'april', 4
                            elif bulan_str.lower() in ['mei', 'may']:
                                return 'mei', 5
                            elif bulan_str.lower() in ['juni', 'jun']:
                                return 'juni', 6
                            elif bulan_str.lower() in ['juli', 'jul']:
                                return 'juli', 7
                            elif bulan_str.lower() in ['agustus', 'agu', 'aug']:
                                return 'agustus', 8
                            elif bulan_str.lower() in ['september', 'sep']:
                                return 'september', 9
                            elif bulan_str.lower() in ['oktober', 'okt', 'oct']:
                                return 'oktober', 10
                            elif bulan_str.lower() in ['november', 'nov']:
                                return 'november', 11
                            elif bulan_str.lower() in ['desember', 'des', 'dec']:
                                return 'desember', 12
                            
                            # Default fallback
                            return bulan_str, 99
                        
                        # Apply normalization
                        df_clean[['bulan_normalized', 'bulan_order']] = df_clean[bulan_col].apply(
                            lambda x: pd.Series(normalize_month(x))
                        )
                        
                        # Group by normalized month
                        grouped = df_clean.groupby(['bulan_normalized', 'bulan_order']).agg({
                            'omset_numeric': 'sum',
                            'pajak_numeric': 'sum'
                        }).reset_index()
                        
                        # FIXED: Sort berdasarkan urutan bulan yang benar (1-12)
                        grouped = grouped.sort_values('bulan_order')
                        
                        # Convert to output format
                        for _, row in grouped.iterrows():
                            if row['bulan_order'] != 99:  # Skip fallback entries
                                monthly_data.append({
                                    'bulan_display': row['bulan_normalized'],
                                    'bulan': row['bulan_normalized'],
                                    'omset_perbulan': float(row['omset_numeric']),
                                    'jumlah_pajak_dibayar': float(row['pajak_numeric'])
                                })
                        
                        print(f"DEBUG Dashboard: Monthly data created: {len(monthly_data)} entries")
                
            except Exception as e:
                print(f"ERROR in monthly trend calculation: {e}")
                import traceback
                traceback.print_exc()
                monthly_data = []
        
        result = {
            'total_usaha': total_usaha,
            'persentase_patuh': persentase_patuh,
            'total_omset': total_omset,
            'jumlah_anomali': anomali_count,
            'status_counts': status_counts,
            'monthly_trend': monthly_data
        }
        
        print(f"DEBUG Dashboard: Final result: {result}")
        return result
        
    except Exception as e:
        print(f"ERROR in calculate_dashboard_metrics: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'total_usaha': 0,
            'persentase_patuh': 0,
            'total_omset': 0,
            'jumlah_anomali': 0,
            'status_counts': {},
            'monthly_trend': []
        }


# Revisi route upload di app.py

@app.route('/', methods=['GET', 'POST'])
def upload():
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '':
            return render_template('upload.html', error="Pilih file terlebih dahulu.")
        
        try:
            print(f"\n=== Processing file: {file.filename} ===")
            
            # Step 1: Preprocess Excel (tetap menggunakan fungsi yang ada)
            df_preprocessed = preprocess_excel(file)
            
            # Step 2: Processing dengan sistem atribut fleksibel - BARU!
            df_raw = process_data_flexible(df_preprocessed)
            
            # Step 3: Hitung dashboard metrics (menggunakan data raw)
            dashboard_data = calculate_dashboard_metrics(df_raw)
            print(f"DEBUG: Dashboard calculated - total_omset: {dashboard_data.get('total_omset', 0)}")
            
            # Step 4: Simpan ke riwayat (gunakan data raw)
            filename = file.filename
            insert_history_flexible(df_raw, filename)
            
            # Step 5: Siapkan data untuk display (format string)
            df_display = prepare_display_data(df_raw)
            
            print(f"=== File processed successfully ===\n")
            
        except ValueError as ve:
            # Error khusus untuk missing required columns
            error_msg = str(ve)
            if "Kolom required tidak ditemukan" in error_msg:
                return render_template('upload.html', 
                                     error=f"File tidak valid: {error_msg}")
            else:
                return render_template('upload.html', 
                                     error=f"Error memproses data: {error_msg}")
        
        except Exception as e:
            print(f"Error processing file: {e}")
            import traceback
            traceback.print_exc()
            return render_template('upload.html', 
                                 error=f"Terjadi error saat memproses file: {e}")

        # Prepare data untuk tampilan tabel - REVISED!
        config = DataAttributeConfig()
        
        # Gunakan helper function untuk mendapatkan kolom display
        show_cols = get_display_columns(df_display, config)
        
        # Gunakan helper function untuk mapping display names
        column_display_mapping = get_column_display_mapping(config)
        
        print(f"DEBUG: Display columns: {show_cols}")
        print(f"DEBUG: Column mapping: {column_display_mapping}")

        # Prepare data untuk tabel dengan styling
        data = []
        for _, row in df_display[show_cols].iterrows():
            row_dict = row.to_dict()
            if 'kondisi' in row_dict:
                row_dict['kondisi_style'] = color_kondisi(row_dict['kondisi'])
            data.append(row_dict)
            
        return render_template('result.html', 
                             data=data, 
                             columns=show_cols, 
                             column_display_mapping=column_display_mapping,
                             dashboard_data=dashboard_data, 
                             from_history=False)
    
    return render_template('upload.html')

def prepare_display_data(df_raw):
    """
    Fungsi terpisah untuk memformat data untuk display
    Input: DataFrame dengan data numeric mentah  
    Output: DataFrame dengan data formatted untuk tampilan
    """
    df_display = df_raw.copy()
    
    # FIXED: Ensure bulan_iso is properly formatted for filtering
    if 'bulan_iso' not in df_display.columns or df_display['bulan_iso'].isna().all():
        # Generate bulan_iso from bulan if missing
        if 'bulan' in df_display.columns:
            print("DEBUG: Generating bulan_iso from bulan column")
            
            def generate_bulan_iso(bulan_val):
                if pd.isna(bulan_val) or bulan_val == '' or bulan_val == '-':
                    return None
                
                bulan_str = str(bulan_val).strip().lower()
                tahun = 2025  # Default tahun
                
                bulan_mapping = {
                    'januari': '01', 'jan': '01',
                    'februari': '02', 'feb': '02', 
                    'maret': '03', 'mar': '03',
                    'april': '04', 'apr': '04',
                    'mei': '05', 'may': '05',
                    'juni': '06', 'jun': '06',
                    'juli': '07', 'jul': '07',
                    'agustus': '08', 'agu': '08', 'aug': '08',
                    'september': '09', 'sep': '09',
                    'oktober': '10', 'okt': '10', 'oct': '10',
                    'november': '11', 'nov': '11',
                    'desember': '12', 'des': '12', 'dec': '12'
                }
                
                # Check if already ISO format (YYYY-MM)
                if re.match(r'^\d{4}-\d{2}$', bulan_str):
                    return bulan_str
                
                # Check if YY-MM format  
                if re.match(r'^\d{2}-\d{2}$', bulan_str):
                    year, month = bulan_str.split('-')
                    return f"20{year}-{month}"
                
                # Convert month name to ISO
                month_num = bulan_mapping.get(bulan_str)
                if month_num:
                    return f"{tahun}-{month_num}"
                
                return None
            
            df_display['bulan_iso'] = df_display['bulan'].apply(generate_bulan_iso)
            print(f"DEBUG: Generated bulan_iso for {df_display['bulan_iso'].notna().sum()} records")
    
    # Clean bulan_iso values - remove 'None', 'nan', empty strings
    if 'bulan_iso' in df_display.columns:
        df_display['bulan_iso'] = df_display['bulan_iso'].apply(
            lambda x: None if pd.isna(x) or str(x).lower() in ['none', 'nan', '', '-'] else x
        )
        print(f"DEBUG: After cleaning, bulan_iso has {df_display['bulan_iso'].notna().sum()} valid values")
    
    # Format tanggal pembayaran
    if 'tanggal_pembayaran' in df_display.columns:
        df_display['tanggal_pembayaran'] = df_display['tanggal_pembayaran'].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else '-'
        )
        print("DEBUG: Formatted tanggal_pembayaran for display")
    
    # Format growth menjadi persen
    if 'growth' in df_display.columns:
        df_display['growth'] = df_display['growth'].apply(
            lambda x: f"{x*100:.2f}%" if pd.notna(x) else '-'
        )
        print("DEBUG: Formatted growth for display as percentage")
    
    # Format omset dan pajak dengan format_currency
    if 'omset_perbulan' in df_display.columns:
        df_display['omset_perbulan'] = df_display['omset_perbulan'].apply(format_currency)
        print("DEBUG: Formatted omset_perbulan for display")
    
    if 'jumlah_pajak_dibayar' in df_display.columns:
        df_display['jumlah_pajak_dibayar'] = df_display['jumlah_pajak_dibayar'].apply(format_currency)
        print("DEBUG: Formatted jumlah_pajak_dibayar for display")
    
    # Clean text columns
    text_columns = ['nama_usaha', 'jenis_pajak_usaha', 'npwpd', 'nopd', 'id_usaha', 'bulan']
    for col in text_columns:
        if col in df_display.columns:
            df_display[col] = df_display[col].astype(str).str.strip().replace('nan', '-')
    
    # Handle remaining NaN values EXCEPT bulan_iso (keep None for filtering logic)
    fill_cols = [col for col in df_display.columns if col != 'bulan_iso']
    df_display[fill_cols] = df_display[fill_cols].fillna('-')
    
    return df_display

@app.route('/riwayat')
def riwayat():
    file_list = fetch_file_list()
    return render_template('riwayat.html', file_list=file_list)

# Tambahkan fungsi helper untuk format angka di bagian atas file
def format_currency(value):
    """
    Format angka untuk tampilan yang lebih bersih
    """
    if pd.isna(value) or value == '' or value == '-' or value is None:
        return '-'
    
    try:
        num_value = float(value)
        if num_value == 0:
            return '-'
        
        # Jika angka adalah bilangan bulat, tampilkan tanpa desimal
        if num_value.is_integer():
            return f"{int(num_value):,}"
        else:
            # Jika ada desimal, tampilkan dengan 2 digit desimal
            return f"{num_value:,.2f}"
            
    except (ValueError, TypeError):
        return str(value)

@app.route('/riwayat/<batch_id>')
def riwayat_detail(batch_id):
    """
    Route untuk menampilkan detail data historis dengan sistem atribut fleksibel
    """
    try:
        # Ambil data dari database
        df = fetch_by_batch_flexible(batch_id)
        
        if df.empty:
            print(f"WARNING: No data found for batch_id: {batch_id}")
            return render_template('result.html', 
                                 data=[], columns=[], 
                                 column_display_mapping={},
                                 dashboard_data={}, from_history=True, 
                                 error="Data tidak ditemukan")
        
        print(f"\n=== PROCESSING HISTORICAL DATA FOR BATCH: {batch_id} ===")
        print(f"DEBUG: Initial shape={df.shape}")
        print(f"DEBUG: Columns={list(df.columns)}")
        
        # ===== STEP 1: VALIDASI DATA HISTORIS =====
        config = DataAttributeConfig()
        
        # Cek apakah data historis memiliki kolom yang diperlukan
        # Jika tidak lengkap, coba mapping dari kolom yang ada
        try:
            df_validated, missing_required, mapped_columns = validate_required_columns(df, config)
            
            # Jika ada kolom yang hilang, coba buat dari kolom lain
            for missing_col in missing_required.copy():
                if missing_col == 'nopd' and 'id_usaha' in df.columns:
                    df['nopd'] = df['id_usaha']
                    missing_required.remove(missing_col)
            
            if missing_required:
                print(f"WARNING: Historical data missing required columns: {missing_required}")
                # Tetap lanjut tapi dengan limited functionality
                
        except Exception as e:
            print(f"WARNING: Column validation failed for historical data: {e}")
            df_validated = df.copy()
        
        # ===== STEP 2: CLEANING HISTORICAL DATA =====
        # Clean text columns
        text_columns = ['nama_usaha', 'bulan', 'nopd', 'npwpd', 'id_usaha']
        for col in text_columns:
            if col in df_validated.columns:
                df_validated[col] = clean_text_data(df_validated[col])
        
        # Convert numeric columns
        numeric_columns = ['omset_perbulan', 'jumlah_pajak_dibayar', 'growth']
        for col in numeric_columns:
            if col in df_validated.columns:
                df_validated[col] = pd.to_numeric(df_validated[col], errors='coerce')
        
        # Convert date columns
        if 'tanggal_pembayaran' in df_validated.columns:
            df_validated['tanggal_pembayaran'] = pd.to_datetime(df_validated['tanggal_pembayaran'], errors='coerce')
        
        # ===== STEP 3: GENERATE MISSING COLUMNS IF NEEDED =====
        # Jika ada kolom tambahan yang hilang, generate ulang
        if 'omset_perbulan' not in df_validated.columns and 'jumlah_pajak_dibayar' in df_validated.columns:
            df_validated['omset_perbulan'] = df_validated['jumlah_pajak_dibayar'].apply(
                lambda x: x * 10 if pd.notna(x) and x > 0 else None
            )
        
        # ===== STEP 4: CALCULATE DASHBOARD =====
        dashboard_data = calculate_dashboard_metrics(df_validated)
        
        # ===== STEP 5: PREPARE DISPLAY =====
        df_display = prepare_display_data(df_validated)
        
        # Use config for display columns
        show_cols = get_display_columns(df_display, config)
        column_display_mapping = get_column_display_mapping(config)
        
        # Prepare data dengan styling
        data = []
        for _, row in df_display[show_cols].iterrows():
            row_dict = row.to_dict()
            if 'kondisi' in row_dict:
                row_dict['kondisi_style'] = color_kondisi(row_dict['kondisi'])
            data.append(row_dict)
        
        print(f"DEBUG: Prepared {len(data)} records for display")
        print(f"=== HISTORICAL DATA PROCESSING COMPLETED ===\n")
        
        return render_template('result.html', 
                             data=data, 
                             columns=show_cols, 
                             column_display_mapping=column_display_mapping,
                             dashboard_data=dashboard_data, 
                             from_history=True)
    
    except Exception as e:
        print(f"ERROR in riwayat_detail for batch {batch_id}: {e}")
        import traceback
        traceback.print_exc()
        
        return render_template('result.html', 
                             data=[], 
                             columns=[], 
                             column_display_mapping={},
                             dashboard_data={'total_usaha': 0,'persentase_patuh': 0,'total_omset': 0,'jumlah_anomali': 0,'status_counts': {},'monthly_trend': []}, 
                             from_history=True,
                             error=f"Terjadi error saat memproses data: {str(e)}")       

        
@app.route('/hapus/<batch_id>', methods=['POST'])
def hapus_batch(batch_id):
    """
    Hapus batch tertentu - menggunakan fungsi dari db.py
    """
    try:
        print(f"DEBUG: Attempting to delete batch: {batch_id}")
        affected_rows = delete_batch(batch_id)
        print(f"DEBUG: Successfully deleted {affected_rows} rows for batch {batch_id}")
        
        if affected_rows == 0:
            print(f"WARNING: No rows found for batch_id: {batch_id}")
        
    except Exception as e:
        print(f"ERROR deleting batch {batch_id}: {e}")
        # Bisa tambah flash message atau redirect dengan error
    
    return redirect(url_for('riwayat'))

@app.route('/hapus-semua-riwayat', methods=['POST'])
def hapus_semua_riwayat():
    """
    Hapus semua riwayat - menggunakan fungsi dari db.py dengan error handling
    """
    try:
        print("DEBUG: hapus_semua_riwayat function called")
        
        # Gunakan fungsi yang sudah ada di db.py
        affected_rows = delete_all_history()
        
        print(f"DEBUG: Successfully deleted {affected_rows} rows from riwayat table")
        
        if affected_rows == 0:
            print("WARNING: No rows were deleted - table might already be empty")
        
    except Exception as e:
        print(f"ERROR in hapus_semua_riwayat: {e}")
        import traceback
        traceback.print_exc()
        
        # Optional: bisa tambah flash message untuk user
        # flash(f"Terjadi error saat menghapus riwayat: {e}", "error")
    
    return redirect(url_for('riwayat'))

if __name__ == '__main__':
    app.run(debug=True)