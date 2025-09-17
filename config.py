# config.py

class DataAttributeConfig:
    """
    Konfigurasi fleksibel untuk atribut data pajak
    Berdasarkan kategorisasi: Required, Optional, Additional
    """
    
    # KOLOM REQUIRED - Wajib ada, sistem error jika tidak ada
    REQUIRED_COLUMNS = {
        'nopd': {
            'aliases': ['nopd', 'id_usaha', 'npwpd'],  # Alternatif nama kolom
            'type': 'string',
            'description': 'ID unik wajib pajak'
        },
        'nama_usaha': {
            'aliases': ['nama_usaha', 'nama', 'business_name'],
            'type': 'string',
            'description': 'Nama perusahaan/usaha'
        },
        'bulan': {
            'aliases': ['bulan', 'periode', 'month', 'bulan_iso'],
            'type': 'string',
            'description': 'Periode pajak yang diparsing'
        },
        'jumlah_pajak_dibayar': {
            'aliases': ['jumlah_pajak_dibayar', 'pajak_dibayar', 'pajak', 'tax_paid'],
            'type': 'numeric',
            'description': 'Nominal pajak yang dibayar'
        }
    }
    
    # KOLOM OPSIONAL - Tidak ditampilkan di output, tapi boleh ada/tidak ada
    OPTIONAL_HIDDEN_COLUMNS = {
        'jenis_pajak_usaha': {
            'aliases': ['jenis_pajak_usaha', 'jenis_pajak', 'tax_type'],
            'type': 'string',
            'description': 'Jenis pajak usaha (ada di data tapi tidak ditampilkan)'
        },
        'npwpd': {
            'aliases': ['npwpd', 'tax_number'],
            'type': 'string',
            'description': 'NPWPD (ada di data tapi tidak ditampilkan)'
        }
    }
    
    # KOLOM OPSIONAL - Ditampilkan di output jika ada
    OPTIONAL_DISPLAY_COLUMNS = {
        'tanggal_pembayaran': {
            'aliases': ['tanggal_pembayaran', 'payment_date', 'tgl_bayar'],
            'type': 'date',
            'description': 'Tanggal pembayaran (ditampilkan jika ada)'
        }
    }
    
    # KOLOM TAMBAHAN - Dihitung/digenerate otomatis
    ADDITIONAL_COLUMNS = {
        'omset_perbulan': {
            'calculation': lambda pajak: pajak * 10 if pajak and pajak > 0 else None,
            'type': 'numeric',
            'description': 'Omset dihitung dari pajak × 10 (asumsi pajak 10% dari omset)'
        },
        'status': {
            'calculation': 'validate_payment_status',  # Fungsi terpisah
            'type': 'string',
            'description': 'Status pembayaran (VALID/TIDAK VALID)'
        },
        'kondisi': {
            'calculation': 'detect_condition',  # Fungsi terpisah
            'type': 'string', 
            'description': 'Kondisi pembayaran (NORMAL/ANOMALI/TIDAK TAAT PAJAK)'
        }
    }
    
    # KOLOM OUTPUT - Urutan tampilan di tabel hasil
    OUTPUT_COLUMN_ORDER = [
        'nopd',
        'nama_usaha', 
        'bulan',
        'omset_perbulan',
        'jumlah_pajak_dibayar',
        'tanggal_pembayaran',  # Optional
        'status',
        'kondisi'
    ]
    
    # MAPPING DISPLAY NAMES
    DISPLAY_NAMES = {
        'nopd': 'NOPD',
        'nama_usaha': 'NAMA USAHA',
        'bulan': 'BULAN', 
        'omset_perbulan': 'OMSET PERBULAN',
        'jumlah_pajak_dibayar': 'JUMLAH PAJAK DIBAYAR',
        'tanggal_pembayaran': 'TANGGAL PEMBAYARAN',
        'status': 'STATUS',
        'kondisi': 'KONDISI'
    }


def validate_required_columns(df, config=None):
    """
    Validasi kolom required dan mapping otomatis dari aliases
    """
    if config is None:
        config = DataAttributeConfig()
    
    validated_df = df.copy()
    missing_required = []
    mapped_columns = {}
    
    for required_col, settings in config.REQUIRED_COLUMNS.items():
        found = False
        
        # Cari kolom dengan nama atau alias yang cocok
        for alias in settings['aliases']:
            if alias in validated_df.columns:
                # Rename ke nama standar jika perlu
                if alias != required_col:
                    validated_df = validated_df.rename(columns={alias: required_col})
                    mapped_columns[alias] = required_col
                found = True
                break
        
        if not found:
            missing_required.append(required_col)
    
    return validated_df, missing_required, mapped_columns


def map_optional_columns(df, config=None):
    """
    Mapping kolom opsional jika ada (tanpa error jika tidak ada)
    """
    if config is None:
        config = DataAttributeConfig()
    
    mapped_df = df.copy()
    found_optional = {'hidden': [], 'display': []}
    
    # Map optional hidden columns
    for opt_col, settings in config.OPTIONAL_HIDDEN_COLUMNS.items():
        for alias in settings['aliases']:
            if alias in mapped_df.columns and alias != opt_col:
                mapped_df = mapped_df.rename(columns={alias: opt_col})
                found_optional['hidden'].append(opt_col)
                break
        else:
            if opt_col in mapped_df.columns:
                found_optional['hidden'].append(opt_col)
    
    # Map optional display columns  
    for opt_col, settings in config.OPTIONAL_DISPLAY_COLUMNS.items():
        for alias in settings['aliases']:
            if alias in mapped_df.columns and alias != opt_col:
                mapped_df = mapped_df.rename(columns={alias: opt_col})
                found_optional['display'].append(opt_col)
                break
        else:
            if opt_col in mapped_df.columns:
                found_optional['display'].append(opt_col)
    
    return mapped_df, found_optional