# db.py

import psycopg2
import os
import uuid
import pandas as pd
from datetime import datetime

DB_PARAMS = {
    'dbname': 'tren_pajak',
    'user': 'postgres',
    'password': 'Bismillah17',
    'host': 'localhost',
    'port': '5432'
}

def get_connection():
    return psycopg2.connect(**DB_PARAMS)

def insert_history_flexible(df, filename):
    """
    FIXED: Insert data ke database dengan penanganan tipe data yang benar
    """
    conn = get_connection()
    cursor = conn.cursor()
    batch_id = str(uuid.uuid4())
    
    print(f"DEBUG INSERT: Processing {len(df)} rows for batch_id: {batch_id}")
    print(f"DEBUG INSERT: Input columns: {list(df.columns)}")
    
    # Definisikan kolom yang akan disimpan ke database
    db_column_mapping = {
        'nopd': 'id_usaha',
        'nama_usaha': 'nama_usaha',
        'bulan': 'bulan',
        'omset_perbulan': 'omset_perbulan',
        'jumlah_pajak_dibayar': 'jumlah_pajak_dibayar',
        'tanggal_pembayaran': 'tanggal_pembayaran',
        'status': 'status',
        'growth': 'growth',
        'kondisi': 'kondisi'
    }
    
    # Buat list kolom yang tersedia dari DataFrame
    available_data_columns = []
    available_db_columns = []
    
    for config_col, db_col in db_column_mapping.items():
        if config_col in df.columns:
            available_data_columns.append(config_col)
            available_db_columns.append(db_col)
    
    print(f"DEBUG INSERT: Saving columns to DB: {available_db_columns}")
    
    # Build dynamic INSERT query
    placeholders = ', '.join(['%s'] * len(available_db_columns))
    columns_str = ', '.join(available_db_columns)
    
    insert_query = f"""
        INSERT INTO riwayat ({columns_str}, filename, batch_id, timestamp)
        VALUES ({placeholders}, %s, %s, %s)
    """
    
    print(f"DEBUG INSERT: Query: {insert_query}")

    success_count = 0
    error_count = 0
    
    for index, row in df.iterrows():
        try:
            # Prepare values untuk kolom yang tersedia
            values = []
            
            for config_col in available_data_columns:
                raw_value = row.get(config_col)
                
                # FIXED: Handle different data types dengan benar
                if config_col == 'tanggal_pembayaran':
                    # Handle datetime properly
                    if pd.isna(raw_value) or str(raw_value).strip() in ['', '-', 'nan', 'None']:
                        values.append(None)
                    elif isinstance(raw_value, pd.Timestamp):
                        values.append(raw_value.date())  # Convert to date object
                    elif isinstance(raw_value, str) and raw_value != '-':
                        try:
                            # Try to parse string to datetime
                            parsed_date = pd.to_datetime(raw_value, errors='coerce')
                            if pd.notna(parsed_date):
                                values.append(parsed_date.date())
                            else:
                                values.append(None)
                        except:
                            values.append(None)
                    else:
                        values.append(None)
                        
                elif config_col in ['omset_perbulan', 'jumlah_pajak_dibayar', 'growth']:
                    # Handle numeric columns
                    if pd.isna(raw_value) or str(raw_value).strip() in ['', '-', 'nan', 'None']:
                        values.append(None)
                    else:
                        try:
                            numeric_value = float(raw_value)
                            values.append(numeric_value)
                        except (ValueError, TypeError):
                            values.append(None)
                            
                else:
                    # Handle string columns (nama_usaha, bulan, status, kondisi, nopd)
                    if pd.isna(raw_value) or str(raw_value).strip() in ['nan', 'None', '']:
                        values.append(None)
                    else:
                        values.append(str(raw_value).strip())
            
            # Tambahkan filename, batch_id, timestamp
            values.extend([filename, batch_id, datetime.now()])
            
            cursor.execute(insert_query, values)
            success_count += 1
            
        except Exception as e:
            print(f"ERROR inserting row {index}: {e}")
            print(f"Row data: {row.to_dict()}")
            print(f"Values: {values}")
            error_count += 1
            continue

    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"DEBUG INSERT: Successfully inserted {success_count} rows, {error_count} errors")
    print(f"DEBUG INSERT: Batch ID: {batch_id}")
    
    return batch_id

def fetch_by_batch_flexible(batch_id):
    """
    FIXED: Fetch data dari database dengan error handling yang lebih baik
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    print(f"DEBUG FETCH: Fetching data for batch_id: {batch_id}")
    
    try:
        # Query dengan SELECT * untuk mengambil semua kolom yang ada
        cursor.execute("""
            SELECT id_usaha, nama_usaha, bulan, omset_perbulan, 
                   jumlah_pajak_dibayar, tanggal_pembayaran, 
                   status, growth, kondisi 
            FROM riwayat
            WHERE batch_id = %s
            ORDER BY id_usaha, CASE 
                WHEN bulan ~ '^\\d{4}-\\d{2}$' THEN bulan
                ELSE '9999-99' 
            END
        """, (batch_id,))

        rows = cursor.fetchall()
        
        # Get column names from cursor description
        column_names = [desc[0] for desc in cursor.description]
        
        print(f"DEBUG FETCH: Found {len(rows)} rows with columns: {column_names}")
        
    except Exception as e:
        print(f"ERROR in fetch query: {e}")
        rows = []
        column_names = []
    finally:
        cursor.close()
        conn.close()

    if not rows:
        print(f"DEBUG FETCH: No data found for batch_id: {batch_id}")
        return pd.DataFrame()
    
    df = pd.DataFrame(rows, columns=column_names)
    
    # FIXED: Mapping database column names ke config names yang benar
    db_to_config_mapping = {
        'id_usaha': 'nopd',
        'nama_usaha': 'nama_usaha',
        'bulan': 'bulan',
        'omset_perbulan': 'omset_perbulan',
        'jumlah_pajak_dibayar': 'jumlah_pajak_dibayar',
        'tanggal_pembayaran': 'tanggal_pembayaran',
        'status': 'status',
        'growth': 'growth',
        'kondisi': 'kondisi'
    }
    
    # Rename kolom sesuai config
    for db_col, config_col in db_to_config_mapping.items():
        if db_col in df.columns and db_col != config_col:
            df = df.rename(columns={db_col: config_col})
    
    print(f"DEBUG FETCH: Final dataframe shape: {df.shape}")
    print(f"DEBUG FETCH: Final columns: {list(df.columns)}")
    
    return df

def create_table_if_not_exists():
    """
    FIXED: Buat tabel dengan struktur PostgreSQL yang benar
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    # Drop dan buat ulang tabel untuk memastikan struktur yang benar
    create_table_query = """
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
    """
    
    # Index terpisah - FIXED untuk PostgreSQL
    index_queries = [
        "CREATE INDEX IF NOT EXISTS idx_riwayat_batch_id ON riwayat(batch_id);",
        "CREATE INDEX IF NOT EXISTS idx_riwayat_id_usaha ON riwayat(id_usaha);",
        "CREATE INDEX IF NOT EXISTS idx_riwayat_timestamp ON riwayat(timestamp);"
    ]
    
    try:
        cursor.execute(create_table_query)
        print("DEBUG: Table 'riwayat' created or verified successfully")
        
        # Buat index
        for index_query in index_queries:
            try:
                cursor.execute(index_query)
            except Exception as idx_error:
                print(f"DEBUG: Index creation info: {idx_error}")
        
        conn.commit()
        print("DEBUG: All indexes created or verified successfully")
        
    except Exception as e:
        print(f"ERROR: Table/index creation failed: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def fetch_file_list():
    """Fetch list file dengan error handling"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT DISTINCT ON (batch_id) filename, batch_id, timestamp
            FROM riwayat
            ORDER BY batch_id, timestamp DESC
        """)
        results = cursor.fetchall()
        
        print(f"DEBUG: Found {len(results)} files in history")
        
        return [{'filename': row[0], 'batch_id': row[1]} for row in results]
        
    except Exception as e:
        print(f"ERROR fetching file list: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def delete_all_history():
    """Hapus semua data riwayat"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM riwayat")
        affected_rows = cursor.rowcount
        conn.commit()
        print(f"DEBUG: Deleted {affected_rows} rows from riwayat")
        return affected_rows
    except Exception as e:
        print(f"ERROR deleting all history: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()

def delete_batch(batch_id):
    """Hapus data berdasarkan batch_id tertentu"""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM riwayat WHERE batch_id = %s", (batch_id,))
        affected_rows = cursor.rowcount
        conn.commit()
        print(f"DEBUG: Deleted {affected_rows} rows for batch_id: {batch_id}")
        return affected_rows
    except Exception as e:
        print(f"ERROR deleting batch {batch_id}: {e}")
        conn.rollback()
        return 0
    finally:
        cursor.close()
        conn.close()

# DEBUGGING FUNCTION - Tambahan untuk troubleshooting
def debug_batch_data(batch_id):
    """
    Fungsi debugging untuk melihat data mentah di database
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT id, id_usaha, nama_usaha, bulan, omset_perbulan, 
                   jumlah_pajak_dibayar, tanggal_pembayaran, status, growth, kondisi,
                   filename, batch_id, timestamp
            FROM riwayat
            WHERE batch_id = %s
            LIMIT 5
        """, (batch_id,))
        
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        
        print(f"\n=== DEBUG BATCH DATA: {batch_id} ===")
        print(f"Found {len(rows)} rows")
        print(f"Columns: {column_names}")
        
        for i, row in enumerate(rows):
            print(f"Row {i+1}: {dict(zip(column_names, row))}")
        
        print("=== END DEBUG ===\n")
        
    except Exception as e:
        print(f"ERROR in debug_batch_data: {e}")
    finally:
        cursor.close()
        conn.close()

# Initialize database saat import
if __name__ == "__main__":
    create_table_if_not_exists()