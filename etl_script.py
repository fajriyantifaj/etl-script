import pandas as pd
import psycopg2
from sqlalchemy import create_engine
pd.set_option('future.no_silent_downcasting', True)

file_path = r'path excel'
year = 'year-' #Ganti sesuai tahun e.g. 2023-
sheet_name = 'MONTH' #Ganti sesuai nama sheet a.k.a bulan e.g. DESEMBER
keyword = 'SISA AKHIR'

def extract(file_path, sheet_name):
    data = pd.read_excel(file_path, sheet_name=sheet_name)
    df = pd.DataFrame(data)
    return df

# Cari index kolom terakhir: kata kunci/keyword pada kolom terakhir -> 'SISA AKHIR'
def last_column(df, keyword):
    for i, column in enumerate(df.columns):
        if df[column].astype(str).str.contains(keyword, case=False, na=False).any():
            return i
    return -1  # Jika tidak ditemukan

def transform(df):
    last_index = last_column(df, keyword)
    if last_index != -1:
        # Potong DataFrame dari awal hingga kolom yang ditemukan
        df = df.iloc[:, :last_index+1]
    else:
        print(f"Kata kunci '{keyword}' tidak ditemukan dalam isi kolom manapun.")
        return None

    # Drop semua baris sebelum ada angka 1 pada kolom pertama
    df = df[df.iloc[:, 0].eq(1).cummax()]

    # Mengganti nama kolom sesuai nama asli pada raw data
    df.columns.values[1:6] = ['Nama Obat', 'Satuan', 'Stok Awal', 'Penerimaan', 'Persediaan']

    # Mengganti nama kolom 'nan' di akhir
    df.columns.values[-3:] = ['Umum', 'BPJS', 'Sisa Akhir']

    # Memilih kolom dan menggabungkannya menjadi satu; 5 kolom awal dan 3 kolom terakhir
    df = pd.concat([df.iloc[:, 1:6], df.iloc[:, -3:]], axis=1)
    df[df.columns[3:8]] = df[df.columns[3:8]].astype(float)

    # month based on the sheet name
    months = {'JANUARI': '01', 'FEBRUARI': '02', 'MARET': '03', 'APRIL': '04', 'MEI': '05', 'JUNI': '06',
              'JULI': '07', 'AGUSTUS': '08', 'SEPTEMBER': '09', 'OKTOBER': '10', 'NOVEMBER': '11', 'DESEMBER': '12'}
    
    month = months.get(sheet_name)
    if month is None:
        print('Month Error')
    else:
        # Buat kolom baru 'Year-Month' pada kolom awal
        year_month = year + month
        df.insert(0, 'Year-Month', year_month)

    # Insert kolom baru sebagai kolom awal/posisi 0 dan 8
    total_pemakaian = df['Umum'] + df['BPJS']
    df.insert(8, 'Total Pemakaian', total_pemakaian)

    df.dropna(subset=['Nama Obat'], inplace=True)
    df.iloc[:, 3:] = df.iloc[:, 3:].fillna(0)

    return df

def load(df):
    # Buat SQLAlchemy engine
    engine = create_engine('postgresql+psycopg2://postgres:password@localhost:5432/db_sediaan_obat')
    # Insert data ke PostgreSQL table
    df.to_sql('stok_obat', engine, if_exists='append', index=False)
    # SQL query
    query = "SELECT * FROM stok_obat"
    # Gunakan pandas untuk mengeksekusi query dan membaca hasilnya ke dalam DataFrame
    load_data = pd.read_sql_query(query, engine)

    return load_data.tail()

def main():
    # Extract
    df = extract(file_path, sheet_name)
    # Transform
    df_transformed = transform(df)
    # Load
    result = load(df_transformed)
    
    print("Data berhasil diproses dan dimasukkan ke database.")
    print("Beberapa baris terakhir dari data yang dimasukkan:")
    print(result)

if __name__ == "__main__":
    main()

