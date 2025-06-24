import os
import logging
import time
import urllib.parse
import multiprocessing as mp
import pandas as pd
import gzip
import csv
import shutil
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

# File paths
csv_file_path = 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/wayfair_admarketplace.csv'
tsv_file_path = csv_file_path.replace('.csv', '.tsv')
output_cleaned_file_path = tsv_file_path.replace('.tsv', '_cleaned.tsv')
updated_output_file_path = os.path.join(os.path.dirname(tsv_file_path), 'amp_klarna_wayfair_updated.tsv.gz')

# Column mapping for renaming
column_mapping = {
    "id": "SKU/id",
    "item_group_id": "GroupId",
    "title": "Name",
    "brand": "Manufacturer",
    "link": "URL",
    "price": "Price",
    "sale_price": "Sale Price",
    "description": "Description",
    "image_link": "Image URL",
    "mpn": "Manufacturer SKU / MPN",
    "gtin": "EAN/GTIN",
    "availability": "Stock status",
    "condition": "Condition",
    "product_type": "Category",
    "google_product_category": "Shipping costs",
    "gender": "Gender",
    "age_group": "AgeGroup",
    "color": "Color",
    "size": "Size",
    "material": "Material",
    "pattern": "Pattern",
    "multipack": "Multipack"
}

# Optimized chunk size
chunksize = 2000000  # Process 500,000 rows at a time for speed
num_workers = max(2, mp.cpu_count() - 2)  # Ensure at least 2 workers for parallelism  # Use all but 2 CPU cores

# Function to remove double quotes from dataframe values
def remove_double_quotes(chunk):
    return chunk.map(lambda x: x.replace('"', '') if isinstance(x, str) else x)

# Step 1: Convert CSV to TSV and remove double quotes
start_time = time.time()
total_lines = sum(1 for _ in open(csv_file_path, 'r', encoding='utf-8', errors='ignore')) - 1  # Excluding header
with pd.read_csv(csv_file_path, sep=',', quotechar='"', quoting=csv.QUOTE_ALL, low_memory=False, dtype=str, chunksize=chunksize) as reader:
    with tqdm(total=total_lines, desc="Converting CSV to TSV", unit="rows") as pbar:
        for i, chunk in enumerate(reader):
            chunk = remove_double_quotes(chunk)
            if i == 0:
                chunk.to_csv(tsv_file_path, sep='\t', index=False, mode='w', quoting=csv.QUOTE_NONE, escapechar='\\')
            else:
                chunk.to_csv(tsv_file_path, sep='\t', index=False, mode='a', header=False, quoting=csv.QUOTE_NONE, escapechar='\\')
            pbar.update(len(chunk))
logging.info(f"CSV has been converted to TSV, cleaned file saved at: {output_cleaned_file_path}")

# Processing TSV file to modify links
file_path = tsv_file_path
output_file_path = os.path.join(os.path.dirname(file_path), 'amp_klarna_wayfair.tsv.gz')
base_url = 'https://klarnashoppingads.ampxdirect.com/?partner=klarnashoppingads&sub1=shoppingads&ctaid=74894&v=1.3&source=als_tiles&match-method=deterministic'

def create_new_link(original_link):
    clean_link = original_link.split('?')[0]
    encoded_link = urllib.parse.quote_plus(clean_link)
    return f"{base_url}&cu={encoded_link}&fbu={encoded_link}"

def process_chunk(chunk):
    if 'link' in chunk.columns:
        chunk['link'] = chunk['link'].astype(str).fillna('').apply(create_new_link)
    return chunk

def process_file_parallel(file_path, output_file_path, chunksize=500000, num_workers=num_workers):
    from multiprocessing import Pool
    pool = Pool(processes=num_workers, maxtasksperchild=5)
    
    with pd.read_csv(file_path, sep='\t', low_memory=False, dtype=str, chunksize=chunksize, on_bad_lines='skip') as reader:
        total_chunks = sum(1 for _ in open(file_path, 'r')) // chunksize
        with tqdm(total=total_chunks, desc="Processing Links", unit="chunks") as pbar:
            results = pool.map(process_chunk, list(reader))
            with gzip.open(output_file_path, 'wt', encoding='utf-8') as gz_file:
                first_chunk = True
                for df in results:
                    df.to_csv(gz_file, sep='\t', index=False, mode='w' if first_chunk else 'a', header=first_chunk)
                    first_chunk = False
                    pbar.update(1)
    
    pool.close()
    pool.join()

process_file_parallel(file_path, output_file_path)
logging.info(f"File with updated links has been saved as {output_file_path}")

# Step 3: Rename columns in the compressed TSV file
try:
    temp_file = updated_output_file_path + ".tmp"
    with gzip.open(output_file_path, 'rb') as f_in, gzip.open(temp_file, 'wb') as f_out:
        first_line = f_in.readline().decode('utf-8').strip()
        if not first_line:
            raise ValueError("Error: File is empty or header is missing!")
        old_columns = first_line.split("\t")
        updated_header = "\t".join([column_mapping.get(col, col) for col in old_columns]) + "\n"
        f_out.write(updated_header.encode('utf-8'))
        shutil.copyfileobj(f_in, f_out, length=1024 * 1024)
    os.replace(temp_file, updated_output_file_path)
    logging.info(f"✅ Header updated successfully! New file saved as: {updated_output_file_path}")
except Exception as e:
    logging.error(f"❌ Error occurred: {e}")
    if os.path.exists(temp_file):
        os.remove(temp_file)

end_time = time.time()
logging.info(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    pass

