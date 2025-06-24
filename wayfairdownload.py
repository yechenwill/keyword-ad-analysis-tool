import os
import paramiko
import logging
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# SFTP credentials from environment variables
SFTP_HOST = os.getenv('SFTP_HOST', 'ftp3.feedonomics.com')
SFTP_PORT = int(os.getenv('SFTP_PORT', 22))  # Default port for SFTP
USERNAME = os.getenv('SFTP_USERNAME', 'fdx_fa0a5d1e44291')
PASSWORD = os.getenv('SFTP_PASSWORD', '11105d72ca7cce3ed36518e0')

# Advertiser details
ADVERTISERS = {
    'Wayfair': {
        'sftp_path': '/incoming/',
        'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Wayfair/',
        'file_pattern': 'admarketplace_full_catalog_en_us.csv.gz'
    }
}

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_file_modified_today(sftp, file_name):
    try:
        file_attr = sftp.stat(file_name)
        modification_time = datetime.fromtimestamp(file_attr.st_mtime, timezone.utc)
        return modification_time.date() == datetime.now(timezone.utc).date(), modification_time
    except Exception as e:
        logging.error(f"Error checking modification time for {file_name}: {e}")
        return False, None

def is_local_file_modified_today(local_file_path):
    if os.path.exists(local_file_path):
        modification_time = datetime.fromtimestamp(os.path.getmtime(local_file_path))
        return modification_time.date() == datetime.now().date(), modification_time
    return False, None

def download_files():
    start_time = time.time()  # Start execution time measurement
    
    transport = None
    sftp = None
    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=USERNAME, password=PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        current_date = datetime.now().strftime('%Y%m%d')
        
        for advertiser, paths in ADVERTISERS.items():
            sftp_folder = paths['sftp_path']
            local_folder = paths['local_path']
            file_pattern = paths['file_pattern']
            
            input_file_name = file_pattern(current_date) if callable(file_pattern) else file_pattern
            local_file_path = os.path.join(local_folder, input_file_name)

            os.makedirs(local_folder, exist_ok=True)

            local_exists, local_mod_time = is_local_file_modified_today(local_file_path)
            if local_exists:
                logging.info(f"{advertiser}: Local file '{input_file_name}' already exists and was modified on {local_mod_time}. Skipping download.")
                continue
            
            sftp.chdir(sftp_folder)
            
            if not callable(file_pattern):
                sftp_updated_today, sftp_mod_time = is_file_modified_today(sftp, input_file_name)
                if not sftp_updated_today:
                    logging.info(f"{advertiser}: File '{input_file_name}' on SFTP was last modified on {sftp_mod_time}. Skipping download.")
                    continue
            
            try:
                sftp.get(input_file_name, local_file_path)
                logging.info(f"{advertiser}: File downloaded and saved as {local_file_path}")
            except Exception as e:
                logging.error(f"{advertiser}: Failed to download '{input_file_name}'. Error: {e}")
                if os.path.exists(local_file_path):
                    os.remove(local_file_path)
    
    except Exception as e:
        logging.error(f"An error occurred while downloading: {e}")
    
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()
    
    end_time = time.time()  # End execution time measurement
    execution_time = end_time - start_time
    logging.info(f"Total execution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    download_files()