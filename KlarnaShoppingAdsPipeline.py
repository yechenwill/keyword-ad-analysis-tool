#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import pyodbc
import paramiko
import numpy as np
import gzip
import os
import csv
from datetime import datetime, timezone


# In[ ]:


# SFTP credentials and connection details
sftp_host = 'ftp.admarketplace.net'
sftp_port = 22  # Default port for SFTP
username = 'ywang'
password = '123456789'  # Recommend using environment variables for credentials

# Dictionary to store SFTP folder paths, local file paths, and file naming conventions for each advertiser
advertisers = {
    'Bloomingdales': {
        'sftp_path': '/sftp/l_bloomingdales/files/',
        'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Bloomingdales/',
        'file_pattern': lambda date: f'{date}_Bloomingdales_PLA.csv'  # Example: 20241017_Bloomingdales_PLA.csv
    },
    'Verizon': {
        'sftp_path': '/sftp/l_verizon/files/',
        'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Verizon/',
        'file_pattern': 'verizon_devices_admarketplace.csv'  # Static file name
    },
    'BedBathBeyond': {
        'sftp_path': '/sftp/l_BedBathBeyond/files/',  # Assuming BedBathBeyond has a different path
        'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/BedBathBeyond/',
        'file_pattern': lambda date: f'{date}_BedBathAndBeyond_PLA.csv.gz'  # Example: 20241017_bedbathbeyond_feed.csv
    },
    'HarryDavid': {
        'sftp_path': '/sftp/l_HarryDavid/files/',  # Assuming HarryDavid has a different path
        'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/HarryDavid/',
        'file_pattern': 'hd_admarketplace.csv'  # Static file name
    },
    'TommyBahama': {
        'sftp_path': '/sftp/l_Tommybahama/files/',  # Assuming TommyBahama has a different path
        'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/TommyBahama/',
        'file_pattern': lambda date: f'{date}_TommyBahama_PLA.csv'  # Example: 20241017_bedbathbeyond_feed.csv
    },
    'Houzz': {
        'sftp_path': '/sftp/l_houzz/files/pla_feed/',  # Assuming Houzz has a different path
        'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Houzz/',
        'file_pattern': 'houzz_full_catalog.txt.gz'  # Static file name
    },
    'Zappos': {
        'sftp_path': '/sftp/l_Zappos-1/files/',  # Assuming Houzz has a different path
        'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Zappos/',
        'file_pattern': 'zapoos_adsmarketplace.txt.gz'  # Static file name
    },
    'Forever21': {
        'sftp_path': '/sftp/l_Forever21/files/',  # Assuming TommyBahama has a different path
        'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Forever21/',
        'file_pattern': lambda date: f'Forever21_PLA{date}.csv'  # Example: 20241017_bedbathbeyond_feed.csv
    },
    'HomeDepot': {
        'sftp_path': '/sftp/l_homedepot/files/',  # Assuming TommyBahama has a different path
        'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/TheHomeDepot/',
        'file_pattern': lambda date: f'{date}_TheHomeDepot_029A.csv.gz'  # Example: 20241017_bedbathbeyond_feed.csv
    },
    'NewBalance': {
        'sftp_path': '/sftp/l_Newbalance/files/',  # Assuming TommyBahama has a different path
        'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/NewBalance/',
        'file_pattern': lambda date: f'{date}_NewBalance_ PLA.csv'  # Example: 20241017_bedbathbeyond_feed.csv
    }
#     'Nike': {
#         'sftp_path': '/sftp/l_nike/files/',  # Assuming TommyBahama has a different path
#         'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Nike/',
#         'file_pattern': lambda date: f'I_Nike_AllProducts_{date}.txt.gz'  # Example: 20241017_bedbathbeyond_feed.csv
#     }
}

# Helper function to check if a file was modified today
def is_file_modified_today(sftp, file_name):
    file_attr = sftp.stat(file_name)
    modification_time = datetime.fromtimestamp(file_attr.st_mtime, timezone.utc)
    return modification_time.date() == datetime.now(timezone.utc).date(), modification_time

# Helper function to check if the local file already exists and was modified today
def is_local_file_modified_today(local_file_path):
    if os.path.exists(local_file_path):
        modification_time = datetime.fromtimestamp(os.path.getmtime(local_file_path))
        return modification_time.date() == datetime.now().date(), modification_time
    return False, None

# Establish SFTP connection once
try:
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    # Get the current date string
    current_date = datetime.now().strftime('%Y%m%d')

    # Step 1: Download all files from SFTP into local folders
    for advertiser, paths in advertisers.items():
        sftp_folder = paths['sftp_path']
        local_folder = paths['local_path']
        file_pattern = paths['file_pattern']  # Naming convention for the file

        # Generate the file name based on whether it's a callable (lambda) or a static string
        input_file_name = file_pattern(current_date) if callable(file_pattern) else file_pattern
        local_file_path = os.path.join(local_folder, input_file_name)

        # Ensure the local folder exists, but don't create the file yet
        os.makedirs(local_folder, exist_ok=True)

        # Check if the local file exists and was modified today
        local_exists, local_mod_time = is_local_file_modified_today(local_file_path)
        if local_exists:
            print(f"{advertiser}: Local file '{input_file_name}' already exists and was modified on {local_mod_time}. Skipping download.")
            continue

        # Navigate to the advertiser's SFTP directory
        sftp.chdir(sftp_folder)

        # For fixed-name files, check if the file was updated today before downloading
        if not callable(file_pattern):  # For static file names
            sftp_updated_today, sftp_mod_time = is_file_modified_today(sftp, input_file_name)
            if not sftp_updated_today:
                print(f"{advertiser}: File '{input_file_name}' on SFTP was last modified on {sftp_mod_time}. Skipping download.")
                continue  # Skip download if file was not updated today

        try:
            # Download the file from SFTP to the local system (local file will only be created if download is successful)
            sftp.get(input_file_name, local_file_path)
            print(f"{advertiser}: File downloaded from SFTP and saved locally as {local_file_path}")
        except Exception as e:
            print(f"{advertiser}: Failed to download '{input_file_name}'. Error: {e}")
            # Remove any partially created or empty file after download failure
            if os.path.exists(local_file_path):
                os.remove(local_file_path)

except Exception as e:
    print(f"An error occurred while downloading: {e}")

finally:
    if sftp:
        sftp.close()
    if transport:
        transport.close()


# In[20]:


# # SFTP credentials and connection details
# sftp_host = 'sftpgo.feedonomics.com'
# sftp_port = 22  # Default port for SFTP
# username = 'fdx_eb4e950355841'
# password = 'c0dff59b60d30e836cd6a5f0'  # Recommend using environment variables for credentials

# advertisers = {
#     'Spanx': {
#         'sftp_path': '/incoming/',
#         'local_path': 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Spanx/',
#         'file_pattern': 'Spanx_AdMarketplace.csv.gz'  # Example: 20241017_Bloomingdales_PLA.csv
#     }
# }

# # Helper function to check if a file was modified today
# def is_file_modified_today(sftp, file_name):
#     file_attr = sftp.stat(file_name)
#     modification_time = datetime.fromtimestamp(file_attr.st_mtime, timezone.utc)
#     return modification_time.date() == datetime.now(timezone.utc).date(), modification_time

# # Helper function to check if the local file already exists and was modified today
# def is_local_file_modified_today(local_file_path):
#     if os.path.exists(local_file_path):
#         modification_time = datetime.fromtimestamp(os.path.getmtime(local_file_path))
#         return modification_time.date() == datetime.now().date(), modification_time
#     return False, None

# # Establish SFTP connection once
# try:
#     transport = paramiko.Transport((sftp_host, sftp_port))
#     transport.connect(username=username, password=password)
#     sftp = paramiko.SFTPClient.from_transport(transport)

#     # Get the current date string
#     current_date = datetime.now().strftime('%Y%m%d')

#     # Step 1: Download all files from SFTP into local folders
#     for advertiser, paths in advertisers.items():
#         sftp_folder = paths['sftp_path']
#         local_folder = paths['local_path']
#         file_pattern = paths['file_pattern']  # Naming convention for the file

#         # Generate the file name based on whether it's a callable (lambda) or a static string
#         input_file_name = file_pattern(current_date) if callable(file_pattern) else file_pattern
#         local_file_path = os.path.join(local_folder, input_file_name)

#         # Ensure the local folder exists, but don't create the file yet
#         os.makedirs(local_folder, exist_ok=True)

#         # Check if the local file exists and was modified today
#         local_exists, local_mod_time = is_local_file_modified_today(local_file_path)
#         if local_exists:
#             print(f"{advertiser}: Local file '{input_file_name}' already exists and was modified on {local_mod_time}. Skipping download.")
#             continue

#         # Navigate to the advertiser's SFTP directory
#         sftp.chdir(sftp_folder)

#         # For fixed-name files, check if the file was updated today before downloading
#         if not callable(file_pattern):  # For static file names
#             sftp_updated_today, sftp_mod_time = is_file_modified_today(sftp, input_file_name)
#             if not sftp_updated_today:
#                 print(f"{advertiser}: File '{input_file_name}' on SFTP was last modified on {sftp_mod_time}. Skipping download.")
#                 continue  # Skip download if file was not updated today

#         try:
#             # Download the file from SFTP to the local system (local file will only be created if download is successful)
#             sftp.get(input_file_name, local_file_path)
#             print(f"{advertiser}: File downloaded from SFTP and saved locally as {local_file_path}")
#         except Exception as e:
#             print(f"{advertiser}: Failed to download '{input_file_name}'. Error: {e}")
#             # Remove any partially created or empty file after download failure
#             if os.path.exists(local_file_path):
#                 os.remove(local_file_path)

# except Exception as e:
#     print(f"An error occurred while downloading: {e}")

# finally:
#     if sftp:
#         sftp.close()
#     if transport:
#         transport.close()


# # In[ ]:


# import os
# import requests
# from datetime import datetime

# # Specify the folder path
# folder_path = 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Ulta'

# # Generate the current timestamp in 'yyyymmddhhmmss' format for the filename
# current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
# input_file_name = f'feed_{current_timestamp}_33000020.txt'
# output_file_name = 'ulta.tsv'

# # Create the full file paths
# input_file_path = os.path.join(folder_path, input_file_name)
# output_file_path = os.path.join(folder_path, output_file_name)

# # URL for the daily download
# url = "https://webadapters.channeladvisor.com/CSEAdapter/Default.aspx?pid=V%5bP%5e%5eC%5ePAosvB6Z.X%5b3KePQjFGq_%5bZX2%5bLd%22(%3dsFt4%5b%60%26K2Ic%23)gwz%3d7Z%5eY%5bbI_SQ8DLu_U%2f%26%5ebucR(%3cwz"

# # Download the file
# try:
#     response = requests.get(url)
#     response.raise_for_status()
    
#     # Save the downloaded content to the specified input file
#     with open(input_file_path, 'wb') as file:
#         file.write(response.content)
#     print(f"Downloaded file saved as: {input_file_path}")

#     # Read the content from the text file and save it as TSV
#     with open(input_file_path, 'r', encoding='utf-8') as file:
#         content = file.read()
        
#     with open(output_file_path, 'w', encoding='utf-8') as tsv_file:
#         tsv_file.write(content)
    
#     print(f"File has been saved as TSV at: {output_file_path}")

# except requests.HTTPError as e:
#     print(f"HTTP error occurred: {e}")
# except UnicodeDecodeError:
#     print("Error: Could not decode the file. Please check the file encoding or try using a different encoding.")


# # In[ ]:


# import gzip
# import csv

# def unzip_gz_to_csv(gz_file_path, output_csv_file_path):
#     # Open the gz file in text mode with UTF-8 encoding
#     with gzip.open(gz_file_path, 'rt', encoding='utf-8') as gz_file:
#         # Open the output CSV file in write mode
#         with open(output_csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
#             reader = csv.reader(gz_file, delimiter=',')  # Assume it's comma-separated
#             writer = csv.writer(csv_file, delimiter=',')  # Writing CSV format

#             for row in reader:
#                 # Write each row to the CSV file
#                 writer.writerow(row)

# # Example usage
# gz_file_path = 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/BedBathBeyond/20241009_BedBathAndBeyond_PLA.csv.gz'
# output_csv_file_path = 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/BedBathBeyond/BBB_admarketplace.csv'

# unzip_gz_to_csv(gz_file_path, output_csv_file_path)


# # In[ ]:


# import pandas as pd
# import csv

# # Path to your original CSV file
# csv_file_path = 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/BedBathBeyond/BBB_admarketplace.csv'

# # Output TSV file path
# tsv_file_path = csv_file_path.replace('.csv', '.tsv')

# # Output file path for the cleaned TSV (with double quotes removed)
# output_cleaned_file_path = tsv_file_path.replace('.tsv', '_cleaned.tsv')

# # Function to remove double quotes from all values in the dataframe
# def remove_double_quotes(chunk):
#     return chunk.applymap(lambda x: x.replace('"', '') if isinstance(x, str) else x)

# # Step 1: Convert CSV to TSV
# # Read and process the CSV file in chunks
# chunksize = 10000  # Process 10,000 rows at a time
# with pd.read_csv(csv_file_path, sep=',', quotechar='"', quoting=csv.QUOTE_ALL, low_memory=False, dtype=str, chunksize=chunksize) as reader:
#     for i, chunk in enumerate(reader):
#         # Step 2: Remove double quotes from the chunk
#         chunk = remove_double_quotes(chunk)

#         # Step 3: Write the chunk to the TSV file with the escape character set
#         if i == 0:
#             # Write the header for the first chunk
#             chunk.to_csv(tsv_file_path, sep='\t', index=False, mode='w', quoting=csv.QUOTE_NONE, escapechar='\\')
#         else:
#             # Append subsequent chunks without writing the header
#             chunk.to_csv(tsv_file_path, sep='\t', index=False, mode='a', header=False, quoting=csv.QUOTE_NONE, escapechar='\\')

# # Step 4: Confirm that the file has been saved
# print(f"CSV has been converted to TSV, and double quotes have been removed. Cleaned file saved at: {output_cleaned_file_path}")


# # In[ ]:


# import pandas as pd
# import urllib.parse

# # Path to your original TSV file
# file_path = 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/BedBathBeyond/BBB_admarketplace.tsv'

# # Base URL to append
# base_url = 'https://klarnashoppingads.ampxdirect.com/?partner=klarnashoppingads&sub1=shoppingads&ctaid=25116&v=1.3&source=als_tiles&match-method=deterministic'

# # Function to encode the link and append it to the base URL
# def create_new_link(original_link):
#     encoded_link = urllib.parse.quote_plus(original_link)
#     return f"{base_url}&cu={encoded_link}&fbu={encoded_link}"

# # Output file path
# output_file_path = file_path.replace('.tsv', '_final.tsv')

# # Process the TSV file in chunks
# chunksize = 10000  # Process 10,000 rows at a time
# with pd.read_csv(file_path, sep='\t', low_memory=False, dtype=str, on_bad_lines='skip', chunksize=chunksize) as reader:
#     for i, chunk in enumerate(reader):
#         # Ensure the 'Link' column is treated as strings and fill NaN with an empty string
#         if 'Link' in chunk.columns:
#             chunk['Link'] = chunk['Link'].astype(str).fillna('')

#             # Apply the function to create a new link
#             chunk['Link'] = chunk['Link'].apply(create_new_link)

#         # Append the processed chunk to the output file
#         if i == 0:
#             # Write the header for the first chunk
#             chunk.to_csv(output_file_path, sep='\t', index=False, mode='w')
#         else:
#             # Append subsequent chunks without writing the header
#             chunk.to_csv(output_file_path, sep='\t', index=False, mode='a', header=False)

# print(f"File with updated links has been saved as {output_file_path}")


# # In[ ]:


# import pandas as pd
# import gzip

# # Path to your .csv.gz file
# file_path = 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Zappos/zappos_admarketplace.txt.gz'

# # Path to save the extracted TSV file (with a .tsv extension)
# output_file_path = file_path.replace('.txt.gz', '.tsv')

# # Read the .gz file with error handling and specify encoding
# with gzip.open(file_path, 'rt', encoding='utf-8', errors='replace') as file:
#     df = pd.read_csv(file, sep='\t', on_bad_lines='skip')

# # Save the dataframe as a TSV file (without index)
# df.to_csv(output_file_path, sep='\t', index=False)

# print(f"File has been saved as {output_file_path}")


# # In[10]:


# def process_zappos(df):
#     # Step 1: Extract the .gz file and save as .tsv
#     try:
#         # Path to save the extracted TSV file (with a .tsv extension)
#         df = file_path.replace('.txt.gz', '.tsv')

#         # Read the .gz file with error handling and specify encoding
#         with gzip.open(file_path, 'rt', encoding='utf-8', errors='replace') as file:
#             df = pd.read_csv(file, sep='\t', on_bad_lines='skip')

#         # Save the dataframe as a TSV file (without index)
#         df.to_csv(tsv_output_file, sep='\t', index=False)
#         print(f"Extracted file has been saved as {tsv_output_file}")

#         # Step 2: Process the TSV file
#         # Read the TSV file
#         df = pd.read_csv(tsv_output_file, sep='\t', low_memory=False, dtype=str)

#         # Ensure the 'link' column is treated as strings and fill NaN with an empty string
#         df['link'] = df['link'].astype(str).fillna('')

#         # Remove anything after '?' in the 'link' column
#         df['link'] = df['link'].apply(lambda x: x.split('?')[0])

#         # Base URL to append
#         base_url = 'https://klarnashoppingads.ampxdirect.com/?partner=klarnashoppingads&sub1=shoppingads&ctaid=75101&v=1.3&source=als_tiles&match-method=deterministic'

#         # Function to encode the link and append it to the base URL
#         def create_new_link(original_link):
#             encoded_link = urllib.parse.quote_plus(original_link)
#             new_link = f"{base_url}&cu={encoded_link}&fbu={encoded_link}"
#             return new_link

#         # Apply the function to the 'link' column
#         df['link'] = df['link'].apply(create_new_link)

#         # Column renaming based on the required mapping
#         column_mapping = {
#             'sku': 'SKU/id',
#             'title': 'Name',
#             'description': 'Description',
#             'google_product_category': 'Category',
#             'link': 'URL',
#             'image_link': 'Image URL',
#             'condition': 'Condition',
#             'availability': 'Stock status',
#             'price': 'Price',
#             'brand': 'Manufacturer',
#             'gtin': 'EAN/GTIN',
#             'mpn': 'Manufacturer SKU / MPN',
#             'gender': 'Gender',
#             'age_group': 'AgeGroup',
#             'color': 'Color',
#             'size': 'Size',
#             'item_group_id': 'GroupId',
#             'material': 'Material',
#             'pattern': 'Pattern',
#             'shipping': 'Shipping costs'
#         }

#         # Rename columns
#         df.rename(columns=column_mapping, inplace=True)

#         # Ensure that 'EAN/GTIN' and 'SKU/id' are treated as strings and remove any '.0'
#         df['SKU/id'] = df['SKU/id'].astype(str).apply(lambda x: x.rstrip('.0') if '.0' in x else x)
#         df['EAN/GTIN'] = df['EAN/GTIN'].astype(str).apply(lambda x: x.rstrip('.0') if '.0' in x else x)

#         # Step to handle numeric columns that show decimal
#         # Identify columns that can be safely converted to integers, excluding 'EAN/GTIN'
#         numeric_cols = df.columns[df.apply(lambda col: col.str.isnumeric(), axis=0).all()]
#         numeric_cols = numeric_cols.drop('EAN/GTIN', errors='ignore')  # Exclude 'EAN/GTIN'

#         # Convert those columns to integers explicitly
#         df[numeric_cols] = df[numeric_cols].apply(lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(int))

#         # List of missing columns based on the requirements
#         missing_columns = ['AdultContent', 'Delivery time', 'Bundled', 'EnergyEfficiencyClass', 'Multipack', 'SizeSystem']

#         # Add missing columns with empty values or default values
#         for col in missing_columns:
#             df[col] = ''  # Set as empty or default as needed

#         # Save the updated dataframe with renamed columns and new fields
#         final_output_file = tsv_output_file.replace('.tsv', '_final.tsv')
#         df.to_csv(final_output_file, sep='\t', index=False)

#         print(f"File with updated links, renamed columns, and added missing columns has been saved as {final_output_file}")

#     except Exception as e:
#         print(f"Error processing the file: {str(e)}")


# # In[ ]:


# import pandas as pd
# import urllib.parse

# # Path to your original TSV file
# file_path = 'C:/Users/ywang/Documents/Codes/Shopping Ads/Klarna/Zappos/zappos_admarketplace.tsv'

# # Read the TSV file
# df = pd.read_csv(file_path, sep='\t', low_memory=False, dtype=str)

# # Ensure the 'link' column is treated as strings and fill NaN with an empty string
# df['link'] = df['link'].astype(str).fillna('')

# # Remove anything after '?' in the 'link' column
# df['link'] = df['link'].apply(lambda x: x.split('?')[0])

# # Base URL to append
# base_url = 'https://klarnashoppingads.ampxdirect.com/?partner=klarnashoppingads&sub1=shoppingads&ctaid=75101&v=1.3&source=als_tiles&match-method=deterministic'

# # Function to encode the link and append it to the base URL
# def create_new_link(original_link):
#     encoded_link = urllib.parse.quote_plus(original_link)
#     new_link = f"{base_url}&cu={encoded_link}&fbu={encoded_link}"
#     return new_link

# # Apply the function to the 'link' column
# df['link'] = df['link'].apply(create_new_link)

# # Column renaming based on the required mapping
# column_mapping = {
#     'sku': 'SKU/id',
#     'title': 'Name',
#     'description': 'Description',
#     'google_product_category': 'Category',
#     'link': 'URL',
#     'image_link': 'Image URL',
#     'condition': 'Condition',
#     'availability': 'Stock status',
#     'price': 'Price',
#     'brand': 'Manufacturer',
#     'gtin': 'EAN/GTIN',  # Ensuring GTIN remains a string
#     'mpn': 'Manufacturer SKU / MPN',
#     'gender': 'Gender',
#     'age_group': 'AgeGroup',
#     'color': 'Color',
#     'size': 'Size',
#     'item_group_id': 'GroupId',
#     'material': 'Material',
#     'pattern': 'Pattern',
#     'shipping': 'Shipping costs'  # Adjust if this column represents shipping costs
# }

# # Rename columns
# df.rename(columns=column_mapping, inplace=True)

# # Ensure that 'EAN/GTIN' is treated as a string and remove any '.0' from GTIN values
# df['SKU/id'] = df['SKU/id'].astype(str).apply(lambda x: x.rstrip('.0') if '.0' in x else x)
# df['EAN/GTIN'] = df['EAN/GTIN'].astype(str).apply(lambda x: x.rstrip('.0') if '.0' in x else x)

# # Step to handle numeric columns that show decimal
# # Identify columns that can be safely converted to integers, excluding 'EAN/GTIN'
# numeric_cols = df.columns[df.apply(lambda col: col.str.isnumeric(), axis=0).all()]
# numeric_cols = numeric_cols.drop('EAN/GTIN', errors='ignore')  # Exclude 'EAN/GTIN'

# # Convert those columns to integers explicitly
# df[numeric_cols] = df[numeric_cols].apply(lambda x: pd.to_numeric(x, errors='coerce').fillna(0).astype(int))

# # List of missing columns based on the requirements
# missing_columns = ['AdultContent', 'Delivery time', 'Bundled', 'EnergyEfficiencyClass', 'Multipack', 'SizeSystem']

# # Add missing columns with empty values or default values
# for col in missing_columns:
#     df[col] = ''  # Set as empty or default as needed

# # Save the updated dataframe with renamed columns and new fields
# output_file_path = file_path.replace('.tsv', '_final.tsv')
# df.to_csv(output_file_path, sep='\t', index=False)

# print(f"File with updated links, renamed columns, and added missing columns has been saved as {output_file_path}")


# # In[11]:


# def process_bloomingdales(df):
#     # Ensure the 'Link' column is treated as strings and fill NaN with an empty string
#     df['Link'] = df['Link'].astype(str).fillna('')

#     # Base URL to append
#     base_url = 'https://klarnashoppingads.ampxdirect.com/?partner=klarnashoppingads&sub1=shoppingads&ctaid=74022&v=1.3&source=als_tiles&match-method=deterministic'

#     # Function to encode the link and append it to the base URL
#     def create_new_link(original_link):
#         encoded_link = urllib.parse.quote_plus(original_link)
#         new_link = f"{base_url}&cu={encoded_link}&fbu={encoded_link}"
#         return new_link

#     # Apply the function to the 'Link' column
#     df['Link'] = df['Link'].apply(create_new_link)

#     # Column renaming based on the required mapping
#     column_mapping = {
#         'ID': 'SKU/id',
#         'Title': 'Name',
#         'Description': 'Description',
#         'Link': 'URL',
#         'Image Link': 'Image URL',
#         'Condition': 'Condition',
#         'Availability': 'Stock status',
#         'Price': 'Price',
#         'Brand': 'Manufacturer',
#         'GTIN': 'EAN/GTIN',
#         'MPN': 'Manufacturer SKU / MPN',
#         'Gender': 'Gender',
#         'Age Group': 'AgeGroup',
#         'Color': 'Color',
#         'Size': 'Size',
#         'Google Product Category': 'Category',
#         'Sale Price': 'Sale Price',
#         'Sale Price Effective Date': 'Sale Price Effective Date',
#         'Expiration Date': 'Expiration Date',
#         'Mobile Link': 'Mobile Link'
#     }

#     # Rename columns based on the mapping
#     df.rename(columns=column_mapping, inplace=True)

#     # List of columns to convert to integers to remove '.0'
#     columns_to_convert = ['SKU/id', 'EAN/GTIN']

#     # Function to remove '.0' by converting to integer where possible
#     def remove_decimal(value):
#         try:
#             # Try converting to float
#             value_float = float(value)
#             # If the float is an integer (no decimal part), convert to int and back to string
#             if value_float.is_integer():
#                 return str(int(value_float))
#             else:
#                 # If there is a decimal part, keep the original value
#                 return value
#         except (ValueError, TypeError):
#             # If conversion fails, return the original value
#             return value

#     # Apply the function to the specified columns
#     for col in columns_to_convert:
#         if col in df.columns:
#             df[col] = df[col].apply(remove_decimal)

#     # Identify other numeric columns that can be safely converted to integers
#     # Exclude the columns we have already processed
#     numeric_cols = df.select_dtypes(include=['object']).columns.difference(columns_to_convert)
#     numeric_cols = numeric_cols[df[numeric_cols].apply(lambda col: col.str.isnumeric().all())]

#     # Convert those columns to integers explicitly
#     df[numeric_cols] = df[numeric_cols].apply(lambda x: x.fillna(0).astype(int))

#     # List of missing columns based on the requirements
#     missing_columns = ['AdultContent', 'Delivery time', 'Bundled', 'EnergyEfficiencyClass', 'Multipack', 'SizeSystem']

#     # Add missing columns with empty values or default values
#     for col in missing_columns:
#         df[col] = ''  # Set as empty or default as needed

#     return df  # Ensure the function returns the processed dataframe


# # In[12]:


# def process_verizon(df):
#     try:
#         df = pd.read_csv(local_file_path, low_memory=False, dtype=str)
#         df['link'] = df['link'].astype(str).fillna('')

#         # Base URL to append
#         base_url = 'https://klarnashoppingads.ampxdirect.com/?partner=klarnashoppingads&sub1=shoppingads&ctaid=26026&v=1.3&source=als_tiles&match-method=deterministic'

#         # Function to encode the link and append it to the base URL
#         def create_new_link(original_link):
#             encoded_link = urllib.parse.quote_plus(original_link)
#             new_link = f"{base_url}&cu={encoded_link}&fbu={encoded_link}"
#             return new_link

#         # Apply the function to the 'link' column
#         df['link'] = df['link'].apply(create_new_link)

#         # Column renaming based on the required mapping
#         column_mapping = {
#             'id': 'SKU/id',
#             'title': 'Name',
#             'description': 'Description',
#             'google_product_category': 'Category',
#             'product_type': 'Product Type',
#             'link': 'URL',
#             'image_link': 'Image URL',
#             'condition': 'Condition',
#             'availability': 'Stock status',
#             'price': 'Price',
#             'brand': 'Manufacturer',
#             'gtin': 'EAN/GTIN',
#             'mpn': 'Manufacturer SKU / MPN',
#             'color': 'Color',
#             'size': 'Size',
#             'shipping': 'Shipping costs',
#             'custom_label_0': 'Custom Label 0',
#             'custom_label_1': 'Custom Label 1',
#             'custom_label_2': 'Custom Label 2',
#             'custom_label_3': 'Custom Label 3',
#             'custom_label_4': 'Custom Label 4',
#             'short_title': 'Short Title',
#             'gender': 'Gender',
#             'age_group': 'AgeGroup',
#             'installment': 'Installment',
#             'availability_date': 'Availability Date'
#         }

#         # Rename columns
#         df.rename(columns=column_mapping, inplace=True)

#         # Function to remove decimals from the SKU and GTIN columns
#         def remove_decimal(value):
#             try:
#                 value_float = float(value)
#                 if value_float.is_integer():
#                     return str(int(value_float))
#                 return value
#             except (ValueError, TypeError):
#                 return value

#         # Apply this function to necessary columns
#         columns_to_convert = ['SKU/id', 'EAN/GTIN']
#         for col in columns_to_convert:
#             if col in df.columns:
#                 df[col] = df[col].apply(remove_decimal)

#     except pd.errors.EmptyDataError:
#         print("Error: The CSV file is empty.")
#     except FileNotFoundError:
#         print(f"Error: The file {local_file_path} does not exist or is empty.")



# # In[13]:


# def process_newbalance(df):
#     try:
#         df = pd.read_csv(local_file_path, low_memory=False, dtype=str)
#         df['link'] = df['link'].astype(str).fillna('')

#         # Base URL to append
#         base_url = 'https://klarnashoppingads.ampxdirect.com/?partner=klarnashoppingads&sub1=shoppingads&ctaid=75063&v=1.3&source=als_tiles&match-method=deterministic'

#         # Function to encode the link and append it to the base URL
#         def create_new_link(original_link):
#             encoded_link = urllib.parse.quote_plus(original_link)
#             new_link = f"{base_url}&cu={encoded_link}&fbu={encoded_link}"
#             return new_link

#         # Apply the function to the 'link' column
#         df['link'] = df['link'].apply(create_new_link)

#         # Column renaming based on the required mapping
#         column_mapping = {
#             'GTIN': 'EAN/GTIN',
#             'MPN': 'Manufacturer SKU / MPN',
#             'ID': 'SKU/id',
#             'Link': 'URL',
#             'Title': 'Name',
#             'Description': 'Description',
#             'Image Link': 'Image URL',
#             'Price': 'Price',
#             'Condition': 'Condition',
#             'Availability': 'Stock status',
#             'Brand': 'Manufacturer',
#             'Google Product Category': 'Category',
#             'Top Performing Product': 'Bundled',  # Assuming relation
#             'Color': 'Color',
#             'Size': 'Size',
#             'Gender': 'Gender',
#             'Age Group': 'AgeGroup',
#             'Sale Price': 'Sale Price',
#             'Sale Price Effective Date': 'Sale Price Effective Date',
#             'Expiration Date': 'Expiration Date',
#     # Additional mappings from second part
#             'SizeSystem': 'SizeSystem',
#             'AdultContent': 'AdultContent',
#             'Delivery time': 'Delivery time',  # Mapped directly from the second part
#             'EnergyEfficiencyClass': 'EnergyEfficiencyClass',
#             'GroupId': 'GroupId',
#             'Material': 'Material',
#             'Multipack': 'Multipack',
#             'Pattern': 'Pattern'
#         }


#         # Rename columns
#         df.rename(columns=column_mapping, inplace=True)

#         # Function to remove decimals from the SKU and GTIN columns
#         def remove_decimal(value):
#             try:
#                 value_float = float(value)
#                 if value_float.is_integer():
#                     return str(int(value_float))
#                 return value
#             except (ValueError, TypeError):
#                 return value

#         # Apply this function to necessary columns
#         columns_to_convert = ['SKU/id', 'EAN/GTIN']
#         for col in columns_to_convert:
#             if col in df.columns:
#                 df[col] = df[col].apply(remove_decimal)

#     except pd.errors.EmptyDataError:
#         print("Error: The CSV file is empty.")
#     except FileNotFoundError:
#         print(f"Error: The file {local_file_path} does not exist or is empty.")


# # In[ ]:


# import pandas as pd
# import urllib.parse
# import os

# # Define the processing function for each file
# def process_file(advertiser, local_file_path, output_file_path):
#     # Read the CSV file (adjust reading logic per advertiser's format if needed)
#     df = pd.read_csv(local_file_path, low_memory=False, dtype=str)

#     # Processing logic specific to each advertiser
#     if advertiser == 'Bloomingdales':
#         df = process_bloomingdales(df)
        
#     elif advertiser == 'Verizon':
#         df = process_verizon(df)
#     elif advertiser == 'NewBalance':
#         df = process_newbalance(df)
#     #     

#     # # Add other advertiser-specific cases here (e.g., Tommy Bahama, HomeDepot, etc.)
    
#     # Save the processed file as TSV (with gzip compression)
#     df.to_csv(output_file_path, sep='\t', index=False, compression='gzip')
#     print(f"{advertiser}: Processed file has been saved as {output_file_path}")


# # Step 2: Process each file individually after downloading
# for advertiser, paths in advertisers.items():
#     local_folder = paths['local_path']
    
#     # Generate input file name (static or dynamic)
#     input_file_name = paths['file_pattern'](current_date) if callable(paths['file_pattern']) else paths['file_pattern']
#     local_file_path = os.path.join(local_folder, input_file_name)
    
#     # Output file for processed data
#     output_file_path = os.path.join(local_folder, f'amp_klarna_{advertiser}.tsv.gz')
    
#     # Process the file with advertiser-specific logic
#     process_file(advertiser, local_file_path, output_file_path)




# # In[ ]:




