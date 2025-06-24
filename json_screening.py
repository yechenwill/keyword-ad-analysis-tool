import csv
import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# File paths
INPUT_JSON = 'qwant_kw_230625 (1).json'
SUMMARY_CSV = 'qwant.csv'
DETAILED_JSON = 'qwant.json'

# Performance settings
MAX_WORKERS = 10  # Number of concurrent requests
REQUEST_DELAY = 0.1  # Delay between requests in seconds
TIMEOUT = 30  # Request timeout in seconds

# API endpoint with query placeholder
API_URL_TEMPLATE = (
    'http://prod-ssp-engine-private.ric1.admarketplace.net/isp'
    '?plid=cjqduwisj4&results-ta=100&qt={}&country-code={}'
    '&region-code=&form-factor={}&os-family=windows'
    '&v=2.0&out=json&diag=enabled&ctaid='
)

HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'application/json'
}

def create_session():
    """Create a requests session with connection pooling and retry logic"""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    
    # Configure adapter with connection pooling
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def fetch_ads(keyword, country_code, form_factor, session):
    """Fetch ads for a single keyword using the provided session"""
    url = API_URL_TEMPLATE.format(keyword, country_code, form_factor)
    try:
        response = session.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return data.get('text_ads', [])
    except Exception as e:
        print(f'❌ Failed for "{keyword}": {e}')
        return []

def process_keyword_batch(keyword_batch, country_code, form_factor, session):
    """Process a batch of keywords concurrently"""
    results = {}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all requests
        future_to_keyword = {
            executor.submit(fetch_ads, keyword, country_code, form_factor, session): keyword
            for keyword in keyword_batch
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_keyword):
            keyword = future_to_keyword[future]
            try:
                ads = future.result()
                results[keyword] = ads
                print(f'      ✅ Completed: "{keyword}" ({len(ads)} ads)')
            except Exception as e:
                print(f'      ❌ Error processing "{keyword}": {e}')
                results[keyword] = []
            
            # Small delay to be respectful to the API
            time.sleep(REQUEST_DELAY)
    
    return results

def main():
    # Load input JSON file
    try:
        with open(INPUT_JSON, 'r', encoding='utf-8') as infile:
            input_data = json.load(infile)
    except FileNotFoundError:
        print(f'❌ Input file "{INPUT_JSON}" not found!')
        return
    except json.JSONDecodeError as e:
        print(f'❌ Invalid JSON in "{INPUT_JSON}": {e}')
        return

    # Handle both single object and array of objects
    if isinstance(input_data, list):
        data_items = input_data
    else:
        data_items = [input_data]

    summary_rows = []
    detailed_dict = {}
    
    # Create a session for connection pooling
    session = create_session()
    
    total_keywords = 0
    processed_keywords = 0

    # Count total keywords for progress tracking
    for item in data_items:
        search_terms = item.get('search-terms', {})
        for keyword_variations in search_terms.values():
            total_keywords += len([k for k in keyword_variations if len(k.strip()) >= 3])

    print(f'🚀 Starting processing of {total_keywords} keywords with {MAX_WORKERS} concurrent workers')
    print(f'⏱️  Estimated time: ~{total_keywords * REQUEST_DELAY / MAX_WORKERS:.1f} seconds')

    # Process each data item
    for item_index, input_data in enumerate(data_items):
        print(f'\n📋 Processing data item {item_index + 1}/{len(data_items)}')
        
        country_code = input_data.get('country-code', 'FR')
        form_factor = input_data.get('form-factor', 'desktop')
        search_terms = input_data.get('search-terms', {})

        if not search_terms:
            print('   ⚠️  No search terms found in this item, skipping...')
            continue

        print(f'   🌍 Country: {country_code}, Form Factor: {form_factor}')
        print(f'   📝 Found {len(search_terms)} main search terms')

        # Process each search term and its variations
        for main_term, keyword_variations in search_terms.items():
            print(f'\n   🔍 Processing main term: "{main_term}"')
            print(f'      Found {len(keyword_variations)} keyword variations')
            
            # Filter valid keywords
            valid_keywords = [k.strip() for k in keyword_variations if len(k.strip()) >= 3]
            
            if not valid_keywords:
                print('      ⚠️  No valid keywords found, skipping...')
                continue
            
            # Process keywords in batches for better progress tracking
            batch_size = 20  # Process 20 keywords at a time
            for i in range(0, len(valid_keywords), batch_size):
                batch = valid_keywords[i:i + batch_size]
                print(f'      📦 Processing batch {i//batch_size + 1}/{(len(valid_keywords) + batch_size - 1)//batch_size}')
                
                # Process batch concurrently
                batch_results = process_keyword_batch(batch, country_code, form_factor, session)
                
                # Process results
                for keyword, ads in batch_results.items():
                    advertiser_names = []
                    details = []

                    for ad in ads:
                        name = ad.get('adv_name', '').strip()
                        score = ad.get('keywordMatchingResult', {}).get('relevanceScore', '')
                        if name:
                            advertiser_names.append(name)
                            details.append({
                                "advertiser_name": name,
                                "relevance_score": score
                            })

                    summary_rows.append({
                        "data_item": item_index + 1,
                        "main_term": main_term,
                        "qt": keyword,
                        "advertisers": ",".join(advertiser_names)
                    })

                    detailed_dict[keyword] = {
                        "data_item": item_index + 1,
                        "main_term": main_term,
                        "details": details
                    }
                    
                    processed_keywords += 1
                    
                    # Show progress
                    progress = (processed_keywords / total_keywords) * 100
                    print(f'      📊 Progress: {processed_keywords}/{total_keywords} ({progress:.1f}%)')

    # Close session
    session.close()

    # Write summary CSV
    with open(SUMMARY_CSV, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["data_item", "main_term", "qt", "advertisers"])
        writer.writeheader()
        writer.writerows(summary_rows)

    # Write detailed JSON
    with open(DETAILED_JSON, 'w', encoding='utf-8') as jsonfile:
        json.dump(detailed_dict, jsonfile, ensure_ascii=False, indent=2)

    print(f'\n✅ Done! Summary saved to:\n  → {SUMMARY_CSV}\n  → {DETAILED_JSON}')
    print(f'📊 Processed {len(summary_rows)} keyword variations from {len(data_items)} data items')

if __name__ == '__main__':
    main()
