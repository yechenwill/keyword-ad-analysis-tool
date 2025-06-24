import csv
import json
import requests

# File paths
INPUT_JSON = 'input_search_terms.json'
SUMMARY_CSV = 'qwant.csv'
DETAILED_JSON = 'qwant.json'

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

def fetch_ads(keyword, country_code, form_factor):
    url = API_URL_TEMPLATE.format(keyword, country_code, form_factor)
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        return data.get('text_ads', [])
    except Exception as e:
        print(f'❌ Failed for "{keyword}": {e}')
        return []

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

    # Extract configuration from JSON
    country_code = input_data.get('country-code', 'FR')
    form_factor = input_data.get('form-factor', 'desktop')
    search_terms = input_data.get('search-terms', {})

    if not search_terms:
        print('❌ No search terms found in the JSON file!')
        return

    summary_rows = []
    detailed_dict = {}

    # Process each search term and its variations
    for main_term, keyword_variations in search_terms.items():
        print(f'\n🔍 Processing main term: "{main_term}"')
        print(f'   Found {len(keyword_variations)} keyword variations')
        
        for keyword in keyword_variations:
            keyword = keyword.strip()
            
            if len(keyword) < 3:
                continue

            print(f'   🔍 Fetching ads for: "{keyword}"')
            ads = fetch_ads(keyword, country_code, form_factor)

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
                "main_term": main_term,
                "qt": keyword,
                "advertisers": ",".join(advertiser_names)
            })

            detailed_dict[keyword] = {
                "main_term": main_term,
                "details": details
            }

    # Write summary CSV
    with open(SUMMARY_CSV, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["main_term", "qt", "advertisers"])
        writer.writeheader()
        writer.writerows(summary_rows)

    # Write detailed JSON
    with open(DETAILED_JSON, 'w', encoding='utf-8') as jsonfile:
        json.dump(detailed_dict, jsonfile, ensure_ascii=False, indent=2)

    print(f'\n✅ Done! Summary saved to:\n  → {SUMMARY_CSV}\n  → {DETAILED_JSON}')
    print(f'📊 Processed {len(summary_rows)} keyword variations from {len(search_terms)} main terms')

if __name__ == '__main__':
    main()
