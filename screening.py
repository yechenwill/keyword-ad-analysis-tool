import csv
import json
import requests

# File paths
INPUT_CSV = 'C:/Users/ywang/Documents/Codes/qwant_fr_5_19.csv'
SUMMARY_CSV = 'C:/Users/ywang/Documents/Codes/qwant.csv'
DETAILED_JSON = 'C:/Users/ywang/Documents/Codes/qwant.json'

# API endpoint with query placeholder
API_URL_TEMPLATE = (
    'http://prod-ssp-engine-private.ric1.admarketplace.net/isp'
    '?plid=cjqduwisj4&results-ta=100&qt={}&country-code=FR'
    '&region-code=&form-factor=desktop&os-family=windows'
    '&v=2.0&out=json&diag=enabled&ctaid='
)

HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    'Accept': 'application/json'
}

def fetch_ads(keyword):
    url = API_URL_TEMPLATE.format(keyword)
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        return data.get('text_ads', [])
    except Exception as e:
        print(f'❌ Failed for "{keyword}": {e}')
        return []

def open_csv_with_fallback(path):
    try:
        return open(path, newline='', encoding='utf-8')
    except UnicodeDecodeError:
        return open(path, newline='', encoding='cp1252')

def main():
    summary_rows = []
    detailed_dict = {}

    with open_csv_with_fallback(INPUT_CSV) as infile:
        reader = csv.DictReader(infile)
        reader.fieldnames = [h.strip().lower() for h in reader.fieldnames]

        for row in reader:
            keyword = row.get('qt') or row.get('keyword') or ''
            keyword = keyword.strip()

            if len(keyword) < 3:
                continue

            print(f'🔍 Fetching ads for: "{keyword}"')
            ads = fetch_ads(keyword)

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
                "qt": keyword,
                "advertisers": ",".join(advertiser_names)
            })

            detailed_dict[keyword] = details

    # Write summary CSV
    with open(SUMMARY_CSV, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["qt", "advertisers"])
        writer.writeheader()
        writer.writerows(summary_rows)

    # Write detailed JSON
    with open(DETAILED_JSON, 'w', encoding='utf-8') as jsonfile:
        json.dump(detailed_dict, jsonfile, ensure_ascii=False, indent=2)

    print(f'\n✅ Done! Summary saved to:\n  → {SUMMARY_CSV}\n  → {DETAILED_JSON}')

if __name__ == '__main__':
    main()
