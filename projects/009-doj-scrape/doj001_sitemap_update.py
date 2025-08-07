# ======================
# Imports
# ======================
import requests
from xml.etree import ElementTree as ET
import csv
import json
import pandas as pd

# ======================
# Configurable Parameters
# ======================
SITEMAP_INDEX_URL = 'https://www.justice.gov/sitemap.xml'  # Replace with your sitemap index URL
OUTPUT_FILE_ALL = 'doj002a_processed_urls.csv'
OUTPUT_FILE_PR = 'doj002b_post_processed_urls.csv'
LASTMOD_FILE = 'lastmod.json'


# ======================
# Main Processing Functions
# ======================
def load_previous_csv(filename):
    try:
        df = pd.read_csv(filename)
        return [tuple(x) for x in df.to_records(index=False)]
    except FileNotFoundError:
        return []

def fetch_sitemap(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content


def parse_sitemap_index(sitemap_index_content):
    root = ET.fromstring(sitemap_index_content)
    sitemap_data = []
    for elem in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}sitemap"):
        loc = elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc").text
        lastmod = elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod").text if elem.find(
            "{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod") is not None else ''
        sitemap_data.append({'loc': loc, 'lastmod': lastmod})
    return sitemap_data


def parse_sitemap_page(sitemap_content):
    root = ET.fromstring(sitemap_content)
    urls = []
    for url_elem in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
        loc = url_elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc").text
        lastmod = url_elem.find("{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod")
        lastmod = lastmod.text if lastmod is not None else ''
        urls.append((loc, lastmod))
    return urls


def save_to_csv(data, filename, filter_func=None):
    data.sort(key=lambda x: x[1])  # Sort by lastmod
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['URL', 'LastMod'])
        for loc, lastmod in data:
            if filter_func is None or filter_func(loc, lastmod):
                writer.writerow([loc, lastmod])



# Load and save lastmod dates to avoid unnecessary fetching
def load_lastmod(filename):
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_lastmod(lastmod_data, filename):
    with open(filename, 'w') as f:
        json.dump(lastmod_data, f)


# ======================
# Script Execution
# ======================
if __name__ == '__main__':
    print("Fetching sitemap index...")
    sitemap_index_content = fetch_sitemap(SITEMAP_INDEX_URL)
    sitemap_data = parse_sitemap_index(sitemap_index_content)

    lastmod_data = load_lastmod(LASTMOD_FILE)

    all_urls = []
    fetched_new_data = False
    for i, data in enumerate(sitemap_data):
        loc, lastmod = data['loc'], data['lastmod']
        print(f"Progress: Fetching sitemap page {i + 1}/{len(sitemap_data)}")
        if loc not in lastmod_data or lastmod_data[loc] != lastmod:
            sitemap_content = fetch_sitemap(loc)
            urls = parse_sitemap_page(sitemap_content)
            all_urls.extend(urls)
            lastmod_data[loc] = lastmod
            fetched_new_data = True

    if not fetched_new_data:
        print("No new sitemap data_4o_websearch. Loading previous URLs.")
        all_urls = load_previous_csv(OUTPUT_FILE_ALL)

    save_lastmod(lastmod_data, LASTMOD_FILE)

    print(f"Progress: Saving {len(all_urls)} URLs to {OUTPUT_FILE_ALL}.")
    save_to_csv(all_urls, OUTPUT_FILE_ALL)

    pr_urls = [url for url in all_urls if '/pr/' in url[0]]
    filtered_pr_urls = [url for url in pr_urls if url[1] and int(url[1].split('-')[0]) >= 2020]
    print(f"Progress: Saving {len(filtered_pr_urls)} '/pr/' URLs with lastmod >= 2020 to {OUTPUT_FILE_PR}.")
    save_to_csv(pr_urls, OUTPUT_FILE_PR, filter_func=lambda x, y: int(y.split('-')[0]) >= 2020 if y else False)



