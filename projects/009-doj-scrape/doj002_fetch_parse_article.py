import os
import csv
import requests
from datetime import datetime
from bs4 import BeautifulSoup
from time import sleep
import pandas as pd
import json
import spacy


# Configuration
CSV_FILE = "C:\\Users\\vince\\OneDrive\\Documents\\Conflixis\\conflixis-ai\\dojscrape\\doj002b_post_processed_urls.csv"
OUTPUT_DIR = "C:\\Users\\vince\\OneDrive\\Documents\\Conflixis\\conflixis-ai\\dojscrape"
START_DATE = "2023-01-01"
# last ran up to 2023-05-01, work backwards, started from 2024-01-01
END_DATE = "2025-01-31"
MAX_RETRIES = 3  # Maximum number of retries for each URL
USE_JSON_FOR_KEYWORDS = False  # Set to True to read keywords from a JSON file; False to use the default set
#KEYWORDS = {'biotronik', 'medical device', 'medical devices', 'pharmaceutical', 'pharmaceuticals', 'healthcare'}  # Default value
KEYWORDS = set()  # Use this line if you don't want keyword filtering

# Read keywords from JSON if the flag is set
if USE_JSON_FOR_KEYWORDS:
    try:
        with open("/common/converted_keywords.json",
                  'r') as f:
            KEYWORDS = set(json.load(f))
    except FileNotFoundError:
        print("Keywords JSON file not found. Using default keywords.")

# Generate date range
dates = pd.date_range(END_DATE, START_DATE, freq='-1D').strftime('%Y-%m-%d')

def clean_url(url):
    """
    Cleans the URL by removing '/alias' at the end, if present.
    """
    if url.endswith('/alias'):
        return url[:-6]  # Removes the last 6 characters ('/alias')
    return url

def load_processed_urls(output_csv_path):
    try:
        with open(output_csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            return set(row[0] for row in reader)
    except FileNotFoundError:
        return set()

def clean_html_body(html_body):
    cleaned_body = html_body.replace('Breadcrumb', '').replace('Justice.gov', '').replace('U.S. Attorneys', '')
    cleaned_body = ' '.join(cleaned_body.split())
    return cleaned_body

def save_html_to_csv(html_content, last_mod, url, csv_writer):
    soup = BeautifulSoup(html_content, 'html.parser')
    article_body = soup.find('main', {'class': 'main-content usa-layout-docs usa-section position-relative'})

    if article_body:
        article_text = article_body.get_text().replace("\\n", " ").replace("\\r", " ").strip()
        article_text = clean_html_body(article_text)
    else:
        article_text = "Article body not found."

    csv_writer.writerow([url, last_mod, article_text])

def main():
    # Initialize spaCy NLP object
    nlp = spacy.load("en_core_web_sm")

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    output_csv_path = os.path.join(OUTPUT_DIR, "dojpr_bodies.csv")
    try:
        df_output = pd.read_csv(output_csv_path)
        processed_urls = set(df_output['URL'])
    except FileNotFoundError:
        processed_urls = set()

    with open(output_csv_path, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not processed_urls:
            writer.writerow(["URL", "LastMod", "HTML_Body"])

        for specific_day in dates:  # Iterating dates in descending order
            processed_count = 0
            skipped_count = 0
            failed_count = 0

            with open(CSV_FILE, 'r', newline='') as url_csvfile:
                reader = csv.reader(url_csvfile)
                next(reader)

                for row in reader:
                    url, lastmod = row
                    url = clean_url(url)  # Ensure the URL is cleaned
                    lastmod_date = datetime.fromisoformat(lastmod).date()

                    if str(lastmod_date) == specific_day:

                        if KEYWORDS:
                            doc = nlp(url)
                            if not any(token.lemma_ in KEYWORDS for token in doc):
                                print(f"Skipping {url} - no keywords")
                                continue

                        if url not in processed_urls:
                            retries = 0
                            while retries <= MAX_RETRIES:
                                print(f"Attempt {retries + 1} - Processing {url} ...")
                                try:
                                    response = requests.get(url)
                                    if response.status_code == 200:
                                        save_html_to_csv(response.text, lastmod, url, writer)
                                        processed_count += 1
                                        break
                                    else:
                                        print(f"Failed with status code {response.status_code}. Retrying...")
                                        retries += 1
                                        sleep(13)
                                except (requests.exceptions.RequestException, requests.exceptions.ConnectionError):
                                    print(f"Failed to download {url}. Retrying...")
                                    retries += 1
                                    sleep(13)
                            if retries > MAX_RETRIES:
                                print(f"Gave up on {url} after {MAX_RETRIES} retries.")
                                failed_count += 1
                            sleep(13)
                        else:
                            print(f"Skipping {url} ...")
                            skipped_count += 1

            print(f"Summary for {specific_day}:\nProcessed: {processed_count}\nSkipped: {skipped_count}\nFailed: {failed_count}")

if __name__ == "__main__":
    main()