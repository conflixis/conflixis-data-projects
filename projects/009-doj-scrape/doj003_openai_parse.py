# -------------------------------
# Imports
# -------------------------------
import os
import json
import sys
from dotenv import load_dotenv
import pandas as pd
import csv
import time
import spacy
import asyncio
from openai import AsyncOpenAI  # New async client

# -------------------------------
# Configurable Parameters
# -------------------------------
USE_JSON_FOR_KEYWORDS = False
ENABLE_KEYWORD_FILTERING = False
KEYWORDS = {'biotronik', 'medical device', 'medical devices', 'pharmaceutical', 'pharmaceuticals', 'healthcare'}
CONCURRENT_TASKS = 100  # Limit number of concurrent API calls

# -------------------------------
# Paths and System Content Setup
# -------------------------------
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, '..'))

csv_file_path = os.path.join(project_root, 'dojscrape', 'dojpr_bodies.csv')
csv_output_path = os.path.join(project_root, 'dojscrape', 'doj003_openai_output.csv')
excel_file_path = os.path.join(project_root, 'open_fine_tuning', 'dojpr_excel_template.xlsx')
env_path = os.path.join(project_root, 'common', '.env')

df = pd.read_excel(excel_file_path)
column_names = list(df.columns)
system_content = (
        "Given a press release from the US Department of Justice, we're interested in extracting only the factual information "
        "explicitly stated in the text about a company that has violated healthcare acts. Provide the following fields in a JSON dict "
        "only if the information is directly stated in the article. If not, do not fabricate any data_4o_websearch: " + ', '.join(
    column_names) + "."
)

# -------------------------------
# Environment Setup and API Key
# -------------------------------
load_dotenv(env_path)
api_key = os.getenv("OPENAI_DOJ_API_KEY")
client = AsyncOpenAI(api_key=api_key)

# -------------------------------
# Initialize NLP Model
# -------------------------------
nlp = spacy.load("en_core_web_sm")

# -------------------------------
# Check Existing Processed URLs (for crash recovery)
# -------------------------------
processed_urls = set()
if os.path.exists(csv_output_path):
    df_output = pd.read_csv(csv_output_path)
    processed_urls = set(df_output['URL'])


# -------------------------------
# Main Processing Functions
# -------------------------------
async def process_data(url, lastmod, html_body, writer, total_records, index, semaphore):
    # Optional keyword filtering
    if ENABLE_KEYWORD_FILTERING and KEYWORDS:
        doc = nlp(html_body)
        if not any(token.lemma_ in KEYWORDS for token in doc):
            print(f"Skipping {url} - no keywords", flush=True)
            return "skipped"

    async with semaphore:
        print(f"Starting API call for {url} [{index + 1}/{total_records}]", flush=True)
        try:
            # Increase timeout if needed (set here to 120 seconds)
            completion = await asyncio.wait_for(
                client.chat.completions.create(
                    model="ft:gpt-3.5-turbo-0613:conflixis::7tyZyO7j",
                    messages=[
                        {"role": "user", "content": system_content},
                        {"role": "user", "content": html_body}
                    ]
                ),
                timeout=120.0
            )
            print(f"Completed API call for {url}", flush=True)
            response_content = completion.choices[0].message.content
            parsed_response = json.loads(response_content)
            parsed_response['URL'] = url
            parsed_response['LastMod'] = lastmod
            writer.writerow([url, lastmod, json.dumps(parsed_response)])
            sys.stdout.flush()
            return "processed"
        except asyncio.TimeoutError:
            print(f"Timeout processing {url}", flush=True)
            return "failed"
        except Exception as e:
            print(f"Failed to process {url}: {e}", flush=True)
            return "failed"


async def main():
    df_csv = pd.read_csv(csv_file_path)
    df_csv = df_csv[~df_csv['URL'].isin(processed_urls)]
    df_csv.reset_index(drop=True, inplace=True)
    total_records = len(df_csv)

    semaphore = asyncio.Semaphore(CONCURRENT_TASKS)

    with open(csv_output_path, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        if not processed_urls:
            writer.writerow(["URL", "LastMod", "Parsed_Response"])

        tasks = []
        for i, row in df_csv.iterrows():
            tasks.append(
                process_data(row['URL'], row['LastMod'], row['HTML_Body'], writer, total_records, i, semaphore))

        results = await asyncio.gather(*tasks)
        print(
            f"Summary:\nProcessed: {results.count('processed')}\nSkipped: {results.count('skipped')}\nFailed: {results.count('failed')}",
            flush=True
        )


# -------------------------------
# Script Execution
# -------------------------------
if __name__ == "__main__":
    asyncio.run(main())
