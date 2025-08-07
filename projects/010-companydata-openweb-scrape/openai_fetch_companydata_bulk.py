import pandas as pd
import os
import json
import re
import threading
from openai import OpenAI
from dotenv import load_dotenv
from pathlib import Path
from queue import Queue

# ----------------------------------------
# Imports
# ----------------------------------------

# ----------------------------------------
# Configurable Parameters
# ----------------------------------------

# Load API key from .env file
load_dotenv("C:\\Users\\vince\\OneDrive\\Documents\\Conflixis\\conflixis-ai\\common\\.env")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# File paths
file_path = 'C:\\Users\\vince\\OneDrive\\Documents\\Conflixis\\conflixis-ai\\companydata\\DATA\\OP_companies.csv'
json_dir = Path('C:\\Users\\vince\\OneDrive\\Documents\\Conflixis\\conflixis-ai\\companydata\\DATA\\Company_JSONs')

# Number of threads for concurrent API calls
num_threads = 10

# ----------------------------------------
# Main Processing Functions
# ----------------------------------------

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "_", name)

def get_company_info(company_name, gpo_id):
    messages = [
        {"role": "system", "content": f"You are a KYB compliance analyst, provide company information about {company_name} including ticker symbol, ticker stock exchange, 2022 revenue, 2021 revenue, number of employees, number of offices, parent company, parent company ticker, parent ticker stock exchange, parent company, parent company country of headquarter, parent company revenue 2022 and parent company revenue 2021  in json format. Ensure that all fields are numerical apart from the company name and ticker symbol. Do not abbreviate numbers, just put in the full amount in USD. I want to analyse in excel. Add one field for commentary. If not data_websearch is available for a field, just put in \"NA\". Only provide the json, I do not want any other text or message."}
    ]
    client = OpenAI(api_key=OPENAI_API_KEY)

    response = client.chat.completions.create(
        model="gpt-4-1106-preview",
        messages=messages,
        response_format={"type": "json_object"}
    )

    try:
        company_info = json.loads(response.choices[0].message.content)
    except json.JSONDecodeError:
        print(f"Error decoding JSON for {company_name}")
        company_info = {"error": "Invalid JSON response"}

    extended_company_info = {
        "GPO_ID": gpo_id,
        "Company Name": company_name,
        "OpenAI Response": company_info
    }
    return extended_company_info

def worker():
    while not company_queue.empty():
        company_name, gpo_id = company_queue.get()
        sanitized_name = sanitize_filename(company_name)
        json_file_path = json_dir / f'{sanitized_name}.json'

        if not json_file_path.exists():
            try:
                company_data = get_company_info(company_name, gpo_id)
                with open(json_file_path, 'w') as f:
                    json.dump(company_data, f, indent=4)
                print(f"Processed {company_name}")
            except Exception as e:
                print(f"Error processing {company_name}: {e}")
        else:
            print(f"Data for {company_name} already exists. Skipping.")
        company_queue.task_done()

# ----------------------------------------
# Script Execution
# ----------------------------------------

df = pd.read_csv(file_path)

json_dir.mkdir(parents=True, exist_ok=True)

company_queue = Queue()

for _, row in df.iterrows():
    company_queue.put((row['Most_Common_SUPPLIER_NAME'], row['GPO_ID']))

threads = []
for i in range(num_threads):
    t = threading.Thread(target=worker)
    t.start()
    threads.append(t)

for t in threads:
    t.join()

print("Completed processing and saved individual JSON files.")
