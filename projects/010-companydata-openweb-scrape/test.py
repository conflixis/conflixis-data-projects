import os
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_company_info(company_name):
    messages = [
        {"role": "system", "content": f"Provide information about {company_name} including ticker symbol, 2022 revenue, 2021 revenue, number of employees, number of offices in json format, parent company, parent company revenue 2022 and parent company revenue 2021.. Ensure that all fields are numerical apart from the company name and ticker symbol. I want to analyse in excel. Add one field for commentary if no data is available for any field I specified."},
    ]

    # Use the API key from your .env file
    client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

    response = client.chat.completions.create(
        model="gpt-4-1106-preview",
        messages=messages,
        response_format={"type": "json_object"}
    )

    return response.choices[0].message.content

# Example usage
company_name = "Google, inc."
print(get_company_info(company_name))