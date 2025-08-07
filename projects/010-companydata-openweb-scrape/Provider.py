import os
from openai import OpenAI
from dotenv import load_dotenv

# Load API key from .env file
load_dotenv("C:\\Users\\vince\\OneDrive\\Documents\\Conflixis\\conflixis-ai\\common\\.env")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

def get_company_info(provider_name):
    messages = [
        {"role": "system", "content": f"Who is {provider_name}? They should be a healthcare provider based in the USA. Provide back the data in json format"},
    ]

    # Use the API key from your .env file
    client = OpenAI(api_key=OPENAI_API_KEY)

    response = client.chat.completions.create(
        model="gpt-4-1106-preview",
        messages=messages,
        response_format={"type": "json_object"}
    )

    return response.choices[0].message.content

# Example usage
provider_name = "David Albala"
print(get_company_info(provider_name))