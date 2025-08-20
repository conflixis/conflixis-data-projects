#!/usr/bin/env python3
"""
Test OpenAI API connection and model availability
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

# Load environment variables
load_dotenv()

def test_openai_models():
    """Test different OpenAI models"""
    
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("❌ OPENAI_API_KEY not found in environment")
        return
    
    print(f"✓ API Key found: {api_key[:10]}...")
    
    client = OpenAI(api_key=api_key)
    
    # Test models
    models_to_test = [
        'gpt-4o-mini',
        'gpt-4o',
        'gpt-5-mini',
        'gpt-5-nano'
    ]
    
    for model in models_to_test:
        print(f"\nTesting model: {model}")
        print("-" * 40)
        
        try:
            # Prepare completion parameters
            completion_params = {
                'model': model,
                'messages': [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": "Say 'Hello World' in JSON format"}
                ]
            }
            
            # Handle model-specific parameters
            if 'gpt-5' in model:
                completion_params['max_completion_tokens'] = 50
            else:
                completion_params['max_tokens'] = 50
                completion_params['temperature'] = 0.1
            
            response = client.chat.completions.create(**completion_params)
            
            print(f"✓ Model {model} is available")
            print(f"  Response: {response.choices[0].message.content[:100]}")
            print(f"  Tokens used: {response.usage.total_tokens if response.usage else 'N/A'}")
            
        except Exception as e:
            error_msg = str(e)
            if 'model' in error_msg.lower() and 'not found' in error_msg.lower():
                print(f"✗ Model {model} not available")
            elif 'api key' in error_msg.lower():
                print(f"✗ API key issue: {error_msg[:100]}")
            else:
                print(f"✗ Error: {error_msg[:200]}")

if __name__ == "__main__":
    print("Testing OpenAI API Connection")
    print("=" * 50)
    test_openai_models()