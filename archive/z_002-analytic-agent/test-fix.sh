#!/bin/bash

echo "Testing Healthcare Compliance Bot - SQL Generation Fix"
echo "===================================================="

# Test 1: Original problematic query
echo -e "\nTest 1: Speaker fees from multiple companies"
echo "--------------------------------------------"
curl -s -X POST http://localhost:3010/api/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Show me physicians who received speaker fees from more than 5 different companies last year"
  }' | grep -o '"success":[^,]*' | head -1

# Test 2: Query with ownership indicator
echo -e "\nTest 2: Ownership + speaker fees"
echo "--------------------------------"
curl -s -X POST http://localhost:3010/api/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Show me physicians with ownership interests who received speaker fees from more than 3 companies"
  }' | grep -o '"success":[^,]*' | head -1

# Test 3: Another Yes/No field
echo -e "\nTest 3: Third party indicator"
echo "------------------------------"
curl -s -X POST http://localhost:3010/api/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Find physicians where third party payment recipient indicator is Yes and they received over $50000"
  }' | grep -o '"success":[^,]*' | head -1

echo -e "\n\nAll tests completed!"