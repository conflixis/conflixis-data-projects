// Test script to directly call the API
const fetch = require('node-fetch');

async function testAPI() {
  try {
    console.log('Testing API endpoint with question: "Show me the ten highest payments received in 2023"');
    
    const response = await fetch('http://localhost:3000/api/analyze', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        query: 'Show me the ten highest payments received in 2023'
      })
    });

    console.log('Response status:', response.status);
    
    const data = await response.json();
    
    if (data.success) {
      console.log('\n=== SUCCESS ===');
      console.log('\nSQL Generated:');
      console.log(data.sqlGenerated);
      console.log('\nQuery Cost: $' + (data.queryCost || 0).toFixed(4));
      console.log('\nCompliance Analysis:');
      console.log(data.complianceAnalysis);
      console.log('\nTotal Rows:', data.totalRows);
      if (data.resultsPreview && data.resultsPreview.length > 0) {
        console.log('\nResults Preview (first row):');
        console.log(JSON.stringify(data.resultsPreview[0], null, 2));
      }
    } else {
      console.log('\n=== ERROR ===');
      console.log('Error:', data.error);
    }
    
  } catch (error) {
    console.error('Error calling API:', error);
  }
}

testAPI();