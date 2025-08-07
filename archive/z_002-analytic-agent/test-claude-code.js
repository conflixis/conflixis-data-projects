// Test Claude Code SDK directly
const { query } = require('@anthropic-ai/claude-code');

async function testClaude() {
  console.log('Testing Claude Code SDK...');
  console.log('ANTHROPIC_API_KEY set:', !!process.env.ANTHROPIC_API_KEY);
  console.log('CLAUDE_KEY set:', !!process.env.CLAUDE_KEY);
  
  try {
    // Map CLAUDE_KEY to ANTHROPIC_API_KEY if needed
    if (process.env.CLAUDE_KEY && !process.env.ANTHROPIC_API_KEY) {
      process.env.ANTHROPIC_API_KEY = process.env.CLAUDE_KEY;
      console.log('Mapped CLAUDE_KEY to ANTHROPIC_API_KEY');
    }
    
    const prompt = `As a Senior Healthcare Compliance Expert, generate a BigQuery SQL query to find the ten highest payments received in 2023 from the table data-analytics-389803.conflixis_datasources.op_general_all. Include relevant columns for compliance analysis.`;
    
    console.log('Sending query to Claude Code...');
    
    let response = '';
    for await (const message of query({
      prompt: prompt,
      options: { maxTurns: 1, allowedTools: [] }
    })) {
      if (message.type === 'text') {
        response += message.content;
      }
    }
    
    console.log('Response received:');
    console.log(response);
    
  } catch (error) {
    console.error('Error:', error);
  }
}

testClaude();