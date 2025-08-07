const path = require('path');
const dotenv = require('dotenv');

// Load from common/.env first
dotenv.config({ path: path.resolve(__dirname, '../../common/.env') });

// Then load local .env to allow overrides
dotenv.config();

console.log('Environment loaded. CLAUDE_KEY:', process.env.CLAUDE_KEY ? 'Set' : 'Not set');