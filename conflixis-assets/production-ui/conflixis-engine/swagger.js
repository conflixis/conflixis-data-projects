const swaggerAutogen = require('swagger-autogen')();

const doc = {
  info: {
    title: 'Conflixis API',
    description: 'The primary API serving requests for client.conflixis.com & portal.conflixis.com',
  },
  host: 'https://api-node-afkcxqwkva-uc.a.run.app',
  schemes: ['https'],
};

const outputFile = './docs/api-spec.json';
const endpointsFiles = ['./packages/api/src/main.ts'];

/* NOTE: if you use the express Router, you must pass in the 
   'endpointsFiles' only the root file where the route starts,
   such as index.js, app.js, routes.js, ... */

swaggerAutogen(outputFile, endpointsFiles, doc);
