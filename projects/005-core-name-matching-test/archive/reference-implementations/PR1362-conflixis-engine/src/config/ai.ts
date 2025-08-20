import { createOpenAI } from '@ai-sdk/openai';
import { config } from './environment';

// Configure the OpenAI provider with the API key
export const openaiProvider = createOpenAI({
  apiKey: config.openai.apiKey,
  // You can add other options here like baseURL for custom endpoints
});

// Helper function to get model
export function getModel(modelName?: string) {
  const model = modelName || config.openai.model;
  return openaiProvider(model);
}
