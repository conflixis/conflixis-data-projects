/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  swcMinify: true,
  env: {
    ANTHROPIC_API_KEY: process.env.CLAUDE_KEY || process.env.ANTHROPIC_API_KEY,
    GOOGLE_CLOUD_PROJECT: process.env.GOOGLE_CLOUD_PROJECT,
  },
}

module.exports = nextConfig