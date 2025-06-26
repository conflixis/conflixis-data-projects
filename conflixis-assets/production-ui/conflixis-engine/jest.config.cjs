module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: ['**/?(*.)+(spec|test).[jt]s?(x)'],
  projects: [
    '<rootDir>/packages/core/jest.config.cjs',
    '<rootDir>/packages/api/jest.config.cjs',
    '<rootDir>/packages/jobs/jest.config.cjs',
  ],
};
