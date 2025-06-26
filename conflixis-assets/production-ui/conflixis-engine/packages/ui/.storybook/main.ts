import type { StorybookConfig } from '@storybook/react-vite';

const config: StorybookConfig = {
  stories: ['../src/**/*.stories.@(ts|tsx)'],
  addons: [
    '@storybook/addon-essentials',
    '@storybook/addon-a11y'
  ],
  framework: '@storybook/react-vite',
  typescript: {
    check: true,
    reactDocgen: 'react-docgen-typescript',
  },
  viteFinal: async (config) => {
    config.css = { preprocessorOptions: { tailwindcss: {} } };
    return config;
  },
};
export default config;
