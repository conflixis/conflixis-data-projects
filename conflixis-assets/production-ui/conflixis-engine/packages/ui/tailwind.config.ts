import type { Config } from 'tailwindcss';

const config: Config = {
  content: ['src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        soehneLight: ['SoehneLight', 'sans-serif'],
        soehneRegular: ['SoehneRegular', 'sans-serif'],
        soehneBold: ['SoehneBold', 'sans-serif'],
        SoehneKraftig: ['SoehneKraftig', 'sans-serif'],
      },
      colors: {
        'conflixis-light-green': '#93baab',
        'conflixis-green': '#0c343a',
        'conflixis-white': '#ffffff',
        'conflixis-gray': '#d1d5db',
        'conflixis-gray-hover': '#b9bec4',
        'conflixis-ivory': '#f8f5ef',
        'conflixis-tan': '#c5bdae',
        'conflixis-mint': '#93baab',
        'conflixis-gold': '#eab96d',
        'conflixis-blue': '#4c94ed',
        'conflixis-red': '#fd7649',
        'conflixis-success-green': '#008000FF',
      },
    },
  },
  plugins: [],
};
export default config;
