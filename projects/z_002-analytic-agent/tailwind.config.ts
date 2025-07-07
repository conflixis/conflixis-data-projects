import type { Config } from 'tailwindcss'

const config: Config = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      fontFamily: {
        soehneLight: ['SoehneLight', 'sans-serif'],
        soehneRegular: ['SoehneRegular', 'sans-serif'],
        soehneBold: ['SoehneBold', 'sans-serif'],
        soehneKraftig: ['SoehneKraftig', 'sans-serif'],
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
        // Keep existing for backward compatibility
        border: "hsl(var(--border))",
        input: "hsl(var(--input))",
        ring: "hsl(var(--ring))",
        background: "hsl(var(--background))",
        foreground: "hsl(var(--foreground))",
        primary: {
          DEFAULT: "hsl(var(--primary))",
          foreground: "hsl(var(--primary-foreground))",
        },
        secondary: {
          DEFAULT: "hsl(var(--secondary))",
          foreground: "hsl(var(--secondary-foreground))",
        },
        destructive: {
          DEFAULT: "hsl(var(--destructive))",
          foreground: "hsl(var(--destructive-foreground))",
        },
        muted: {
          DEFAULT: "hsl(var(--muted))",
          foreground: "hsl(var(--muted-foreground))",
        },
        accent: {
          DEFAULT: "hsl(var(--accent))",
          foreground: "hsl(var(--accent-foreground))",
        },
      },
      animation: {
        "pulse-slow": "pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite",
      },
    },
  },
  plugins: [require("tailwindcss-animate")],
}
export default config