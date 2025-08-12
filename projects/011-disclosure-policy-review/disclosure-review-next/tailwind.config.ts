import type { Config } from "tailwindcss";

export default {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        // Conflixis Brand Colors - matching original HTML exactly
        'conflixis-green': '#0c343a',
        'conflixis-light-green': '#93baab',
        'conflixis-white': '#ffffff',
        'conflixis-gray': '#d1d5db',
        'conflixis-gray-hover': '#b9bec4',
        'conflixis-ivory': '#f8f5ef',
        'conflixis-tan': '#c5bdae',
        'conflixis-mint': '#93baab',
        'conflixis-gold': '#eab96d',
        'conflixis-blue': '#4c94ed',
        'conflixis-red': '#fd7649',
        'conflixis-success-green': '#008000',
        
        // Background colors
        background: "var(--background)",
        foreground: "var(--foreground)",
      },
      fontFamily: {
        'soehneKraftig': ['SoehneKraftig', 'sans-serif'],
        'soehneLeicht': ['SoehneLeicht', 'sans-serif'],
        'ivarDisplay': ['IvarDisplay', 'serif'],
      },
      animation: {
        'fade-in': 'fadeIn 0.3s ease-out',
        'slide-in': 'slideIn 0.3s ease-out',
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
      },
      keyframes: {
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        slideIn: {
          '0%': { 
            opacity: '0',
            transform: 'translateY(-20px) scale(0.95)'
          },
          '100%': { 
            opacity: '1',
            transform: 'translateY(0) scale(1)'
          },
        },
      },
    },
  },
  plugins: [],
} satisfies Config;