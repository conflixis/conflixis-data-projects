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
        // Conflixis Brand Colors
        'conflixis-green': '#0c343a',
        'conflixis-light-green': '#93baab',
        'conflixis-white': '#ffffff',
        'conflixis-gray': '#d1d5db',
        'conflixis-tan': '#f5e6d3',
        'conflixis-ivory': '#f9f7f4',
        'conflixis-gold': '#eab96d',
        'conflixis-blue': '#4c94ed',
        'conflixis-navy': '#1e3a5f',
        'conflixis-mint': '#34d399',
        'conflixis-red': '#dc2626',
        'conflixis-success-green': '#10b981',
        
        // Background colors
        background: "var(--background)",
        foreground: "var(--foreground)",
      },
      fontFamily: {
        'soehneKraftig': ['Soehne Kraftig', 'sans-serif'],
        'soehneLeicht': ['Soehne Leicht', 'sans-serif'],
        'ivarDisplay': ['Ivar Display', 'serif'],
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