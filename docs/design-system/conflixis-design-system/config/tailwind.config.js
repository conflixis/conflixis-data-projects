/** @type {import('tailwindcss').Config} */
module.exports = {
  darkMode: ['class'],
  content: [
    './pages/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  theme: {
    extend: {
      fontFamily: {
        soehneLeicht: ['SoehneLeicht', 'sans-serif'],
        soehneKraftig: ['SoehneKraftig', 'sans-serif'],
        ivarDisplay: ['IvarDisplay', 'serif']
      },
      colors: {
        // Conflixis Brand Colors
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
        
        // shadcn/ui colors
        background: 'hsl(var(--background))',
        foreground: 'hsl(var(--foreground))',
        card: {
          DEFAULT: 'hsl(var(--card))',
          foreground: 'hsl(var(--card-foreground))'
        },
        popover: {
          DEFAULT: 'hsl(var(--popover))',
          foreground: 'hsl(var(--popover-foreground))'
        },
        primary: {
          DEFAULT: 'hsl(var(--primary))',
          foreground: 'hsl(var(--primary-foreground))'
        },
        secondary: {
          DEFAULT: 'hsl(var(--secondary))',
          foreground: 'hsl(var(--secondary-foreground))'
        },
        muted: {
          DEFAULT: 'hsl(var(--muted))',
          foreground: 'hsl(var(--muted-foreground))'
        },
        accent: {
          DEFAULT: 'hsl(var(--accent))',
          foreground: 'hsl(var(--accent-foreground))'
        },
        destructive: {
          DEFAULT: 'hsl(var(--destructive))',
          foreground: 'hsl(var(--destructive-foreground))'
        },
        border: 'hsl(var(--border))',
        input: 'hsl(var(--input))',
        ring: 'hsl(var(--ring))',
        chart: {
          '1': 'hsl(var(--chart-1))',
          '2': 'hsl(var(--chart-2))',
          '3': 'hsl(var(--chart-3))',
          '4': 'hsl(var(--chart-4))',
          '5': 'hsl(var(--chart-5))'
        }
      },
      borderRadius: {
        lg: 'var(--radius)',
        md: 'calc(var(--radius) - 2px)',
        sm: 'calc(var(--radius) - 4px)'
      },
      keyframes: {
        "accordion-down": {
          from: { height: "0" },
          to: { height: "var(--radix-accordion-content-height)" },
        },
        "accordion-up": {
          from: { height: "var(--radix-accordion-content-height)" },
          to: { height: "0" },
        },
        "collapsible-down": {
          from: { 
            height: "0",
            opacity: "0"
          },
          to: { 
            height: "var(--radix-collapsible-content-height)",
            opacity: "1"
          },
        },
        "collapsible-up": {
          from: { 
            height: "var(--radix-collapsible-content-height)",
            opacity: "1"
          },
          to: { 
            height: "0",
            opacity: "0"
          },
        },
      },
      animation: {
        "accordion-down": "accordion-down 0.3s cubic-bezier(0.87, 0, 0.13, 1)",
        "accordion-up": "accordion-up 0.3s cubic-bezier(0.87, 0, 0.13, 1)",
        "collapsible-down": "collapsible-down 1s ease-in-out",
        "collapsible-up": "collapsible-up 1s ease-in-out",
      }
    }
  },
  plugins: [require("tailwindcss-animate")],
}