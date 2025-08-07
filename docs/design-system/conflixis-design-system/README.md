# Conflixis Design System

A comprehensive design system extracted from the Conflixis platform, featuring brand colors, typography, animations, and components built on top of shadcn/ui.

## 🎨 Overview

This design system provides all the visual assets and components needed to create web applications with consistent Conflixis branding. It includes:

- **Brand Colors**: Complete color palette with primary, neutral, and accent colors
- **Typography**: Custom fonts (Soehne and Ivar families) with defined text styles
- **Animations**: Smooth, professional animations and transitions
- **Components**: Pre-styled UI components based on shadcn/ui
- **Patterns**: Complex reusable patterns for common UI tasks

## 📁 Directory Structure

```
conflixis-design-system/
├── assets/                 # Static assets
│   ├── logos/             # Conflixis logo files
│   └── fonts/             # Font files (woff2 format)
├── foundations/           # Core design tokens
│   ├── colors.ts          # Color definitions
│   ├── typography.ts      # Font and text styles
│   └── animations.css     # Animation keyframes
├── components/            # React components
│   ├── showcase-components/  # Design showcase components
│   └── ui-components/        # Reusable UI components
├── patterns/              # Complex UI patterns
├── config/                # Configuration files
│   ├── tailwind.config.js   # Tailwind with Conflixis theme
│   ├── globals.css          # Global styles and font-face
│   └── components.json      # shadcn/ui configuration
└── examples/              # Usage examples
    ├── standalone-page.html  # Simple HTML example
    └── react-example.tsx     # React component example
```

## 🚀 Quick Start

### For Simple HTML Projects

1. Include the fonts and styles in your HTML:

```html
<!DOCTYPE html>
<html>
<head>
    <!-- Include Tailwind CSS -->
    <script src="https://cdn.tailwindcss.com"></script>
    
    <!-- Configure Tailwind with Conflixis colors -->
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    colors: {
                        'conflixis-green': '#0c343a',
                        'conflixis-light-green': '#93baab',
                        'conflixis-gold': '#eab96d',
                        'conflixis-blue': '#4c94ed',
                        'conflixis-red': '#fd7649',
                        // ... other colors
                    }
                }
            }
        }
    </script>
    
    <!-- Link to fonts -->
    <link rel="stylesheet" href="path/to/globals.css">
</head>
<body class="bg-conflixis-ivory text-conflixis-green">
    <!-- Your content -->
</body>
</html>
```

### For React/Next.js Projects

1. **Install shadcn/ui** (if not already installed):
```bash
npx shadcn-ui@latest init
```

2. **Copy the Tailwind configuration**:
```bash
cp conflixis-design-system/config/tailwind.config.js ./
```

3. **Copy font files** to your public directory:
```bash
cp -r conflixis-design-system/assets/fonts ./public/fonts
```

4. **Import global styles** in your main CSS file:
```css
@import 'conflixis-design-system/config/globals.css';
```

5. **Use the components**:
```tsx
import { conflixisColors } from './foundations/colors';

function MyComponent() {
  return (
    <div className="bg-conflixis-green text-white p-6 rounded-lg">
      <h1 className="font-ivarDisplay text-3xl">Welcome to Conflixis</h1>
      <p className="font-soehneLeicht">Building the future of healthcare analytics</p>
    </div>
  );
}
```

## 🎨 Brand Colors

### Primary Colors
- **Conflixis Green** `#0c343a` - Primary brand color for headers and main actions
- **Light Green/Mint** `#93baab` - Secondary color for accents

### Neutral Colors
- **Ivory** `#f8f5ef` - Main background color
- **Gray** `#d1d5db` - Borders and dividers
- **Tan** `#c5bdae` - Subtle backgrounds

### Accent Colors
- **Gold** `#eab96d` - Highlights and important information
- **Blue** `#4c94ed` - Information and links
- **Red** `#fd7649` - Errors and alerts

## 📝 Typography

### Font Families

1. **Soehne Leicht** (Light)
   - Usage: Body text, descriptions
   - Class: `font-soehneLeicht`

2. **Soehne Kraftig** (Bold)
   - Usage: Buttons, emphasis
   - Class: `font-soehneKraftig`

3. **Ivar Display** (Serif)
   - Usage: Headings, titles
   - Class: `font-ivarDisplay`

### Text Styles

```css
/* Headings */
.text-h1 { @apply text-4xl font-bold font-ivarDisplay; }
.text-h2 { @apply text-3xl font-semibold font-ivarDisplay; }
.text-h3 { @apply text-2xl font-semibold font-ivarDisplay; }

/* Body */
.text-body { @apply text-base font-soehneLeicht; }
.text-body-large { @apply text-lg font-soehneLeicht; }
.text-body-small { @apply text-sm font-soehneLeicht; }
```

## ✨ Animations

The design system includes various animation utilities:

### Basic Animations
- `.animate-fade-in` - Fade in with upward motion
- `.animate-slide-in` - Slide in from left
- `.animate-chart-grow` - Grow from bottom (for charts)

### Special Effects
- `.animate-pulse-slow` - Gentle pulsing effect
- `.animate-spin-slow` - Slow rotation
- `.animate-wave-[1-5]` - Wave effects with delays
- `.animate-gradient-shift` - Gradient color transition
- `.animate-glow` - Glowing shadow effect

### Loading States
- `.animate-pulse-dot-[1-3]` - Sequential dot loading
- `.animate-progress` - Progress bar animation

## 🧩 Components

### Buttons
```html
<!-- Primary Button -->
<button class="bg-conflixis-green hover:bg-opacity-90 text-white px-6 py-3 rounded-lg font-soehneKraftig">
  Get Started
</button>

<!-- Outline Button -->
<button class="border-2 border-conflixis-green text-conflixis-green hover:bg-conflixis-green hover:text-white px-6 py-3 rounded-lg">
  Learn More
</button>
```

### Cards
```html
<div class="bg-white rounded-lg shadow-sm hover:shadow-md transition-shadow p-6">
  <h3 class="text-xl font-soehneKraftig mb-2">Card Title</h3>
  <p class="text-gray-600">Card content goes here</p>
</div>
```

### Badges
```html
<span class="inline-flex items-center px-3 py-1 rounded-full text-sm font-soehneKraftig bg-conflixis-green text-white">
  Active
</span>
```

## 🔧 Integration with shadcn/ui

This design system is built to work seamlessly with shadcn/ui components. The color tokens map to CSS variables used by shadcn:

```css
:root {
  --primary: /* maps to conflixis-green */
  --secondary: /* maps to conflixis-light-green */
  --accent: /* maps to conflixis-gold */
  /* ... other mappings */
}
```

To use shadcn components with Conflixis styling:

1. Install the component:
```bash
npx shadcn-ui@latest add button
```

2. The component will automatically use Conflixis colors through the configured theme

## 📚 Examples

Check the `examples/` directory for:

- **standalone-page.html** - Complete HTML page with all design elements
- **react-example.tsx** - React dashboard component with tabs, KPI cards, and more

## 🛠️ Customization

To customize the design system:

1. **Colors**: Edit `foundations/colors.ts`
2. **Typography**: Modify `foundations/typography.ts`
3. **Animations**: Add new keyframes to `foundations/animations.css`
4. **Tailwind**: Update `config/tailwind.config.js`

## 📋 Best Practices

1. **Color Usage**
   - Use `conflixis-green` for primary actions and headers
   - Use `conflixis-ivory` as the main background
   - Use accent colors sparingly for emphasis

2. **Typography**
   - Use Ivar Display for all headings
   - Use Soehne Leicht for body text
   - Use Soehne Kraftig for buttons and emphasis

3. **Spacing**
   - Follow Tailwind's spacing scale
   - Maintain consistent padding (p-4, p-6, p-8)
   - Use rounded-lg for cards and containers

4. **Animations**
   - Use subtle animations for better UX
   - Apply `animate-fade-in` to page sections
   - Use `hover:shadow-md` for interactive elements

## 🤝 Contributing

When adding new components or patterns:

1. Follow the existing file structure
2. Document new additions in this README
3. Include usage examples
4. Ensure compatibility with both light and dark modes (if applicable)

## 📄 License

This design system is proprietary to Conflixis. All rights reserved.

---

For questions or support, please contact the Conflixis development team.