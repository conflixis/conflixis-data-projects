/**
 * Conflixis Typography System
 * Font families and text styles for the Conflixis design system
 */

export const conflixisFonts = {
  soehneLeicht: ['SoehneLeicht', 'sans-serif'],
  soehneKraftig: ['SoehneKraftig', 'sans-serif'],
  ivarDisplay: ['IvarDisplay', 'serif'],
} as const;

export const fontFamilies = [
  { 
    name: 'Soehne Leicht', 
    className: 'font-soehneLeicht', 
    description: 'Light weight display font',
    fontFamily: 'SoehneLeicht',
    usage: 'Body text and general content'
  },
  { 
    name: 'Soehne Kraftig', 
    className: 'font-soehneKraftig', 
    description: 'Bold weight display font',
    fontFamily: 'SoehneKraftig',
    usage: 'Emphasis and buttons'
  },
  { 
    name: 'Ivar Display', 
    className: 'font-ivarDisplay', 
    description: 'Serif display font',
    fontFamily: 'IvarDisplay',
    usage: 'Headings and titles'
  },
];

export const textStyles = [
  { 
    name: 'Heading 1', 
    element: 'h1', 
    className: 'text-4xl font-bold',
    fontSize: '2.25rem',
    lineHeight: '2.5rem',
    fontWeight: 'bold'
  },
  { 
    name: 'Heading 2', 
    element: 'h2', 
    className: 'text-3xl font-semibold',
    fontSize: '1.875rem',
    lineHeight: '2.25rem',
    fontWeight: '600'
  },
  { 
    name: 'Heading 3', 
    element: 'h3', 
    className: 'text-2xl font-semibold',
    fontSize: '1.5rem',
    lineHeight: '2rem',
    fontWeight: '600'
  },
  { 
    name: 'Heading 4', 
    element: 'h4', 
    className: 'text-xl font-medium',
    fontSize: '1.25rem',
    lineHeight: '1.75rem',
    fontWeight: '500'
  },
  { 
    name: 'Body Large', 
    element: 'p', 
    className: 'text-lg',
    fontSize: '1.125rem',
    lineHeight: '1.75rem',
    fontWeight: 'normal'
  },
  { 
    name: 'Body', 
    element: 'p', 
    className: 'text-base',
    fontSize: '1rem',
    lineHeight: '1.5rem',
    fontWeight: 'normal'
  },
  { 
    name: 'Body Small', 
    element: 'p', 
    className: 'text-sm',
    fontSize: '0.875rem',
    lineHeight: '1.25rem',
    fontWeight: 'normal'
  },
  { 
    name: 'Caption', 
    element: 'p', 
    className: 'text-xs text-muted-foreground',
    fontSize: '0.75rem',
    lineHeight: '1rem',
    fontWeight: 'normal'
  },
];

export type ConflixisFont = keyof typeof conflixisFonts;