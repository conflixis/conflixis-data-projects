/**
 * Conflixis Brand Colors
 * Official color palette for the Conflixis design system
 */

export const conflixisColors = {
  // Primary Colors
  'conflixis-green': '#0c343a',        // Primary brand color
  'conflixis-light-green': '#93baab',  // Secondary/accent
  'conflixis-mint': '#93baab',         // Alias for light-green
  
  // Neutral Colors
  'conflixis-white': '#ffffff',
  'conflixis-gray': '#d1d5db',
  'conflixis-gray-hover': '#b9bec4',
  'conflixis-ivory': '#f8f5ef',        // Background color
  'conflixis-tan': '#c5bdae',
  
  // Accent Colors
  'conflixis-gold': '#eab96d',         // Highlights
  'conflixis-blue': '#4c94ed',         // Information
  'conflixis-red': '#fd7649',          // Alerts/errors
  'conflixis-success-green': '#008000FF',
} as const;

export const colorGroups = {
  'Primary Colors': ['conflixis-green', 'conflixis-light-green', 'conflixis-mint'],
  'Neutral Colors': ['conflixis-white', 'conflixis-gray', 'conflixis-gray-hover', 'conflixis-ivory', 'conflixis-tan'],
  'Accent Colors': ['conflixis-gold', 'conflixis-blue', 'conflixis-red', 'conflixis-success-green'],
};

// Color usage guidelines
export const colorGuidelines = {
  primary: {
    color: 'conflixis-green',
    hex: '#0c343a',
    usage: 'Primary actions, headers, and brand identity'
  },
  secondary: {
    color: 'conflixis-light-green',
    hex: '#93baab',
    usage: 'Secondary elements and accents'
  },
  accent: {
    gold: {
      color: 'conflixis-gold',
      hex: '#eab96d',
      usage: 'Highlights and important information'
    },
    blue: {
      color: 'conflixis-blue',
      hex: '#4c94ed',
      usage: 'Information and data visualization'
    },
    red: {
      color: 'conflixis-red',
      hex: '#fd7649',
      usage: 'Alerts, errors, and warnings'
    }
  },
  background: {
    color: 'conflixis-ivory',
    hex: '#f8f5ef',
    usage: 'Main background color for pages'
  }
};

export type ConflixisColor = keyof typeof conflixisColors;