'use client';

import { useState } from 'react';
import { conflixisColors } from '@workspace/ui/lib/conflixis-theme';
import { Check, Copy } from 'lucide-react';

export function ColorPalette() {
  const [copiedColor, setCopiedColor] = useState<string | null>(null);

  const copyToClipboard = (colorName: string, colorValue: string) => {
    navigator.clipboard.writeText(colorValue);
    setCopiedColor(colorName);
    setTimeout(() => setCopiedColor(null), 2000);
  };

  const colorGroups = {
    'Primary Colors': ['conflixis-green', 'conflixis-light-green', 'conflixis-mint'],
    'Neutral Colors': ['conflixis-white', 'conflixis-gray', 'conflixis-gray-hover', 'conflixis-ivory', 'conflixis-tan'],
    'Accent Colors': ['conflixis-gold', 'conflixis-blue', 'conflixis-red', 'conflixis-success-green'],
  };

  return (
    <div className="space-y-8">
      {Object.entries(colorGroups).map(([groupName, colorNames]) => (
        <div key={groupName}>
          <h3 className="text-lg font-semibold mb-4">{groupName}</h3>
          <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
            {colorNames.map((colorName) => {
              const colorValue = conflixisColors[colorName as keyof typeof conflixisColors];
              return (
                <div
                  key={colorName}
                  className="group relative rounded-lg border border-border overflow-hidden hover:shadow-lg transition-shadow cursor-pointer"
                  onClick={() => copyToClipboard(colorName, colorValue)}
                >
                  <div 
                    className="h-24 w-full"
                    style={{ backgroundColor: colorValue }}
                  />
                  <div className="p-4 bg-background">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="font-medium text-sm">{colorName.replace('conflixis-', '').replace(/-/g, ' ')}</p>
                        <p className="text-xs text-muted-foreground font-mono">{colorValue}</p>
                      </div>
                      <div className="opacity-0 group-hover:opacity-100 transition-opacity">
                        {copiedColor === colorName ? (
                          <Check className="h-4 w-4 text-green-500" />
                        ) : (
                          <Copy className="h-4 w-4 text-muted-foreground" />
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      ))}
    </div>
  );
}