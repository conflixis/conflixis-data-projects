'use client';

export function Typography() {
  const textSamples = [
    { name: 'Heading 1', element: 'h1', className: 'text-4xl font-bold', text: 'The quick brown fox jumps over the lazy dog' },
    { name: 'Heading 2', element: 'h2', className: 'text-3xl font-semibold', text: 'The quick brown fox jumps over the lazy dog' },
    { name: 'Heading 3', element: 'h3', className: 'text-2xl font-semibold', text: 'The quick brown fox jumps over the lazy dog' },
    { name: 'Heading 4', element: 'h4', className: 'text-xl font-medium', text: 'The quick brown fox jumps over the lazy dog' },
    { name: 'Body Large', element: 'p', className: 'text-lg', text: 'The quick brown fox jumps over the lazy dog' },
    { name: 'Body', element: 'p', className: 'text-base', text: 'The quick brown fox jumps over the lazy dog' },
    { name: 'Body Small', element: 'p', className: 'text-sm', text: 'The quick brown fox jumps over the lazy dog' },
    { name: 'Caption', element: 'p', className: 'text-xs text-muted-foreground', text: 'The quick brown fox jumps over the lazy dog' },
  ];

  const fontFamilies = [
    { name: 'Soehne Leicht', className: 'font-soehneLeicht', description: 'Light weight display font' },
    { name: 'Soehne Kraftig', className: 'font-soehneKraftig', description: 'Bold weight display font' },
    { name: 'Ivar Display', className: 'font-ivarDisplay', description: 'Serif display font' },
  ];

  return (
    <div className="space-y-8">
      <div>
        <h3 className="text-lg font-semibold mb-4">Font Families</h3>
        <div className="space-y-4">
          {fontFamilies.map((font) => (
            <div key={font.name} className="p-4 rounded-lg border border-border">
              <div className="flex items-baseline justify-between mb-2">
                <h4 className="font-medium">{font.name}</h4>
                <span className="text-sm text-muted-foreground">{font.description}</span>
              </div>
              <p className={`text-2xl ${font.className}`}>
                The quick brown fox jumps over the lazy dog
              </p>
              <p className={`text-lg ${font.className} mt-2`}>
                ABCDEFGHIJKLMNOPQRSTUVWXYZ<br />
                abcdefghijklmnopqrstuvwxyz<br />
                0123456789
              </p>
            </div>
          ))}
        </div>
      </div>

      <div>
        <h3 className="text-lg font-semibold mb-4">Text Styles</h3>
        <div className="space-y-4">
          {textSamples.map((sample) => {
            const Element = sample.element as any;
            return (
              <div key={sample.name} className="space-y-2">
                <div className="flex items-baseline gap-4">
                  <span className="text-sm text-muted-foreground w-24">{sample.name}</span>
                  <code className="text-xs bg-muted px-2 py-1 rounded">{sample.className}</code>
                </div>
                <Element className={sample.className}>
                  {sample.text}
                </Element>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}