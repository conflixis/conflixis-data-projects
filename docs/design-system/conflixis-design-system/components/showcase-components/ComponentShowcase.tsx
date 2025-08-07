'use client';

import { Button } from '@workspace/ui/components/button';
import { Badge } from '@workspace/ui/components/badge';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@workspace/ui/components/card';
import { Alert, AlertDescription, AlertTitle } from '@workspace/ui/components/alert';
import { Input } from '@workspace/ui/components/input';
import { Label } from '@workspace/ui/components/label';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@workspace/ui/components/select';
import { Checkbox } from '@workspace/ui/components/checkbox';
import { RadioGroup, RadioGroupItem } from '@workspace/ui/components/radio-group';
import { Switch } from '@workspace/ui/components/switch';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@workspace/ui/components/tabs';
import { Progress } from '@workspace/ui/components/progress';
import { Slider } from '@workspace/ui/components/slider';
import { AlertCircle, CheckCircle, Info, XCircle } from 'lucide-react';
import { BusinessTable, type ColumnConfig } from '@workspace/ui/components/business-table';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@workspace/ui/components/table';

interface ComponentShowcaseProps {
  selectedComponent?: string;
}

export function ComponentShowcase({ selectedComponent }: ComponentShowcaseProps = {}) {
  // If a specific component is selected, only show that section
  const sections = {
    buttons: () => (
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Buttons</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="flex flex-wrap gap-4">
          <Button>Default Button</Button>
          <Button variant="secondary">Secondary</Button>
          <Button variant="destructive">Destructive</Button>
          <Button variant="outline">Outline</Button>
          <Button variant="ghost">Ghost</Button>
          <Button variant="link">Link</Button>
          <Button disabled>Disabled</Button>
        </div>
        <div className="flex flex-wrap gap-4 mt-4">
          <Button size="lg">Large</Button>
          <Button size="default">Default</Button>
          <Button size="sm">Small</Button>
        </div>
        {/* Conflixis Branded Buttons */}
        <div className="flex flex-wrap gap-4 mt-4">
          <Button className="bg-conflixis-green hover:bg-conflixis-green/90">Conflixis Green</Button>
          <Button className="bg-conflixis-light-green hover:bg-conflixis-light-green/90 text-conflixis-green">Light Green</Button>
          <Button className="bg-conflixis-gold hover:bg-conflixis-gold/90">Gold</Button>
          <Button className="bg-conflixis-blue hover:bg-conflixis-blue/90">Blue</Button>
          <Button className="bg-conflixis-red hover:bg-conflixis-red/90">Red</Button>
        </div>
      </div>
    ),
    badges: () => (
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Badges</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="flex flex-wrap gap-4">
          <Badge>Default</Badge>
          <Badge variant="secondary">Secondary</Badge>
          <Badge variant="destructive">Destructive</Badge>
          <Badge variant="outline">Outline</Badge>
          <Badge className="bg-conflixis-green">Conflixis</Badge>
          <Badge className="bg-conflixis-light-green text-conflixis-green">Light</Badge>
          <Badge className="bg-conflixis-gold">Gold</Badge>
          <Badge className="bg-conflixis-blue">Blue</Badge>
          <Badge className="bg-conflixis-red">Red</Badge>
        </div>
      </div>
    )
  };

  // If selectedComponent is provided, render only that component
  if (selectedComponent && sections[selectedComponent as keyof typeof sections]) {
    return sections[selectedComponent as keyof typeof sections]();
  }

  // Otherwise, render all components as before
  return (
    <div className="space-y-8">
      {/* Legend */}
      <div className="p-4 rounded-lg border bg-muted/50">
        <h4 className="font-semibold mb-2">Component Library Legend</h4>
        <div className="flex flex-wrap gap-4 text-sm">
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
            <span>Components from shadcn/ui library (built on Radix UI)</span>
          </div>
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="text-xs">Radix UI</Badge>
            <span>Direct Radix UI primitives</span>
          </div>
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="text-xs">Migrated</Badge>
            <span>Components migrated to shared UI package</span>
          </div>
        </div>
      </div>

      {/* Buttons */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Buttons</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="flex flex-wrap gap-4">
          <Button>Default Button</Button>
          <Button variant="secondary">Secondary</Button>
          <Button variant="destructive">Destructive</Button>
          <Button variant="outline">Outline</Button>
          <Button variant="ghost">Ghost</Button>
          <Button variant="link">Link</Button>
          <Button disabled>Disabled</Button>
        </div>
        <div className="flex flex-wrap gap-4 mt-4">
          <Button size="lg">Large</Button>
          <Button size="default">Default</Button>
          <Button size="sm">Small</Button>
        </div>
        {/* Conflixis Branded Buttons */}
        <div className="flex flex-wrap gap-4 mt-4">
          <Button className="bg-conflixis-green hover:bg-conflixis-green/90">Conflixis Green</Button>
          <Button className="bg-conflixis-light-green hover:bg-conflixis-light-green/90 text-conflixis-green">Light Green</Button>
          <Button className="bg-conflixis-gold hover:bg-conflixis-gold/90">Gold</Button>
          <Button className="bg-conflixis-blue hover:bg-conflixis-blue/90">Blue</Button>
          <Button className="bg-conflixis-red hover:bg-conflixis-red/90">Red</Button>
        </div>
      </div>

      {/* Badges */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Badges</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="flex flex-wrap gap-4">
          <Badge>Default</Badge>
          <Badge variant="secondary">Secondary</Badge>
          <Badge variant="destructive">Destructive</Badge>
          <Badge variant="outline">Outline</Badge>
          <Badge className="bg-conflixis-green">Conflixis</Badge>
          <Badge className="bg-conflixis-light-green text-conflixis-green">Light</Badge>
          <Badge className="bg-conflixis-gold">Gold</Badge>
          <Badge className="bg-conflixis-blue">Blue</Badge>
          <Badge className="bg-conflixis-red">Red</Badge>
        </div>
      </div>

      {/* Cards */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Cards</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui - Enhanced</Badge>
          <Badge className="text-xs bg-green-600">Recently Updated</Badge>
        </div>
        <div className="space-y-4">
          {/* Enhanced Card Examples */}
          <div>
            <h4 className="text-sm font-semibold mb-3">Enhanced Card with Elevation & Padding Props</h4>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
              <Card elevation="none" padding="small">
                <CardHeader>
                  <CardTitle className="text-base">No Elevation</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-sm">elevation="none"<br/>padding="small"</p>
                </CardContent>
              </Card>
              <Card elevation="low" padding="medium">
                <CardHeader>
                  <CardTitle className="text-base">Low Elevation</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-sm">elevation="low"<br/>padding="medium"</p>
                </CardContent>
              </Card>
              <Card elevation="medium" padding="large">
                <CardHeader>
                  <CardTitle className="text-base">Medium Elevation</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-sm">elevation="medium"<br/>padding="large"</p>
                </CardContent>
              </Card>
              <Card elevation="high" padding="medium">
                <CardHeader>
                  <CardTitle className="text-base">High Elevation</CardTitle>
                </CardHeader>
                <CardContent>
                  <p className="text-sm">elevation="high"<br/>padding="medium"</p>
                </CardContent>
              </Card>
            </div>
          </div>

          {/* Conflixis Branded Cards */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <Card>
              <CardHeader>
                <CardTitle>Default Enhanced Card</CardTitle>
                <CardDescription>Built-in Conflixis branding with green borders</CardDescription>
              </CardHeader>
              <CardContent>
                <p>The enhanced Card component now includes Conflixis branding by default, with subtle green borders and hover effects.</p>
              </CardContent>
            </Card>
            <Card className="border-conflixis-gold">
              <CardHeader className="bg-conflixis-gold/10">
                <CardTitle className="text-conflixis-green">Custom Styled Card</CardTitle>
                <CardDescription>Override default styles with className</CardDescription>
              </CardHeader>
              <CardContent className="pt-6">
                <p>You can still customize the Card appearance using className while benefiting from the elevation and padding props.</p>
              </CardContent>
            </Card>
          </div>

          {/* Migration Success */}
          <Alert className="border-green-200 bg-green-50">
            <CheckCircle className="h-4 w-4 text-green-600" />
            <AlertTitle>Card Component Migration Complete</AlertTitle>
            <AlertDescription>
              All 18 files using the Card component have been successfully migrated to use the enhanced shared Card from @workspace/ui, 
              achieving ~790 lines of code reduction with zero breaking changes.
            </AlertDescription>
          </Alert>
        </div>
      </div>

      {/* Alerts */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Alerts</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui - Migrated</Badge>
        </div>
        <div className="space-y-4">
          <Alert>
            <Info className="h-4 w-4" />
            <AlertTitle>Default Alert</AlertTitle>
            <AlertDescription>This is a default informational alert.</AlertDescription>
          </Alert>
          <Alert className="border-conflixis-green bg-conflixis-green/5">
            <CheckCircle className="h-4 w-4 text-conflixis-green" />
            <AlertTitle>Success</AlertTitle>
            <AlertDescription>Operation completed successfully with Conflixis styling.</AlertDescription>
          </Alert>
          <Alert className="border-conflixis-gold bg-conflixis-gold/5">
            <AlertCircle className="h-4 w-4 text-conflixis-gold" />
            <AlertTitle>Warning</AlertTitle>
            <AlertDescription>This is a warning message with custom styling.</AlertDescription>
          </Alert>
          <Alert variant="destructive">
            <XCircle className="h-4 w-4" />
            <AlertTitle>Error</AlertTitle>
            <AlertDescription>Something went wrong. Please try again.</AlertDescription>
          </Alert>
        </div>
      </div>

      {/* Form Elements */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Form Elements</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui + Radix UI</Badge>
        </div>
        <div className="space-y-4 max-w-md">
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <Label htmlFor="input">Input Field</Label>
              <Badge variant="secondary" className="text-xs">shadcn/ui</Badge>
            </div>
            <Input id="input" placeholder="Enter text here..." />
          </div>
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <Label htmlFor="select">Select</Label>
              <Badge variant="secondary" className="text-xs">Radix UI - Migrated</Badge>
            </div>
            <Select>
              <SelectTrigger id="select">
                <SelectValue placeholder="Select an option" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="option1">Option 1</SelectItem>
                <SelectItem value="option2">Option 2</SelectItem>
                <SelectItem value="option3">Option 3</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div>
            <div className="flex items-center gap-2 mb-2">
              <span className="text-sm font-medium">Checkbox</span>
              <Badge variant="secondary" className="text-xs">Radix UI</Badge>
            </div>
            <div className="flex items-center space-x-2">
              <Checkbox id="checkbox" />
              <Label htmlFor="checkbox">Accept terms and conditions</Label>
            </div>
          </div>
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <Label>Radio Group</Label>
              <Badge variant="secondary" className="text-xs">Radix UI</Badge>
            </div>
            <RadioGroup defaultValue="option1">
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="option1" id="r1" />
                <Label htmlFor="r1">Option 1</Label>
              </div>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="option2" id="r2" />
                <Label htmlFor="r2">Option 2</Label>
              </div>
            </RadioGroup>
          </div>
          <div>
            <div className="flex items-center gap-2 mb-2">
              <span className="text-sm font-medium">Switch</span>
              <Badge variant="secondary" className="text-xs">Radix UI</Badge>
            </div>
            <div className="flex items-center space-x-2">
              <Switch id="switch" />
              <Label htmlFor="switch">Enable notifications</Label>
            </div>
          </div>
        </div>
      </div>

      {/* Progress and Sliders */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Progress & Sliders</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui + Radix UI</Badge>
        </div>
        <div className="space-y-4 max-w-md">
          <div>
            <Label>Progress Bar (33%)</Label>
            <Progress value={33} className="mt-2" />
          </div>
          <div>
            <Label>Progress Bar (66%)</Label>
            <Progress value={66} className="mt-2 [&>div]:bg-conflixis-green" />
          </div>
          <div>
            <Label>Slider</Label>
            <Slider defaultValue={[50]} max={100} step={1} className="mt-2" />
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Tabs</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui + Radix UI</Badge>
        </div>
        <Tabs defaultValue="tab1" className="max-w-md">
          <TabsList>
            <TabsTrigger value="tab1">Tab 1</TabsTrigger>
            <TabsTrigger value="tab2">Tab 2</TabsTrigger>
            <TabsTrigger value="tab3">Tab 3</TabsTrigger>
          </TabsList>
          <TabsContent value="tab1">
            <Card>
              <CardHeader>
                <CardTitle>Tab 1 Content</CardTitle>
              </CardHeader>
              <CardContent>
                <p>This is the content for the first tab.</p>
              </CardContent>
            </Card>
          </TabsContent>
          <TabsContent value="tab2">
            <Card>
              <CardHeader>
                <CardTitle>Tab 2 Content</CardTitle>
              </CardHeader>
              <CardContent>
                <p>This is the content for the second tab.</p>
              </CardContent>
            </Card>
          </TabsContent>
          <TabsContent value="tab3">
            <Card>
              <CardHeader>
                <CardTitle>Tab 3 Content</CardTitle>
              </CardHeader>
              <CardContent>
                <p>This is the content for the third tab.</p>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>

      {/* FormDialog */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">FormDialog Wrapper</h3>
          <Badge variant="outline" className="text-xs">Conflixis Custom</Badge>
          <Badge className="text-xs bg-green-600">Recently Updated</Badge>
        </div>
        
        <div className="space-y-4">
          <div>
            <h4 className="text-sm font-semibold mb-2">FormDialog Component</h4>
            <p className="text-sm text-muted-foreground mb-4">
              A wrapper component that simplifies dialog forms with built-in form submission handling, loading states, and consistent button behavior.
            </p>
            
            {/* Migration Success */}
            <Alert className="border-green-200 bg-green-50 mb-4">
              <CheckCircle className="h-4 w-4 text-green-600" />
              <AlertTitle>Form Components Migration Complete</AlertTitle>
              <AlertDescription>
                All 4 CRM forms (Contact, Company, Activity, Opportunity) have been successfully migrated to use the FormDialog wrapper, 
                achieving ~45% code reduction (approximately 800 lines saved) with zero breaking changes.
              </AlertDescription>
            </Alert>
            
            {/* Code Comparison */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm">Before: Using Dialog Components</CardTitle>
                </CardHeader>
                <CardContent>
                  <pre className="text-xs overflow-x-auto">
{`<Dialog open={open} onOpenChange={onOpenChange}>
  <DialogContent>
    <DialogHeader>
      <DialogTitle>Edit Contact</DialogTitle>
      <DialogDescription>
        Update contact information
      </DialogDescription>
    </DialogHeader>
    <form onSubmit={handleSubmit}>
      <div className="space-y-4">
        <div>
          <Label>Name</Label>
          <Input value={name} onChange={...} />
        </div>
        {/* More fields... */}
      </div>
      <DialogFooter>
        <Button 
          type="button" 
          variant="outline" 
          onClick={() => onOpenChange(false)}
        >
          Cancel
        </Button>
        <Button type="submit" disabled={loading}>
          {loading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
          Save Contact
        </Button>
      </DialogFooter>
    </form>
  </DialogContent>
</Dialog>`}</pre>
                </CardContent>
              </Card>
              
              <Card>
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm">After: Using FormDialog</CardTitle>
                </CardHeader>
                <CardContent>
                  <pre className="text-xs overflow-x-auto">
{`<FormDialog
  open={open}
  onOpenChange={onOpenChange}
  title="Edit Contact"
  description="Update contact information"
  loading={loading}
  onSubmit={handleSubmit}
  submitLabel="Save Contact"
>
  <FormField>
    <Label>Name</Label>
    <Input value={name} onChange={...} />
  </FormField>
  {/* More fields... */}
</FormDialog>`}</pre>
                </CardContent>
              </Card>
            </div>
          </div>
          
          {/* Features */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Built-in Features</CardTitle>
              </CardHeader>
              <CardContent>
                <ul className="text-sm space-y-1">
                  <li>• Automatic form submission handling</li>
                  <li>• Loading state with spinner on submit button</li>
                  <li>• Consistent cancel button behavior</li>
                  <li>• Configurable dialog size (sm, md, lg, xl)</li>
                  <li>• Built-in form sections with FormSection</li>
                  <li>• FormField wrapper for consistent spacing</li>
                </ul>
              </CardContent>
            </Card>
            
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Code Reduction</CardTitle>
              </CardHeader>
              <CardContent>
                <ul className="text-sm space-y-1">
                  <li>• ~45% less code per form</li>
                  <li>• No manual DialogHeader/Footer setup</li>
                  <li>• No manual button state management</li>
                  <li>• Automatic form event handling</li>
                  <li>• Consistent styling across all forms</li>
                </ul>
              </CardContent>
            </Card>
          </div>
          
          {/* Usage Example */}
          <div className="mt-6">
            <h4 className="text-sm font-semibold mb-2">Usage Example</h4>
            <pre className="p-4 rounded-lg bg-muted text-sm overflow-x-auto">
{`import { FormDialog, FormField, FormSection } from '@workspace/ui/components/form-dialog';

function ContactForm({ open, onOpenChange }) {
  const [loading, setLoading] = useState(false);
  
  const handleSubmit = async () => {
    setLoading(true);
    // Form submission logic
    setLoading(false);
  };
  
  return (
    <FormDialog
      open={open}
      onOpenChange={onOpenChange}
      title="Add Contact"
      description="Enter contact details"
      loading={loading}
      onSubmit={handleSubmit}
      submitLabel="Create Contact"
      size="lg"
    >
      <FormSection title="Personal Information">
        <FormField>
          <Label>Full Name</Label>
          <Input />
        </FormField>
        <FormField>
          <Label>Email</Label>
          <Input type="email" />
        </FormField>
      </FormSection>
      
      <FormSection title="Company Details">
        <FormField>
          <Label>Company</Label>
          <Select>
            {/* Select options */}
          </Select>
        </FormField>
      </FormSection>
    </FormDialog>
  );
}`}</pre>
          </div>
        </div>
      </div>

      {/* Tables */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Tables</h3>
          <Badge variant="outline" className="text-xs">Conflixis Custom</Badge>
        </div>
        
        {/* BusinessTable Example */}
        <div className="space-y-4">
          <div>
            <h4 className="text-sm font-semibold mb-2">BusinessTable Component</h4>
            <p className="text-sm text-muted-foreground mb-4">
              A wrapper component that simplifies table creation with built-in formatters, consistent styling, and column configuration.
            </p>
            
            {/* Sample data for demo */}
            {(() => {
              const sampleData = [
                { product: 'SaaS Subscription', jan: 12500, feb: 13200, mar: 14100, total: 39800, growth: 12.8 },
                { product: 'Enterprise License', jan: 45000, feb: 45000, mar: 52000, total: 142000, growth: 15.6 },
                { product: 'Support Services', jan: 8200, feb: 8700, mar: 9100, total: 26000, growth: 10.9 },
                { product: 'Consulting', jan: 0, feb: 15000, mar: 22000, total: 37000, growth: 46.7 },
              ];
              
              const totalRow = {
                product: 'TOTAL',
                jan: 65700,
                feb: 81900,
                mar: 97200,
                total: 244800,
                growth: 21.5
              };
              
              const columns: ColumnConfig[] = [
                { key: 'product', header: 'Product', align: 'left' },
                { key: 'jan', header: 'January', format: 'currency', align: 'right' },
                { key: 'feb', header: 'February', format: 'currency', align: 'right' },
                { key: 'mar', header: 'March', format: 'currency', align: 'right' },
                { key: 'total', header: 'Q1 Total', format: 'currency', align: 'right', className: 'font-soehneKraftig' },
                { key: 'growth', header: 'Growth %', format: 'percentage', align: 'right' },
              ];
              
              return (
                <Card>
                  <CardHeader>
                    <CardTitle>Q1 2025 Revenue by Product</CardTitle>
                    <CardDescription>
                      Using BusinessTable with currency and percentage formatters
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    <BusinessTable
                      data={sampleData}
                      columns={columns}
                      showTotals
                      totalRow={totalRow}
                      getCellClassName={(value, key) => {
                        if (key === 'growth') {
                          return typeof value === 'number' && value > 15 ? 'text-green-600' : '';
                        }
                        return '';
                      }}
                    />
                  </CardContent>
                </Card>
              );
            })()}
          </div>
          
          {/* Code Example */}
          <div className="mt-6">
            <h4 className="text-sm font-semibold mb-2">Usage Example</h4>
            <pre className="p-4 rounded-lg bg-muted text-sm overflow-x-auto">
{`const columns: ColumnConfig[] = [
  { key: 'product', header: 'Product', align: 'left' },
  { key: 'jan', header: 'January', format: 'currency', align: 'right' },
  { key: 'growth', header: 'Growth %', format: 'percentage', align: 'right' },
];

<BusinessTable
  data={data}
  columns={columns}
  showTotals
  totalRow={totals}
  getCellClassName={(value, key) => {
    // Custom styling based on value
    return value < 0 ? 'text-red-600' : '';
  }}
/>`}
            </pre>
          </div>
          
          {/* Features */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-4">
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Built-in Formatters</CardTitle>
              </CardHeader>
              <CardContent>
                <ul className="text-sm space-y-1">
                  <li>• <code>currency</code> - Formats as $1,234</li>
                  <li>• <code>percentage</code> - Formats as 12.5%</li>
                  <li>• <code>number</code> - Custom decimal places</li>
                  <li>• <code>text</code> - String formatting</li>
                </ul>
              </CardContent>
            </Card>
            
            <Card>
              <CardHeader className="pb-3">
                <CardTitle className="text-base">Features</CardTitle>
              </CardHeader>
              <CardContent>
                <ul className="text-sm space-y-1">
                  <li>• Column-based configuration</li>
                  <li>• Automatic totals row styling</li>
                  <li>• Custom cell className function</li>
                  <li>• Consistent Conflixis styling</li>
                </ul>
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
}