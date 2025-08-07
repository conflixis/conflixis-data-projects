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
import { AlertCircle, CheckCircle, Info, XCircle, Mail, Calendar, Phone, FileText, Activity, Clock, ChevronDown, Users, Building2, DollarSign } from 'lucide-react';
import { BusinessTable, type ColumnConfig } from '@workspace/ui/components/business-table';
import { KPICard } from '@workspace/ui/components/kpi-card';
import { FormDialog, FormField, FormSection } from '@workspace/ui/components/form-dialog';
import { AnimatedCollapsible } from '@/components/AnimatedCollapsible';
import { useState } from 'react';
import { cn } from '@/lib/utils';

export const componentStories = {
  buttons: {
    title: 'Buttons',
    description: 'Interactive button components with various styles and states',
    render: () => <ButtonStory />
  },
  badges: {
    title: 'Badges',
    description: 'Small count and labeling components',
    render: () => <BadgeStory />
  },
  cards: {
    title: 'Cards',
    description: 'Container components with elevation and padding options',
    render: () => <CardStory />
  },
  alerts: {
    title: 'Alerts',
    description: 'Contextual feedback messages for user actions',
    render: () => <AlertStory />
  },
  forms: {
    title: 'Form Elements',
    description: 'Input fields, selects, checkboxes, and other form controls',
    render: () => <FormStory />
  },
  progress: {
    title: 'Progress & Sliders',
    description: 'Visual indicators for progress and value selection',
    render: () => <ProgressStory />
  },
  tabs: {
    title: 'Tabs',
    description: 'Organize content into tabbed sections',
    render: () => <TabStory />
  },
  tables: {
    title: 'Tables',
    description: 'Display tabular data with sorting and formatting',
    render: () => <TableStory />
  },
  timeline: {
    title: 'Timeline',
    description: 'Chronological display of events with Conflixis styling',
    render: () => <TimelineStory />
  },
  'animated-collapsible': {
    title: 'Animated Collapsible',
    description: 'Smooth expanding and collapsing sections with Framer Motion animations',
    render: () => <AnimatedCollapsibleStory />
  },
  'form-dialog': {
    title: 'FormDialog Pattern',
    description: 'Simplified dialog forms with built-in loading and submission handling',
    render: () => <FormDialogStory />
  },
  'business-table': {
    title: 'BusinessTable Pattern',
    description: 'Powerful table component with built-in formatters',
    render: () => <BusinessTableStory />
  },
  'kpi-card': {
    title: 'KPICard Pattern',
    description: 'Key performance indicator display cards',
    render: () => <KPICardStory />
  },
  'healthcare-professional-profile': {
    title: 'Healthcare Professional Profile Mockup',
    description: 'Comprehensive healthcare professional profile with all 16 sections',
    render: () => (
      <div className="text-center space-y-4">
        <p className="text-muted-foreground">
          A full mockup of the healthcare professional profile page showing all data sections
        </p>
        <div className="flex gap-4 justify-center">
          <a href="/modules/showcase/healthcare-professional-profile-simple" target="_blank">
            <Button>View Simple Mockup</Button>
          </a>
          <a href="/modules/showcase/healthcare-professional-profile-mockup" target="_blank">
            <Button variant="outline">View Full Mockup (with tabs)</Button>
          </a>
        </div>
      </div>
    )
  }
};

function ButtonStory() {
  return (
    <div className="space-y-8">
      <div>
        <h3 className="text-sm font-semibold mb-4">Variants</h3>
        <div className="flex flex-wrap gap-4">
          <Button>Default</Button>
          <Button variant="secondary">Secondary</Button>
          <Button variant="destructive">Destructive</Button>
          <Button variant="outline">Outline</Button>
          <Button variant="ghost">Ghost</Button>
          <Button variant="link">Link</Button>
        </div>
      </div>

      <div>
        <h3 className="text-sm font-semibold mb-4">Sizes</h3>
        <div className="flex flex-wrap items-center gap-4">
          <Button size="lg">Large</Button>
          <Button size="default">Default</Button>
          <Button size="sm">Small</Button>
        </div>
      </div>

      <div>
        <h3 className="text-sm font-semibold mb-4">Conflixis Branded</h3>
        <div className="flex flex-wrap gap-4">
          <Button className="bg-conflixis-green hover:bg-conflixis-green/90">Conflixis Green</Button>
          <Button className="bg-conflixis-light-green hover:bg-conflixis-light-green/90 text-conflixis-green">Light Green</Button>
          <Button className="bg-conflixis-gold hover:bg-conflixis-gold/90">Gold</Button>
          <Button className="bg-conflixis-blue hover:bg-conflixis-blue/90">Blue</Button>
          <Button className="bg-conflixis-red hover:bg-conflixis-red/90">Red</Button>
        </div>
      </div>
    </div>
  );
}

function BadgeStory() {
  return (
    <div className="space-y-8">
      <div>
        <h3 className="text-sm font-semibold mb-4">Variants</h3>
        <div className="flex flex-wrap gap-4">
          <Badge>Default</Badge>
          <Badge variant="secondary">Secondary</Badge>
          <Badge variant="destructive">Destructive</Badge>
          <Badge variant="outline">Outline</Badge>
        </div>
      </div>

      <div>
        <h3 className="text-sm font-semibold mb-4">Conflixis Branded</h3>
        <div className="flex flex-wrap gap-4">
          <Badge className="bg-conflixis-green">Conflixis</Badge>
          <Badge className="bg-conflixis-light-green text-conflixis-green">Light</Badge>
          <Badge className="bg-conflixis-gold">Gold</Badge>
          <Badge className="bg-conflixis-blue">Blue</Badge>
          <Badge className="bg-conflixis-red">Red</Badge>
        </div>
      </div>
    </div>
  );
}

function CardStory() {
  return (
    <div className="space-y-8">
      <div>
        <h3 className="text-sm font-semibold mb-4">Elevation Variants</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          <Card elevation="none" padding="small">
            <CardHeader>
              <CardTitle className="text-base">No Elevation</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm">elevation="none"</p>
            </CardContent>
          </Card>
          <Card elevation="low" padding="medium">
            <CardHeader>
              <CardTitle className="text-base">Low Elevation</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm">elevation="low"</p>
            </CardContent>
          </Card>
          <Card elevation="medium" padding="large">
            <CardHeader>
              <CardTitle className="text-base">Medium Elevation</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm">elevation="medium"</p>
            </CardContent>
          </Card>
          <Card elevation="high" padding="medium">
            <CardHeader>
              <CardTitle className="text-base">High Elevation</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm">elevation="high"</p>
            </CardContent>
          </Card>
        </div>
      </div>

      <Alert className="border-green-200 bg-green-50">
        <CheckCircle className="h-4 w-4 text-green-600" />
        <AlertTitle>Card Component Enhanced</AlertTitle>
        <AlertDescription>
          The Card component now includes Conflixis branding by default, with subtle green borders and hover effects.
        </AlertDescription>
      </Alert>
    </div>
  );
}

function AlertStory() {
  return (
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
  );
}

function FormStory() {
  const [switchEnabled, setSwitchEnabled] = useState(false);
  
  return (
    <div className="space-y-6 max-w-md">
      <div className="space-y-2">
        <Label htmlFor="input">Input Field</Label>
        <Input id="input" placeholder="Enter text here..." />
      </div>
      
      <div className="space-y-2">
        <Label htmlFor="select">Select</Label>
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
      
      <div className="flex items-center space-x-2">
        <Checkbox id="checkbox" />
        <Label htmlFor="checkbox">Accept terms and conditions</Label>
      </div>
      
      <div className="space-y-2">
        <Label>Radio Group</Label>
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
      
      <div className="flex items-center space-x-2">
        <Switch id="switch" checked={switchEnabled} onCheckedChange={setSwitchEnabled} />
        <Label htmlFor="switch">Enable notifications</Label>
      </div>
    </div>
  );
}

function ProgressStory() {
  return (
    <div className="space-y-6 max-w-md">
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
  );
}

function TabStory() {
  return (
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
  );
}

function TableStory() {
  const sampleData = [
    { product: 'SaaS Subscription', jan: 12500, feb: 13200, mar: 14100, total: 39800, growth: 12.8 },
    { product: 'Enterprise License', jan: 45000, feb: 45000, mar: 52000, total: 142000, growth: 15.6 },
    { product: 'Support Services', jan: 8200, feb: 8700, mar: 9100, total: 26000, growth: 10.9 },
  ];
  
  const columns: ColumnConfig[] = [
    { key: 'product', header: 'Product', align: 'left' },
    { key: 'jan', header: 'January', format: 'currency', align: 'right' },
    { key: 'feb', header: 'February', format: 'currency', align: 'right' },
    { key: 'mar', header: 'March', format: 'currency', align: 'right' },
    { key: 'total', header: 'Q1 Total', format: 'currency', align: 'right', className: 'font-soehneKraftig' },
    { key: 'growth', header: 'Growth %', format: 'percentage', align: 'right' },
  ];
  
  return (
    <div>
      <h3 className="text-sm font-semibold mb-4">BusinessTable with Formatters</h3>
      <BusinessTable
        data={sampleData}
        columns={columns}
        getCellClassName={(value, key) => {
          if (key === 'growth') {
            return typeof value === 'number' && value > 15 ? 'text-green-600' : '';
          }
          return '';
        }}
      />
    </div>
  );
}

function FormDialogStory() {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  
  const handleSubmit = async () => {
    setLoading(true);
    await new Promise(resolve => setTimeout(resolve, 2000));
    setLoading(false);
    setOpen(false);
  };
  
  return (
    <div>
      <Button onClick={() => setOpen(true)}>Open Form Dialog</Button>
      
      <FormDialog
        open={open}
        onOpenChange={setOpen}
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
            <Input placeholder="John Doe" />
          </FormField>
          <FormField>
            <Label>Email</Label>
            <Input type="email" placeholder="john@example.com" />
          </FormField>
        </FormSection>
        
        <FormSection title="Company Details">
          <FormField>
            <Label>Company</Label>
            <Select>
              <SelectTrigger>
                <SelectValue placeholder="Select company" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="acme">Acme Corp</SelectItem>
                <SelectItem value="globex">Globex Inc</SelectItem>
              </SelectContent>
            </Select>
          </FormField>
        </FormSection>
      </FormDialog>

      <Alert className="mt-4 border-green-200 bg-green-50">
        <CheckCircle className="h-4 w-4 text-green-600" />
        <AlertTitle>FormDialog Benefits</AlertTitle>
        <AlertDescription>
          ~45% less code per form • Automatic loading states • Consistent styling
        </AlertDescription>
      </Alert>
    </div>
  );
}

function BusinessTableStory() {
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
    <div className="space-y-6">
      <div>
        <h3 className="text-sm font-semibold mb-4">With Totals Row</h3>
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
      </div>

      <Alert className="border-blue-200 bg-blue-50">
        <Info className="h-4 w-4 text-blue-600" />
        <AlertTitle>Built-in Formatters</AlertTitle>
        <AlertDescription>
          <code>currency</code>, <code>percentage</code>, <code>number</code>, and custom format functions
        </AlertDescription>
      </Alert>
    </div>
  );
}

function KPICardStory() {
  return (
    <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
      <KPICard
        title="2025 ARR"
        value="$1,250,000"
        subtitle="Annual recurring revenue"
        trend="up"
        color="green"
      />
      <KPICard
        title="MRR Growth"
        value="12.5%"
        subtitle="Month-over-month"
        trend="up"
        color="blue"
      />
      <KPICard
        title="Churn Rate"
        value="5.2%"
        subtitle="Monthly churn"
        trend="down"
        color="gold"
      />
      <KPICard
        title="CAC Payback"
        value="14 months"
        subtitle="Customer acquisition cost"
        trend="down"
        color="red"
      />
    </div>
  );
}

function TimelineStory() {
  const timelineData = [
    { 
      type: 'email',
      title: 'Product Demo Follow-up',
      description: 'Sent follow-up email with pricing details and implementation timeline.',
      time: 'Today at 10:30 AM',
      datetime: '2025-08-01T10:30:00',
      tags: ['Follow-up', 'Pricing'],
      actions: ['View', 'Reply']
    },
    {
      type: 'meeting',
      title: 'Executive Presentation',
      description: 'Presented roadmap and integration capabilities to leadership team.',
      time: 'Yesterday at 2:00 PM',
      datetime: '2025-07-31T14:00:00',
      actions: ['Notes', 'Recording']
    },
    {
      type: 'call',
      title: 'Technical Deep Dive',
      description: 'Discussed API architecture and security requirements with engineering.',
      time: '3 days ago',
      datetime: '2025-07-28T11:00:00',
      actions: ['Summary']
    },
    {
      type: 'document',
      title: 'Contract Negotiation',
      description: 'Legal team reviewing terms. Expecting feedback by end of week.',
      time: '4 days ago',
      datetime: '2025-07-27T16:30:00',
      tags: ['Legal', 'In Review'],
      actions: ['View Contract']
    },
    {
      type: 'activity',
      title: 'Deal Stage Updated',
      description: 'Moved from Qualification to Proposal stage based on budget confirmation.',
      time: '6 days ago',
      datetime: '2025-07-25T09:00:00'
    }
  ];

  return (
    <div className="space-y-8">
      <div>
        <h3 className="text-sm font-semibold mb-4">Conflixis-Style Timeline</h3>
        <div className="relative max-w-2xl">
          {timelineData.map((item, index) => (
            <div key={index} className="flex group">
              {/* Timeline Opposite Content (time) - left side for even indexes */}
              <div className="flex-1 min-h-[72px] flex items-start justify-end pr-6 pt-1">
                {index % 2 === 0 ? (
                  <span className="text-sm font-medium text-muted-foreground">{item.time}</span>
                ) : (
                  <div className="text-right">
                    <p className="font-semibold text-foreground">{item.title}</p>
                    <p className="text-sm text-muted-foreground mt-1">{item.description}</p>
                    {item.tags && (
                      <div className="flex gap-2 mt-2 justify-end">
                        {item.tags.map((tag, i) => (
                          <Badge key={i} variant="outline" className="text-xs">{tag}</Badge>
                        ))}
                      </div>
                    )}
                    {item.actions && (
                      <div className="flex gap-2 mt-2 justify-end">
                        {item.actions.map((action, i) => (
                          <Button key={i} size="sm" variant="ghost" className="h-7 text-xs">{action}</Button>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>
              
              {/* Timeline Separator - always in center */}
              <div className="flex flex-col items-center">
                {/* Timeline Dot with hover effect */}
                <div className="relative">
                  <div className={cn(
                    "w-10 h-10 rounded-full flex-shrink-0 transition-all duration-200 flex items-center justify-center text-white",
                    "group-hover:scale-110 group-hover:shadow-lg",
                    item.type === 'email' && "bg-[#4c94ed] shadow-[#4c94ed]/50",
                    item.type === 'meeting' && "bg-[#93baab] shadow-[#93baab]/50",
                    item.type === 'call' && "bg-[#0c343a] shadow-[#0c343a]/50",
                    item.type === 'document' && "bg-[#eab96d] shadow-[#eab96d]/50",
                    item.type === 'activity' && "bg-[#fd7649] shadow-[#fd7649]/50"
                  )}>
                    {item.type === 'email' && <Mail className="h-5 w-5" />}
                    {item.type === 'meeting' && <Calendar className="h-5 w-5" />}
                    {item.type === 'call' && <Phone className="h-5 w-5" />}
                    {item.type === 'document' && <FileText className="h-5 w-5" />}
                    {item.type === 'activity' && <Activity className="h-5 w-5" />}
                  </div>
                  <div className={cn(
                    "absolute inset-0 rounded-full animate-ping opacity-0 group-hover:opacity-50",
                    item.type === 'email' && "bg-[#4c94ed]",
                    item.type === 'meeting' && "bg-[#93baab]",
                    item.type === 'call' && "bg-[#0c343a]",
                    item.type === 'document' && "bg-[#eab96d]",
                    item.type === 'activity' && "bg-[#fd7649]"
                  )} />
                </div>
                {/* Timeline Connector with gradient */}
                {index < timelineData.length - 1 && (
                  <div className="w-0.5 flex-1 bg-gradient-to-b from-gray-300 to-gray-200 my-3 mx-auto min-h-[60px]" />
                )}
              </div>
              
              {/* Timeline Content - right side for odd indexes */}
              <div className="flex-1 min-h-[72px] flex items-start pl-6 pt-1">
                {index % 2 === 1 ? (
                  <span className="text-sm font-medium text-muted-foreground">{item.time}</span>
                ) : (
                  <div>
                    <p className="font-semibold text-foreground">{item.title}</p>
                    <p className="text-sm text-muted-foreground mt-1">{item.description}</p>
                    {item.tags && (
                      <div className="flex gap-2 mt-2">
                        {item.tags.map((tag, i) => (
                          <Badge key={i} variant="outline" className="text-xs">{tag}</Badge>
                        ))}
                      </div>
                    )}
                    {item.actions && (
                      <div className="flex gap-2 mt-2">
                        {item.actions.map((action, i) => (
                          <Button key={i} size="sm" variant="ghost" className="h-7 text-xs">{action}</Button>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      <Alert className="border-blue-200 bg-blue-50">
        <Info className="h-4 w-4 text-blue-600" />
        <AlertTitle>Timeline Features</AlertTitle>
        <AlertDescription>
          Alternating layout • Conflixis brand colors • Hover animations • Activity type icons • Flexible content areas
        </AlertDescription>
      </Alert>
    </div>
  );
}

function AnimatedCollapsibleStory() {
  const [section1Open, setSection1Open] = useState(true);
  const [section2Open, setSection2Open] = useState(false);
  const [section3Open, setSection3Open] = useState(false);

  return (
    <div className="space-y-8">
      <div>
        <h3 className="text-sm font-semibold mb-4">Smooth Animations with Framer Motion</h3>
        <div className="space-y-4 max-w-2xl">
          <Card>
            <CardHeader>
              <AnimatedCollapsible 
                open={section1Open} 
                onOpenChange={setSection1Open}
                trigger={
                  <Button variant="ghost" className="w-full justify-between p-0 hover:bg-transparent">
                    <div className="text-left">
                      <CardTitle className="flex items-center gap-2">
                        <Building2 className="h-5 w-5" />
                        Company Information
                      </CardTitle>
                      <CardDescription>View detailed company information</CardDescription>
                    </div>
                    <ChevronDown 
                      className={cn(
                        "h-4 w-4 transition-transform duration-400",
                        section1Open && "rotate-180"
                      )}
                    />
                  </Button>
                }
              >
                <CardContent className="pt-6">
                  <div className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label className="text-muted-foreground">Industry</Label>
                        <p className="font-medium">Healthcare Technology</p>
                      </div>
                      <div>
                        <Label className="text-muted-foreground">Founded</Label>
                        <p className="font-medium">2019</p>
                      </div>
                      <div>
                        <Label className="text-muted-foreground">Employees</Label>
                        <p className="font-medium">150-500</p>
                      </div>
                      <div>
                        <Label className="text-muted-foreground">Revenue</Label>
                        <p className="font-medium">$25M - $50M</p>
                      </div>
                    </div>
                    <div>
                      <Label className="text-muted-foreground">Description</Label>
                      <p className="text-sm mt-1">Leading provider of AI-powered healthcare solutions, specializing in patient care optimization and clinical decision support systems.</p>
                    </div>
                  </div>
                </CardContent>
              </AnimatedCollapsible>
            </CardHeader>
          </Card>

          <Card>
            <CardHeader>
              <AnimatedCollapsible 
                open={section2Open} 
                onOpenChange={setSection2Open}
                trigger={
                  <Button variant="ghost" className="w-full justify-between p-0 hover:bg-transparent">
                    <div className="text-left">
                      <CardTitle className="flex items-center gap-2">
                        <Users className="h-5 w-5" />
                        Team Members
                      </CardTitle>
                      <CardDescription>View team contacts and roles</CardDescription>
                    </div>
                    <ChevronDown 
                      className={cn(
                        "h-4 w-4 transition-transform duration-400",
                        section2Open && "rotate-180"
                      )}
                    />
                  </Button>
                }
              >
                <CardContent className="pt-6">
                  <div className="space-y-3">
                    <div className="flex items-center justify-between p-3 rounded-lg border">
                      <div>
                        <p className="font-medium">Sarah Johnson</p>
                        <p className="text-sm text-muted-foreground">Chief Executive Officer</p>
                      </div>
                      <Badge>Primary</Badge>
                    </div>
                    <div className="flex items-center justify-between p-3 rounded-lg border">
                      <div>
                        <p className="font-medium">Michael Chen</p>
                        <p className="text-sm text-muted-foreground">VP of Sales</p>
                      </div>
                      <Badge variant="outline">Decision Maker</Badge>
                    </div>
                    <div className="flex items-center justify-between p-3 rounded-lg border">
                      <div>
                        <p className="font-medium">Emily Rodriguez</p>
                        <p className="text-sm text-muted-foreground">Technical Lead</p>
                      </div>
                      <Badge variant="outline">Technical</Badge>
                    </div>
                  </div>
                </CardContent>
              </AnimatedCollapsible>
            </CardHeader>
          </Card>

          <Card>
            <CardHeader>
              <AnimatedCollapsible 
                open={section3Open} 
                onOpenChange={setSection3Open}
                trigger={
                  <Button variant="ghost" className="w-full justify-between p-0 hover:bg-transparent">
                    <div className="text-left">
                      <CardTitle className="flex items-center gap-2">
                        <DollarSign className="h-5 w-5" />
                        Financial Metrics
                      </CardTitle>
                      <CardDescription>Key performance indicators</CardDescription>
                    </div>
                    <ChevronDown 
                      className={cn(
                        "h-4 w-4 transition-transform duration-400",
                        section3Open && "rotate-180"
                      )}
                    />
                  </Button>
                }
              >
                <CardContent className="pt-6">
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-1">
                      <Label className="text-muted-foreground">MRR</Label>
                      <p className="text-2xl font-bold">$125,000</p>
                      <p className="text-xs text-green-600">+12% from last month</p>
                    </div>
                    <div className="space-y-1">
                      <Label className="text-muted-foreground">ARR</Label>
                      <p className="text-2xl font-bold">$1.5M</p>
                      <p className="text-xs text-green-600">+25% YoY</p>
                    </div>
                    <div className="space-y-1">
                      <Label className="text-muted-foreground">Churn Rate</Label>
                      <p className="text-2xl font-bold">5.2%</p>
                      <p className="text-xs text-muted-foreground">Industry avg: 7.5%</p>
                    </div>
                    <div className="space-y-1">
                      <Label className="text-muted-foreground">LTV:CAC</Label>
                      <p className="text-2xl font-bold">3.2:1</p>
                      <p className="text-xs text-green-600">Healthy ratio</p>
                    </div>
                  </div>
                </CardContent>
              </AnimatedCollapsible>
            </CardHeader>
          </Card>
        </div>
      </div>

      <Alert className="border-green-200 bg-green-50">
        <CheckCircle className="h-4 w-4 text-green-600" />
        <AlertTitle>AnimatedCollapsible Features</AlertTitle>
        <AlertDescription>
          • Smooth height animations with Framer Motion (400ms duration)<br/>
          • Custom easing curve for natural motion<br/>
          • Opacity transitions for elegant reveal<br/>
          • Fully accessible with keyboard support<br/>
          • Works with any trigger component
        </AlertDescription>
      </Alert>
    </div>
  );
}