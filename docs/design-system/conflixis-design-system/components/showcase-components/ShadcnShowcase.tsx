'use client';

import React, { useState } from 'react';
import { Avatar, AvatarFallback, AvatarImage } from '@workspace/ui/components/avatar';
import { Button } from '@workspace/ui/components/button';
import { Calendar } from '@workspace/ui/components/calendar';
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@workspace/ui/components/collapsible';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle, DialogTrigger } from '@workspace/ui/components/dialog';
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuSeparator, DropdownMenuTrigger } from '@workspace/ui/components/dropdown-menu';
import { Popover, PopoverContent, PopoverTrigger } from '@workspace/ui/components/popover';
import { Skeleton } from '@workspace/ui/components/skeleton';
import { NavigationMenu, NavigationMenuContent, NavigationMenuItem, NavigationMenuLink, NavigationMenuList, NavigationMenuTrigger } from '@workspace/ui/components/navigation-menu';
import { useToast } from '@/hooks/use-toast';
import { Badge } from '@workspace/ui/components/badge';
import { Input } from '@workspace/ui/components/input';
import { Label } from '@workspace/ui/components/label';
import { Textarea } from '@workspace/ui/components/textarea';
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from '@workspace/ui/components/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@workspace/ui/components/tabs';
import { Alert, AlertDescription, AlertTitle } from '@workspace/ui/components/alert';
import { Progress } from '@workspace/ui/components/progress';
import { Checkbox } from '@workspace/ui/components/checkbox';
import { RadioGroup, RadioGroupItem } from '@workspace/ui/components/radio-group';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@workspace/ui/components/select';
import { Switch } from '@workspace/ui/components/switch';
import { Slider } from '@workspace/ui/components/slider';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@workspace/ui/components/table';
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from '@workspace/ui/components/accordion';
import { Separator } from '@workspace/ui/components/separator';
import { 
  ChevronDown, 
  ChevronsUpDown, 
  Plus, 
  Calendar as CalendarIcon,
  Settings,
  User,
  LogOut,
  CreditCard,
  Bell,
  Keyboard,
  Moon,
  Sun,
  Laptop,
  AlertCircle,
  CheckCircle,
  Info,
  XCircle,
  Copy,
  Check,
  Loader2,
  Mail,
  MessageSquare,
  Users,
  FileText,
  Download,
  Upload,
  Search,
  Filter,
  MoreHorizontal,
  MoreVertical,
  Trash,
  Edit,
  Save,
  X,
  Home,
  Menu,
  Package,
  Activity,
  DollarSign
} from 'lucide-react';

export function ShadcnShowcase() {
  const [date, setDate] = useState<Date | undefined>(new Date());
  const [isOpen, setIsOpen] = useState(false);
  const [progress, setProgress] = useState(13);
  const { toast } = useToast();

  React.useEffect(() => {
    const timer = setTimeout(() => setProgress(66), 500);
    return () => clearTimeout(timer);
  }, []);

  return (
    <div className="space-y-10">
      {/* Avatar */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Avatar</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="flex items-center gap-4">
          <Avatar>
            <AvatarImage src="https://github.com/shadcn.png" alt="@shadcn" />
            <AvatarFallback>CN</AvatarFallback>
          </Avatar>
          <Avatar>
            <AvatarFallback className="bg-conflixis-green text-white">CF</AvatarFallback>
          </Avatar>
          <Avatar>
            <AvatarFallback className="bg-conflixis-light-green text-conflixis-green">JD</AvatarFallback>
          </Avatar>
          <Avatar>
            <AvatarFallback className="bg-conflixis-gold text-white">AB</AvatarFallback>
          </Avatar>
        </div>
      </div>

      {/* Calendar */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Calendar</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="max-w-sm">
          <Calendar
            mode="single"
            selected={date}
            onSelect={setDate}
            className="rounded-md border"
          />
        </div>
      </div>

      {/* Collapsible */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Collapsible</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <Collapsible
          open={isOpen}
          onOpenChange={setIsOpen}
          className="w-[350px] space-y-2"
        >
          <div className="flex items-center justify-between space-x-4 px-4">
            <h4 className="text-sm font-semibold">
              @conflixis/platform starred 3 repositories
            </h4>
            <CollapsibleTrigger asChild>
              <Button variant="ghost" size="sm" className="w-9 p-0">
                <ChevronsUpDown className="h-4 w-4" />
                <span className="sr-only">Toggle</span>
              </Button>
            </CollapsibleTrigger>
          </div>
          <div className="rounded-md border px-4 py-3 font-mono text-sm">
            @conflixis/ui
          </div>
          <CollapsibleContent className="space-y-2">
            <div className="rounded-md border px-4 py-3 font-mono text-sm">
              @conflixis/services
            </div>
            <div className="rounded-md border px-4 py-3 font-mono text-sm">
              @conflixis/types
            </div>
          </CollapsibleContent>
        </Collapsible>
      </div>

      {/* Dialog */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Dialog</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <Dialog>
          <DialogTrigger asChild>
            <Button className="bg-conflixis-green hover:bg-conflixis-green/90">
              Open Dialog
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Edit Profile</DialogTitle>
              <DialogDescription>
                Make changes to your profile here. Click save when you&apos;re done.
              </DialogDescription>
            </DialogHeader>
            <div className="grid gap-4 py-4">
              <div className="grid grid-cols-4 items-center gap-4">
                <Label htmlFor="name" className="text-right">
                  Name
                </Label>
                <Input
                  id="name"
                  defaultValue="Conflixis User"
                  className="col-span-3"
                />
              </div>
              <div className="grid grid-cols-4 items-center gap-4">
                <Label htmlFor="username" className="text-right">
                  Username
                </Label>
                <Input
                  id="username"
                  defaultValue="@conflixis"
                  className="col-span-3"
                />
              </div>
            </div>
            <DialogFooter>
              <Button type="submit" className="bg-conflixis-green hover:bg-conflixis-green/90">
                Save changes
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      {/* Dropdown Menu */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Dropdown Menu</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="outline">
              Open Menu <ChevronDown className="ml-2 h-4 w-4" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent className="w-56">
            <DropdownMenuLabel>My Account</DropdownMenuLabel>
            <DropdownMenuSeparator />
            <DropdownMenuItem>
              <User className="mr-2 h-4 w-4" />
              <span>Profile</span>
            </DropdownMenuItem>
            <DropdownMenuItem>
              <CreditCard className="mr-2 h-4 w-4" />
              <span>Billing</span>
            </DropdownMenuItem>
            <DropdownMenuItem>
              <Settings className="mr-2 h-4 w-4" />
              <span>Settings</span>
            </DropdownMenuItem>
            <DropdownMenuItem>
              <Keyboard className="mr-2 h-4 w-4" />
              <span>Keyboard shortcuts</span>
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem className="text-conflixis-red">
              <LogOut className="mr-2 h-4 w-4" />
              <span>Log out</span>
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>

      {/* Navigation Menu */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Navigation Menu</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <NavigationMenu>
          <NavigationMenuList>
            <NavigationMenuItem>
              <NavigationMenuTrigger className="bg-transparent">Getting started</NavigationMenuTrigger>
              <NavigationMenuContent>
                <ul className="grid gap-3 p-6 md:w-[400px] lg:w-[500px] lg:grid-cols-[.75fr_1fr]">
                  <li className="row-span-3">
                    <NavigationMenuLink asChild>
                      <a
                        className="flex h-full w-full select-none flex-col justify-end rounded-md bg-gradient-to-b from-conflixis-light-green/20 to-conflixis-green/20 p-6 no-underline outline-none focus:shadow-md"
                        href="/"
                      >
                        <div className="mb-2 mt-4 text-lg font-medium">
                          Conflixis Platform
                        </div>
                        <p className="text-sm leading-tight text-muted-foreground">
                          Comprehensive financial analytics and business management platform
                        </p>
                      </a>
                    </NavigationMenuLink>
                  </li>
                  <li>
                    <NavigationMenuLink asChild>
                      <a href="#" className="block select-none space-y-1 rounded-md p-3 leading-none no-underline outline-none transition-colors hover:bg-accent hover:text-accent-foreground focus:bg-accent focus:text-accent-foreground">
                        <div className="text-sm font-medium leading-none">Introduction</div>
                        <p className="line-clamp-2 text-sm leading-snug text-muted-foreground">
                          Get started with the Conflixis platform
                        </p>
                      </a>
                    </NavigationMenuLink>
                  </li>
                  <li>
                    <NavigationMenuLink asChild>
                      <a href="#" className="block select-none space-y-1 rounded-md p-3 leading-none no-underline outline-none transition-colors hover:bg-accent hover:text-accent-foreground focus:bg-accent focus:text-accent-foreground">
                        <div className="text-sm font-medium leading-none">Installation</div>
                        <p className="line-clamp-2 text-sm leading-snug text-muted-foreground">
                          How to install dependencies and structure your app
                        </p>
                      </a>
                    </NavigationMenuLink>
                  </li>
                </ul>
              </NavigationMenuContent>
            </NavigationMenuItem>
            <NavigationMenuItem>
              <NavigationMenuTrigger className="bg-transparent">Components</NavigationMenuTrigger>
              <NavigationMenuContent>
                <ul className="grid w-[400px] gap-3 p-4 md:w-[500px] md:grid-cols-2 lg:w-[600px]">
                  <li>
                    <NavigationMenuLink asChild>
                      <a href="#" className="block select-none space-y-1 rounded-md p-3 leading-none no-underline outline-none transition-colors hover:bg-accent hover:text-accent-foreground focus:bg-accent focus:text-accent-foreground">
                        <div className="text-sm font-medium leading-none">Alert Dialog</div>
                        <p className="line-clamp-2 text-sm leading-snug text-muted-foreground">
                          A modal dialog that interrupts interaction
                        </p>
                      </a>
                    </NavigationMenuLink>
                  </li>
                  <li>
                    <NavigationMenuLink asChild>
                      <a href="#" className="block select-none space-y-1 rounded-md p-3 leading-none no-underline outline-none transition-colors hover:bg-accent hover:text-accent-foreground focus:bg-accent focus:text-accent-foreground">
                        <div className="text-sm font-medium leading-none">Hover Card</div>
                        <p className="line-clamp-2 text-sm leading-snug text-muted-foreground">
                          For sighted users to preview content
                        </p>
                      </a>
                    </NavigationMenuLink>
                  </li>
                </ul>
              </NavigationMenuContent>
            </NavigationMenuItem>
          </NavigationMenuList>
        </NavigationMenu>
      </div>

      {/* Popover */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Popover</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <Popover>
          <PopoverTrigger asChild>
            <Button
              variant="outline"
              className="w-[280px] justify-start text-left font-normal"
            >
              <CalendarIcon className="mr-2 h-4 w-4" />
              {date ? date.toDateString() : <span>Pick a date</span>}
            </Button>
          </PopoverTrigger>
          <PopoverContent className="w-auto p-0">
            <Calendar
              mode="single"
              selected={date}
              onSelect={setDate}
              initialFocus
            />
          </PopoverContent>
        </Popover>
      </div>

      {/* Skeleton */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Skeleton</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-3">
          <div className="flex items-center space-x-4">
            <Skeleton className="h-12 w-12 rounded-full bg-conflixis-gray/20" />
            <div className="space-y-2">
              <Skeleton className="h-4 w-[250px] bg-conflixis-gray/20" />
              <Skeleton className="h-4 w-[200px] bg-conflixis-gray/20" />
            </div>
          </div>
          <Skeleton className="h-[125px] w-full rounded-xl bg-conflixis-gray/20" />
        </div>
      </div>

      {/* Toast Demo */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Toast</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button
            onClick={() => {
              toast({
                title: "Success!",
                description: "Your changes have been saved.",
              })
            }}
            className="bg-conflixis-success-green hover:bg-conflixis-success-green/90"
          >
            Show Success Toast
          </Button>
          <Button
            variant="destructive"
            onClick={() => {
              toast({
                variant: "destructive",
                title: "Error!",
                description: "Something went wrong.",
              })
            }}
          >
            Show Error Toast
          </Button>
          <Button
            variant="outline"
            onClick={() => {
              toast({
                title: "Scheduled: Catch up",
                description: "Friday, February 10, 2023 at 5:57 PM",
              })
            }}
          >
            Show Default Toast
          </Button>
        </div>
      </div>

      {/* Theme Toggle Demo */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Theme Toggle</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" size="icon">
            <Sun className="h-[1.2rem] w-[1.2rem] rotate-0 scale-100 transition-all dark:-rotate-90 dark:scale-0" />
            <Moon className="absolute h-[1.2rem] w-[1.2rem] rotate-90 scale-0 transition-all dark:rotate-0 dark:scale-100" />
            <span className="sr-only">Toggle theme</span>
          </Button>
          <Button variant="outline" size="icon">
            <Laptop className="h-[1.2rem] w-[1.2rem]" />
            <span className="sr-only">System theme</span>
          </Button>
        </div>
      </div>

      {/* Form Example */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Form Example</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="max-w-md space-y-4">
          <div className="space-y-2">
            <Label htmlFor="email">Email</Label>
            <Input 
              id="email" 
              type="email" 
              placeholder="email@conflixis.com"
              className="focus:ring-conflixis-green" 
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="message">Message</Label>
            <Textarea 
              id="message" 
              placeholder="Type your message here..."
              className="focus:ring-conflixis-green min-h-[100px]" 
            />
          </div>
          <Button className="bg-conflixis-green hover:bg-conflixis-green/90">
            Send Message
          </Button>
        </div>
      </div>

      {/* Table */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Table</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[100px]">Invoice</TableHead>
              <TableHead>Status</TableHead>
              <TableHead>Method</TableHead>
              <TableHead className="text-right">Amount</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow>
              <TableCell className="font-medium">INV001</TableCell>
              <TableCell>
                <Badge className="bg-conflixis-success-green">Paid</Badge>
              </TableCell>
              <TableCell>Credit Card</TableCell>
              <TableCell className="text-right">$250.00</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="font-medium">INV002</TableCell>
              <TableCell>
                <Badge className="bg-conflixis-gold">Pending</Badge>
              </TableCell>
              <TableCell>PayPal</TableCell>
              <TableCell className="text-right">$150.00</TableCell>
            </TableRow>
            <TableRow>
              <TableCell className="font-medium">INV003</TableCell>
              <TableCell>
                <Badge className="bg-conflixis-red">Overdue</Badge>
              </TableCell>
              <TableCell>Bank Transfer</TableCell>
              <TableCell className="text-right">$350.00</TableCell>
            </TableRow>
          </TableBody>
        </Table>
      </div>

      {/* Tabs */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Tabs</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <Tabs defaultValue="account" className="w-[400px]">
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="account">Account</TabsTrigger>
            <TabsTrigger value="password">Password</TabsTrigger>
          </TabsList>
          <TabsContent value="account">
            <Card>
              <CardHeader>
                <CardTitle>Account</CardTitle>
                <CardDescription>
                  Make changes to your account here. Click save when you're done.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-2">
                <div className="space-y-1">
                  <Label htmlFor="name">Name</Label>
                  <Input id="name" defaultValue="Pedro Duarte" />
                </div>
                <div className="space-y-1">
                  <Label htmlFor="username">Username</Label>
                  <Input id="username" defaultValue="@peduarte" />
                </div>
              </CardContent>
              <CardFooter>
                <Button className="bg-conflixis-green hover:bg-conflixis-green/90">Save changes</Button>
              </CardFooter>
            </Card>
          </TabsContent>
          <TabsContent value="password">
            <Card>
              <CardHeader>
                <CardTitle>Password</CardTitle>
                <CardDescription>
                  Change your password here. After saving, you'll be logged out.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-2">
                <div className="space-y-1">
                  <Label htmlFor="current">Current password</Label>
                  <Input id="current" type="password" />
                </div>
                <div className="space-y-1">
                  <Label htmlFor="new">New password</Label>
                  <Input id="new" type="password" />
                </div>
              </CardContent>
              <CardFooter>
                <Button className="bg-conflixis-green hover:bg-conflixis-green/90">Save password</Button>
              </CardFooter>
            </Card>
          </TabsContent>
        </Tabs>
      </div>

      {/* Alert */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Alert</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-4">
          <Alert>
            <Info className="h-4 w-4" />
            <AlertTitle>Heads up!</AlertTitle>
            <AlertDescription>
              You can add components to your app using the cli.
            </AlertDescription>
          </Alert>
          <Alert className="border-conflixis-green bg-conflixis-green/5">
            <CheckCircle className="h-4 w-4 text-conflixis-green" />
            <AlertTitle>Success!</AlertTitle>
            <AlertDescription>
              Your application has been deployed successfully.
            </AlertDescription>
          </Alert>
          <Alert className="border-conflixis-gold bg-conflixis-gold/5">
            <AlertCircle className="h-4 w-4 text-conflixis-gold" />
            <AlertTitle>Warning!</AlertTitle>
            <AlertDescription>
              Your subscription is about to expire. Please renew.
            </AlertDescription>
          </Alert>
          <Alert variant="destructive">
            <XCircle className="h-4 w-4" />
            <AlertTitle>Error</AlertTitle>
            <AlertDescription>
              Your session has expired. Please log in again.
            </AlertDescription>
          </Alert>
        </div>
      </div>

      {/* Progress */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Progress</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-4 max-w-md">
          <div>
            <div className="flex justify-between mb-2">
              <span className="text-sm">Project Completion</span>
              <span className="text-sm text-muted-foreground">75%</span>
            </div>
            <Progress value={75} className="[&>div]:bg-conflixis-green" />
          </div>
          <div>
            <div className="flex justify-between mb-2">
              <span className="text-sm">Revenue Target</span>
              <span className="text-sm text-muted-foreground">45%</span>
            </div>
            <Progress value={45} className="[&>div]:bg-conflixis-gold" />
          </div>
          <div>
            <div className="flex justify-between mb-2">
              <span className="text-sm">Customer Satisfaction</span>
              <span className="text-sm text-muted-foreground">92%</span>
            </div>
            <Progress value={92} className="[&>div]:bg-conflixis-success-green" />
          </div>
        </div>
      </div>

      {/* Card Variants */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Card Variants</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          <Card>
            <CardHeader>
              <CardTitle>
                <div className="flex items-center gap-2">
                  <Package className="h-5 w-5 text-conflixis-green" />
                  Total Products
                </div>
              </CardTitle>
              <CardDescription>Active inventory items</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-conflixis-green">1,234</div>
              <p className="text-xs text-muted-foreground">
                +20.1% from last month
              </p>
            </CardContent>
          </Card>
          <Card className="border-conflixis-gold">
            <CardHeader className="bg-conflixis-gold/10">
              <CardTitle>
                <div className="flex items-center gap-2">
                  <Users className="h-5 w-5 text-conflixis-gold" />
                  Active Users
                </div>
              </CardTitle>
              <CardDescription>Currently online</CardDescription>
            </CardHeader>
            <CardContent className="pt-6">
              <div className="text-2xl font-bold text-conflixis-gold">573</div>
              <p className="text-xs text-muted-foreground">
                +201 since last hour
              </p>
            </CardContent>
          </Card>
          <Card className="bg-conflixis-green text-white">
            <CardHeader>
              <CardTitle className="text-white">
                <div className="flex items-center gap-2">
                  <Activity className="h-5 w-5" />
                  System Status
                </div>
              </CardTitle>
              <CardDescription className="text-conflixis-light-green">
                All systems operational
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">99.9%</div>
              <p className="text-xs text-conflixis-light-green">
                Uptime last 30 days
              </p>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Badge Variants */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Badge Variants</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="flex flex-wrap gap-2">
          <Badge>Default</Badge>
          <Badge variant="secondary">Secondary</Badge>
          <Badge variant="outline">Outline</Badge>
          <Badge variant="destructive">Destructive</Badge>
          <Badge className="bg-conflixis-green">Conflixis Green</Badge>
          <Badge className="bg-conflixis-light-green text-conflixis-green">Light Green</Badge>
          <Badge className="bg-conflixis-gold">Gold</Badge>
          <Badge className="bg-conflixis-blue">Blue</Badge>
          <Badge className="bg-conflixis-red">Red</Badge>
          <Badge className="bg-gradient-to-r from-conflixis-green to-conflixis-light-green">Gradient</Badge>
        </div>
      </div>

      {/* Button Variants */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Button Variants & States</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-4">
          <div className="flex flex-wrap gap-2">
            <Button>Default</Button>
            <Button variant="secondary">Secondary</Button>
            <Button variant="destructive">Destructive</Button>
            <Button variant="outline">Outline</Button>
            <Button variant="ghost">Ghost</Button>
            <Button variant="link">Link</Button>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button size="lg">Large</Button>
            <Button size="default">Default</Button>
            <Button size="sm">Small</Button>
            <Button size="icon">
              <Search className="h-4 w-4" />
            </Button>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button disabled>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Loading
            </Button>
            <Button className="bg-conflixis-green hover:bg-conflixis-green/90">
              <Check className="mr-2 h-4 w-4" />
              Conflixis
            </Button>
            <Button variant="outline" className="border-conflixis-green text-conflixis-green hover:bg-conflixis-green hover:text-white">
              <Download className="mr-2 h-4 w-4" />
              Download
            </Button>
          </div>
        </div>
      </div>

      {/* Input Variants */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Input Variants</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-4 max-w-md">
          <div className="space-y-2">
            <Label htmlFor="search">Search</Label>
            <div className="relative">
              <Search className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
              <Input id="search" placeholder="Search..." className="pl-8" />
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="email-icon">Email with icon</Label>
            <div className="relative">
              <Mail className="absolute left-2 top-2.5 h-4 w-4 text-muted-foreground" />
              <Input id="email-icon" type="email" placeholder="Email" className="pl-8" />
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="disabled">Disabled</Label>
            <Input id="disabled" disabled placeholder="Disabled input" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="with-button">With button</Label>
            <div className="flex gap-2">
              <Input id="with-button" placeholder="Enter value" />
              <Button className="bg-conflixis-green hover:bg-conflixis-green/90">Submit</Button>
            </div>
          </div>
        </div>
      </div>

      {/* Select & Combobox */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Select & Dropdowns</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-4 max-w-md">
          <div className="space-y-2">
            <Label>Country</Label>
            <Select>
              <SelectTrigger>
                <SelectValue placeholder="Select a country" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="us">United States</SelectItem>
                <SelectItem value="uk">United Kingdom</SelectItem>
                <SelectItem value="ca">Canada</SelectItem>
                <SelectItem value="au">Australia</SelectItem>
                <SelectItem value="de">Germany</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>Priority</Label>
            <Select>
              <SelectTrigger>
                <SelectValue placeholder="Select priority" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="high">
                  <div className="flex items-center">
                    <Badge className="bg-conflixis-red mr-2">High</Badge>
                    Urgent tasks
                  </div>
                </SelectItem>
                <SelectItem value="medium">
                  <div className="flex items-center">
                    <Badge className="bg-conflixis-gold mr-2">Medium</Badge>
                    Normal priority
                  </div>
                </SelectItem>
                <SelectItem value="low">
                  <div className="flex items-center">
                    <Badge className="bg-conflixis-green mr-2">Low</Badge>
                    Can wait
                  </div>
                </SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
      </div>

      {/* Checkbox & Radio */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Checkbox & Radio</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-6">
          <div className="space-y-4">
            <Label>Select features</Label>
            <div className="space-y-2">
              <div className="flex items-center space-x-2">
                <Checkbox id="feature1" defaultChecked />
                <label
                  htmlFor="feature1"
                  className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                >
                  Enable notifications
                </label>
              </div>
              <div className="flex items-center space-x-2">
                <Checkbox id="feature2" />
                <label
                  htmlFor="feature2"
                  className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                >
                  Enable 2FA authentication
                </label>
              </div>
              <div className="flex items-center space-x-2">
                <Checkbox id="feature3" defaultChecked />
                <label
                  htmlFor="feature3"
                  className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                >
                  Receive marketing emails
                </label>
              </div>
            </div>
          </div>
          <div className="space-y-4">
            <Label>Select plan</Label>
            <RadioGroup defaultValue="pro">
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="free" id="free" />
                <label htmlFor="free" className="flex items-center gap-2">
                  <span className="font-medium">Free</span>
                  <Badge variant="outline">$0/month</Badge>
                </label>
              </div>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="pro" id="pro" />
                <label htmlFor="pro" className="flex items-center gap-2">
                  <span className="font-medium">Pro</span>
                  <Badge className="bg-conflixis-green">$19/month</Badge>
                </label>
              </div>
              <div className="flex items-center space-x-2">
                <RadioGroupItem value="enterprise" id="enterprise" />
                <label htmlFor="enterprise" className="flex items-center gap-2">
                  <span className="font-medium">Enterprise</span>
                  <Badge className="bg-conflixis-gold">Custom</Badge>
                </label>
              </div>
            </RadioGroup>
          </div>
        </div>
      </div>

      {/* Switch */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Switch</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-4">
          <div className="flex items-center justify-between max-w-md p-4 border rounded-lg">
            <div className="space-y-0.5">
              <Label>Airplane Mode</Label>
              <p className="text-sm text-muted-foreground">
                Disable all wireless connections
              </p>
            </div>
            <Switch />
          </div>
          <div className="flex items-center justify-between max-w-md p-4 border rounded-lg">
            <div className="space-y-0.5">
              <Label>Notifications</Label>
              <p className="text-sm text-muted-foreground">
                Receive notifications about updates
              </p>
            </div>
            <Switch defaultChecked />
          </div>
        </div>
      </div>

      {/* Slider */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Slider</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-4 max-w-md">
          <div className="space-y-2">
            <div className="flex justify-between">
              <Label>Volume</Label>
              <span className="text-sm text-muted-foreground">50%</span>
            </div>
            <Slider defaultValue={[50]} max={100} step={1} />
          </div>
          <div className="space-y-2">
            <div className="flex justify-between">
              <Label>Price Range</Label>
              <span className="text-sm text-muted-foreground">$20 - $80</span>
            </div>
            <Slider
              defaultValue={[20, 80]}
              max={100}
              step={10}
              className="[&_[role=slider]]:bg-conflixis-green"
            />
          </div>
        </div>
      </div>

      {/* Loading States */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Loading States</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-4">
          <div className="flex items-center gap-4">
            <Loader2 className="h-4 w-4 animate-spin" />
            <Loader2 className="h-6 w-6 animate-spin text-conflixis-green" />
            <Loader2 className="h-8 w-8 animate-spin text-conflixis-gold" />
          </div>
          <div className="flex gap-2">
            <Button disabled>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Processing
            </Button>
            <Button disabled className="bg-conflixis-green">
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Saving...
            </Button>
          </div>
        </div>
      </div>

      {/* Accordion */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Accordion</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <Accordion type="single" collapsible className="w-full max-w-md">
          <AccordionItem value="item-1">
            <AccordionTrigger>What is Conflixis?</AccordionTrigger>
            <AccordionContent>
              Conflixis is a comprehensive financial analytics and business management platform designed for modern SaaS companies.
            </AccordionContent>
          </AccordionItem>
          <AccordionItem value="item-2">
            <AccordionTrigger>How do I get started?</AccordionTrigger>
            <AccordionContent>
              You can get started by signing up for a free trial. Our onboarding process will guide you through setting up your account and importing your financial data.
            </AccordionContent>
          </AccordionItem>
          <AccordionItem value="item-3">
            <AccordionTrigger>What integrations are available?</AccordionTrigger>
            <AccordionContent>
              We integrate with QuickBooks, Stripe, Salesforce, and many other popular business tools. You can also use our API to build custom integrations.
            </AccordionContent>
          </AccordionItem>
        </Accordion>
      </div>

      {/* Separator */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Separator</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-4 max-w-md">
          <div>
            <div className="space-y-1">
              <h4 className="text-sm font-medium leading-none">Conflixis Platform</h4>
              <p className="text-sm text-muted-foreground">
                An enterprise-grade financial analytics solution.
              </p>
            </div>
            <Separator className="my-4" />
            <div className="flex h-5 items-center space-x-4 text-sm">
              <div>Blog</div>
              <Separator orientation="vertical" />
              <div>Docs</div>
              <Separator orientation="vertical" />
              <div>Source</div>
            </div>
          </div>
          <div className="space-y-4">
            <h4 className="text-sm font-medium">Custom Separators</h4>
            <Separator className="bg-conflixis-green" />
            <Separator className="bg-conflixis-gold" />
            <Separator className="bg-gradient-to-r from-conflixis-green to-conflixis-light-green h-0.5" />
          </div>
        </div>
      </div>

      {/* Icon Buttons */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Icon Buttons</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button size="icon" variant="outline">
            <Home className="h-4 w-4" />
          </Button>
          <Button size="icon" variant="ghost">
            <Search className="h-4 w-4" />
          </Button>
          <Button size="icon" className="bg-conflixis-green hover:bg-conflixis-green/90">
            <Plus className="h-4 w-4" />
          </Button>
          <Button size="icon" variant="secondary">
            <Settings className="h-4 w-4" />
          </Button>
          <Button size="icon" variant="destructive">
            <Trash className="h-4 w-4" />
          </Button>
          <Button size="icon" variant="outline" className="border-conflixis-gold text-conflixis-gold hover:bg-conflixis-gold hover:text-white">
            <Bell className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Complex Form */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Complex Form Example</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <Card className="max-w-lg">
          <CardHeader>
            <CardTitle>Create Account</CardTitle>
            <CardDescription>
              Enter your information to create a new account
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="first-name">First name</Label>
                  <Input id="first-name" placeholder="John" />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="last-name">Last name</Label>
                  <Input id="last-name" placeholder="Doe" />
                </div>
              </div>
              <div className="space-y-2">
                <Label htmlFor="email">Email</Label>
                <Input id="email" type="email" placeholder="john@conflixis.com" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="role">Role</Label>
                <Select>
                  <SelectTrigger id="role">
                    <SelectValue placeholder="Select a role" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="admin">Administrator</SelectItem>
                    <SelectItem value="manager">Manager</SelectItem>
                    <SelectItem value="developer">Developer</SelectItem>
                    <SelectItem value="designer">Designer</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>Notifications</Label>
                <div className="space-y-2">
                  <div className="flex items-center space-x-2">
                    <Switch id="email-notifications" />
                    <Label htmlFor="email-notifications" className="font-normal">
                      Email notifications
                    </Label>
                  </div>
                  <div className="flex items-center space-x-2">
                    <Switch id="sms-notifications" />
                    <Label htmlFor="sms-notifications" className="font-normal">
                      SMS notifications
                    </Label>
                  </div>
                </div>
              </div>
              <div className="flex items-center space-x-2">
                <Checkbox id="terms" />
                <label
                  htmlFor="terms"
                  className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
                >
                  I agree to the terms and conditions
                </label>
              </div>
            </form>
          </CardContent>
          <CardFooter className="flex justify-between">
            <Button variant="outline">Cancel</Button>
            <Button className="bg-conflixis-green hover:bg-conflixis-green/90">
              Create Account
            </Button>
          </CardFooter>
        </Card>
      </div>

      {/* Data Display Cards */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Data Display Cards</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                Total Revenue
              </CardTitle>
              <DollarSign className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">$45,231.89</div>
              <p className="text-xs text-muted-foreground">
                +20.1% from last month
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                Subscriptions
              </CardTitle>
              <Users className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">+2350</div>
              <p className="text-xs text-muted-foreground">
                +180.1% from last month
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Sales</CardTitle>
              <CreditCard className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">+12,234</div>
              <p className="text-xs text-muted-foreground">
                +19% from last month
              </p>
            </CardContent>
          </Card>
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">
                Active Now
              </CardTitle>
              <Activity className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">+573</div>
              <p className="text-xs text-muted-foreground">
                +201 since last hour
              </p>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Empty States */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Empty States</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="grid gap-4 md:grid-cols-2">
          <Card>
            <CardContent className="flex flex-col items-center justify-center h-[200px] text-center">
              <FileText className="h-10 w-10 text-muted-foreground mb-4" />
              <h3 className="font-semibold">No documents yet</h3>
              <p className="text-sm text-muted-foreground mb-4">
                Upload your first document to get started
              </p>
              <Button className="bg-conflixis-green hover:bg-conflixis-green/90">
                <Upload className="mr-2 h-4 w-4" />
                Upload Document
              </Button>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="flex flex-col items-center justify-center h-[200px] text-center">
              <Search className="h-10 w-10 text-muted-foreground mb-4" />
              <h3 className="font-semibold">No results found</h3>
              <p className="text-sm text-muted-foreground">
                Try adjusting your search or filter criteria
              </p>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Lists */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Lists</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <Card className="max-w-md">
          <CardHeader>
            <CardTitle>Recent Activity</CardTitle>
            <CardDescription>Your team's latest updates</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <div className="flex items-center gap-4">
                <Avatar>
                  <AvatarFallback className="bg-conflixis-green text-white">JD</AvatarFallback>
                </Avatar>
                <div className="flex-1 space-y-1">
                  <p className="text-sm font-medium leading-none">
                    Jane Doe updated the revenue dashboard
                  </p>
                  <p className="text-sm text-muted-foreground">
                    2 hours ago
                  </p>
                </div>
                <Button variant="ghost" size="icon">
                  <MoreHorizontal className="h-4 w-4" />
                </Button>
              </div>
              <Separator />
              <div className="flex items-center gap-4">
                <Avatar>
                  <AvatarFallback className="bg-conflixis-gold text-white">AS</AvatarFallback>
                </Avatar>
                <div className="flex-1 space-y-1">
                  <p className="text-sm font-medium leading-none">
                    Alex Smith shared a new report
                  </p>
                  <p className="text-sm text-muted-foreground">
                    5 hours ago
                  </p>
                </div>
                <Button variant="ghost" size="icon">
                  <MoreHorizontal className="h-4 w-4" />
                </Button>
              </div>
              <Separator />
              <div className="flex items-center gap-4">
                <Avatar>
                  <AvatarFallback className="bg-conflixis-light-green text-conflixis-green">MJ</AvatarFallback>
                </Avatar>
                <div className="flex-1 space-y-1">
                  <p className="text-sm font-medium leading-none">
                    Mike Johnson completed the quarterly review
                  </p>
                  <p className="text-sm text-muted-foreground">
                    Yesterday
                  </p>
                </div>
                <Button variant="ghost" size="icon">
                  <MoreHorizontal className="h-4 w-4" />
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Notification Styles */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Notification Styles</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-4 max-w-md">
          <div className="rounded-lg border p-4 bg-conflixis-green/5 border-conflixis-green">
            <div className="flex gap-2">
              <CheckCircle className="h-5 w-5 text-conflixis-green shrink-0 mt-0.5" />
              <div className="space-y-1">
                <p className="text-sm font-medium">Payment successful</p>
                <p className="text-sm text-muted-foreground">
                  Your payment of $250 has been processed successfully.
                </p>
              </div>
            </div>
          </div>
          <div className="rounded-lg border p-4 bg-conflixis-gold/5 border-conflixis-gold">
            <div className="flex gap-2">
              <AlertCircle className="h-5 w-5 text-conflixis-gold shrink-0 mt-0.5" />
              <div className="space-y-1">
                <p className="text-sm font-medium">Subscription expiring soon</p>
                <p className="text-sm text-muted-foreground">
                  Your subscription will expire in 7 days. Please renew to continue.
                </p>
              </div>
            </div>
          </div>
          <div className="rounded-lg border p-4 bg-conflixis-red/5 border-conflixis-red">
            <div className="flex gap-2">
              <XCircle className="h-5 w-5 text-conflixis-red shrink-0 mt-0.5" />
              <div className="space-y-1">
                <p className="text-sm font-medium">Error processing request</p>
                <p className="text-sm text-muted-foreground">
                  We couldn't process your request. Please try again later.
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Animated Progress */}
      <div>
        <div className="flex items-center gap-2 mb-4">
          <h3 className="text-lg font-semibold">Animated Progress</h3>
          <Badge variant="outline" className="text-xs">shadcn/ui</Badge>
        </div>
        <div className="space-y-4 max-w-md">
          <div>
            <div className="flex justify-between mb-2">
              <span className="text-sm">Uploading files...</span>
              <span className="text-sm text-muted-foreground">{progress}%</span>
            </div>
            <Progress value={progress} className="[&>div]:bg-conflixis-green [&>div]:transition-all" />
          </div>
          <Button 
            onClick={() => setProgress(Math.min(progress + 10, 100))}
            className="bg-conflixis-green hover:bg-conflixis-green/90"
          >
            Increase Progress
          </Button>
        </div>
      </div>
    </div>
  );
}