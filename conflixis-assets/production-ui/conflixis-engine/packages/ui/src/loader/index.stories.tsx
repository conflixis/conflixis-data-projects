import type { Meta, StoryObj } from '@storybook/react';
import { Loader } from './index';

const meta: Meta<typeof Loader> = {
  title: 'Loader',
  component: Loader,
  parameters: {
    layout: 'centered',
  },
  argTypes: {
    className: {
      control: 'text',
      description: 'Additional CSS classes to apply to the loader',
    },
  },
};

export default meta;
type Story = StoryObj<typeof Loader>;

// Default loader
export const Default: Story = {
  args: {
    className: 'h-8 w-8',
  },
};

// Small loader
export const Small: Story = {
  args: {
    className: 'h-4 w-4',
  },
};

// Medium loader
export const Medium: Story = {
  args: {
    className: 'h-6 w-6',
  },
};

// Large loader
export const Large: Story = {
  args: {
    className: 'h-12 w-12',
  },
};

// Extra large loader
export const ExtraLarge: Story = {
  args: {
    className: 'h-16 w-16',
  },
};

// Custom color loader
export const CustomColor: Story = {
  args: {
    className: 'h-8 w-8 fill-blue-500',
  },
};

// Multiple loaders showcase
export const Sizes: Story = {
  render: () => (
    <div className="flex gap-4 items-center">
      <div className="text-center">
        <Loader className="h-4 w-4" />
        <p className="text-sm mt-2">Small</p>
      </div>
      <div className="text-center">
        <Loader className="h-6 w-6" />
        <p className="text-sm mt-2">Medium</p>
      </div>
      <div className="text-center">
        <Loader className="h-8 w-8" />
        <p className="text-sm mt-2">Default</p>
      </div>
      <div className="text-center">
        <Loader className="h-12 w-12" />
        <p className="text-sm mt-2">Large</p>
      </div>
      <div className="text-center">
        <Loader className="h-16 w-16" />
        <p className="text-sm mt-2">Extra Large</p>
      </div>
    </div>
  ),
};

// Different colors showcase
export const Colors: Story = {
  render: () => (
    <div className="flex gap-4 items-center">
      <div className="text-center">
        <Loader className="h-8 w-8 fill-blue-500" />
        <p className="text-sm mt-2">Blue</p>
      </div>
      <div className="text-center">
        <Loader className="h-8 w-8 fill-green-500" />
        <p className="text-sm mt-2">Green</p>
      </div>
      <div className="text-center">
        <Loader className="h-8 w-8 fill-red-500" />
        <p className="text-sm mt-2">Red</p>
      </div>
      <div className="text-center">
        <Loader className="h-8 w-8 fill-purple-500" />
        <p className="text-sm mt-2">Purple</p>
      </div>
      <div className="text-center">
        <Loader className="h-8 w-8 fill-gray-800" />
        <p className="text-sm mt-2">Dark Gray</p>
      </div>
    </div>
  ),
};

// Loader in context examples
export const InContext: Story = {
  render: () => (
    <div className="space-y-4">
      <div className="flex items-center gap-2 p-4 border rounded">
        <Loader className="h-5 w-5" />
        <span>Loading data...</span>
      </div>
      
      <div className="bg-gray-100 p-8 rounded text-center">
        <Loader className="h-12 w-12 mx-auto mb-4" />
        <p className="text-gray-600">Please wait while we process your request</p>
      </div>
      
      <button className="bg-blue-500 text-white px-4 py-2 rounded flex items-center gap-2" disabled>
        <Loader className="h-4 w-4 fill-white" />
        <span>Submitting...</span>
      </button>
    </div>
  ),
};
