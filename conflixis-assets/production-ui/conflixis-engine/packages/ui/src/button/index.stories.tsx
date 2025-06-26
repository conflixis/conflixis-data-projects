import type { Meta, StoryObj } from '@storybook/react';
import { Button } from './index';

const meta: Meta<typeof Button> = {
  title: 'Button',
  component: Button,
  parameters: {
    layout: 'centered',
  },
  argTypes: {
    text: {
      control: 'text',
      description: 'The text to display in the button',
    },
    style: {
      control: 'select',
      options: ['primary', 'primary-outlined', 'secondary', 'link', 'white', 'green', 'danger'],
      description: 'The visual style of the button',
    },
    disabled: {
      control: 'boolean',
      description: 'Whether the button is disabled',
    },
    showLoader: {
      control: 'boolean',
      description: 'Whether to show a loading spinner',
    },
    showTooltip: {
      control: 'boolean',
      description: 'Whether to show a tooltip',
    },
    tooltipText: {
      control: 'text',
      description: 'The text to display in the tooltip',
      if: { arg: 'showTooltip', truthy: true },
    },
    tooltipPosition: {
      control: 'select',
      options: ['top', 'bottom', 'left', 'right'],
      description: 'The position of the tooltip',
      if: { arg: 'showTooltip', truthy: true },
    },
    type: {
      control: 'select',
      options: ['button', 'submit', 'reset'],
      description: 'The HTML button type',
    },
    onClick: {
      action: 'clicked',
    },
  },
};

export default meta;
type Story = StoryObj<typeof Button>;

// Primary button (default)
export const Primary: Story = {
  args: {
    text: 'Primary Button',
    style: 'primary',
  },
};

// Primary outlined button
export const PrimaryOutlined: Story = {
  args: {
    text: 'Primary Outlined',
    style: 'primary-outlined',
  },
};

// Secondary button
export const Secondary: Story = {
  args: {
    text: 'Secondary Button',
    style: 'secondary',
  },
};

// White button
export const White: Story = {
  args: {
    text: 'White Button',
    style: 'white',
  },
};

// Green button
export const Green: Story = {
  args: {
    text: 'Green Button',
    style: 'green',
  },
};

// Danger button
export const Danger: Story = {
  args: {
    text: 'Danger Button',
    style: 'danger',
  },
};

// Link style button
export const Link: Story = {
  args: {
    text: 'Link Button',
    style: 'link',
  },
};

// Loading state
export const Loading: Story = {
  args: {
    text: 'Loading...',
    showLoader: true,
    style: 'primary',
  },
};

// Disabled state
export const Disabled: Story = {
  args: {
    text: 'Disabled Button',
    disabled: true,
    style: 'primary',
  },
};

// With tooltip
export const WithTooltip: Story = {
  args: {
    text: 'Hover me',
    showTooltip: true,
    tooltipText: 'This is a helpful tooltip',
    tooltipPosition: 'top',
    style: 'primary',
  },
};

// With icon before text
export const WithIcon: Story = {
  args: {
    text: 'Download',
    icon: (
      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
      </svg>
    ),
    style: 'primary',
  },
};

// With icon after text
export const WithIconAfter: Story = {
  args: {
    text: 'Next',
    iconAfter: (
      <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
      </svg>
    ),
    style: 'primary',
  },
};

// All styles showcase
export const AllStyles: Story = {
  render: () => (
    <div className="flex flex-col gap-4">
      <div className="flex gap-2 items-center">
        <Button text="Primary" style="primary" />
        <Button text="Primary Outlined" style="primary-outlined" />
        <Button text="Secondary" style="secondary" />
        <Button text="White" style="white" />
        <Button text="Green" style="green" />
        <Button text="Danger" style="danger" />
        <Button text="Link" style="link" />
      </div>
      <div className="flex gap-2 items-center">
        <Button text="Primary Disabled" style="primary" disabled />
        <Button text="Outlined Disabled" style="primary-outlined" disabled />
        <Button text="Secondary Disabled" style="secondary" disabled />
        <Button text="White Disabled" style="white" disabled />
        <Button text="Green Disabled" style="green" disabled />
        <Button text="Danger Disabled" style="danger" disabled />
      </div>
    </div>
  ),
};
