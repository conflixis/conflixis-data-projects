import type { Meta, StoryObj } from '@storybook/react';
import { Card, CardHeader, CardTitle, CardContent, CardFooter } from './index';

const meta: Meta<typeof Card> = {
  title: 'Card',
  component: Card,
  argTypes: {
    elevation: {
      control: 'select',
      options: ['none', 'low', 'medium', 'high'],
    },
    padding: {
      control: 'select',
      options: ['none', 'small', 'medium', 'large'],
    },
  },
};
export default meta;

type Story = StoryObj<typeof Card>;

export const Default: Story = {
  args: {
    elevation: 'low',
    padding: 'medium',
    children: (
      <>
        <CardHeader>
          <CardTitle>Example Card</CardTitle>
        </CardHeader>
        <CardContent>Card content goes here.</CardContent>
        <CardFooter>Footer actions</CardFooter>
      </>
    ),
  },
};
