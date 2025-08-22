'use client';

import React from 'react';
import { clsx } from 'clsx';

interface StatCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  icon?: React.ReactNode;
  color?: 'green' | 'blue' | 'yellow' | 'red' | 'gray';
  className?: string;
}

export default function StatCard({ 
  title, 
  value, 
  subtitle, 
  icon, 
  color = 'gray',
  className = '' 
}: StatCardProps) {
  const colorClasses = {
    green: 'text-green-600',
    blue: 'text-conflixis-blue',
    yellow: 'text-yellow-600',
    red: 'text-red-600',
    gray: 'text-gray-600',
  };

  return (
    <div className={clsx('bg-white rounded-lg shadow p-4', className)}>
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <p className="text-xs text-gray-600 uppercase tracking-wide">{title}</p>
          <p className={clsx('text-2xl font-bold mt-1', colorClasses[color])}>
            {typeof value === 'number' ? value.toLocaleString() : value}
          </p>
          {subtitle && (
            <p className="text-xs text-gray-500 mt-1">{subtitle}</p>
          )}
        </div>
        {icon && (
          <div className={clsx('p-2 rounded-lg bg-gray-50', colorClasses[color])}>
            {icon}
          </div>
        )}
      </div>
    </div>
  );
}