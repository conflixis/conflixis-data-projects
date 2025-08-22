'use client';

import React from 'react';
import { clsx } from 'clsx';

export type RiskTier = 'low' | 'moderate' | 'high' | 'critical';

interface RiskBadgeProps {
  tier: RiskTier;
  className?: string;
  size?: 'sm' | 'md' | 'lg';
}

export default function RiskBadge({ tier, className = '', size = 'md' }: RiskBadgeProps) {
  const baseClasses = 'inline-block font-semibold rounded-lg uppercase tracking-wide';
  
  const sizeClasses = {
    sm: 'px-2 py-0.5 text-xs',
    md: 'px-3 py-1 text-xs',
    lg: 'px-4 py-1.5 text-sm',
  };

  const colorClasses = {
    low: 'bg-green-50 text-green-700 border border-green-200',
    moderate: 'bg-yellow-50 text-yellow-700 border border-yellow-200',
    high: 'bg-orange-50 text-orange-700 border border-orange-200',
    critical: 'bg-red-50 text-red-700 border border-red-200',
  };

  return (
    <span className={clsx(baseClasses, sizeClasses[size], colorClasses[tier], className)}>
      {tier} risk
    </span>
  );
}