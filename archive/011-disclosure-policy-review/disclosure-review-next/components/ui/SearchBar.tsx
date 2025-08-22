'use client';

import React, { useState, useCallback } from 'react';
import { Search, X } from 'lucide-react';

interface SearchBarProps {
  placeholder?: string;
  value?: string;
  onChange?: (value: string) => void;
  onSearch?: (value: string) => void;
  className?: string;
}

export default function SearchBar({
  placeholder = 'Search...',
  value: controlledValue,
  onChange,
  onSearch,
  className = '',
}: SearchBarProps) {
  const [internalValue, setInternalValue] = useState('');
  const value = controlledValue !== undefined ? controlledValue : internalValue;

  const handleChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = e.target.value;
    if (controlledValue === undefined) {
      setInternalValue(newValue);
    }
    onChange?.(newValue);
  }, [controlledValue, onChange]);

  const handleSubmit = useCallback((e: React.FormEvent) => {
    e.preventDefault();
    onSearch?.(value);
  }, [value, onSearch]);

  const handleClear = useCallback(() => {
    if (controlledValue === undefined) {
      setInternalValue('');
    }
    onChange?.('');
    onSearch?.('');
  }, [controlledValue, onChange, onSearch]);

  return (
    <form onSubmit={handleSubmit} className={`relative ${className}`}>
      <div className="relative">
        <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
        <input
          type="text"
          value={value}
          onChange={handleChange}
          placeholder={placeholder}
          className="w-full pl-10 pr-10 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-conflixis-green focus:border-transparent"
        />
        {value && (
          <button
            type="button"
            onClick={handleClear}
            className="absolute right-3 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-gray-600"
          >
            <X className="w-5 h-5" />
          </button>
        )}
      </div>
    </form>
  );
}