/**
 * This is a placeholder test to make sure the internal turbo packages are working
 * It is not intended to be used in production as-is
 */

import React, { HTMLAttributes } from 'react';

export interface CardProps extends HTMLAttributes<HTMLDivElement> {
  elevation?: 'none' | 'low' | 'medium' | 'high';
  padding?: 'none' | 'small' | 'medium' | 'large';
}

export const Card: React.FC<CardProps> = ({
                                            children,
                                            elevation = 'low',
                                            padding = 'medium',
                                            className = '',
                                            ...props
                                          }) => {
  const baseStyles = 'bg-white rounded-lg';

  const elevationStyles = {
    none: '',
    low: 'shadow',
    medium: 'shadow-md',
    high: 'shadow-lg',
  };

  const paddingStyles = {
    none: 'p-0',
    small: 'p-2',
    medium: 'p-4',
    large: 'p-6',
  };

  const combinedClassName = `${baseStyles} ${elevationStyles[elevation]} ${paddingStyles[padding]} ${className}`;

  return (
    <div className={combinedClassName} {...props}>
      {children}
    </div>
  );
};

export const CardHeader: React.FC<HTMLAttributes<HTMLDivElement>> = ({
                                                                       children,
                                                                       className = '',
                                                                       ...props
                                                                     }) => {
  return (
    <div className={`mb-4 ${className}`} {...props}>
      {children}
    </div>
  );
};

export const CardTitle: React.FC<HTMLAttributes<HTMLHeadingElement>> = ({
                                                                          children,
                                                                          className = '',
                                                                          ...props
                                                                        }) => {
  return (
    <h3 className={`text-lg font-semibold ${className}`} {...props}>
      {children}
    </h3>
  );
};

export const CardContent: React.FC<HTMLAttributes<HTMLDivElement>> = ({
                                                                        children,
                                                                        className = '',
                                                                        ...props
                                                                      }) => {
  return (
    <div className={className} {...props}>
      {children}
    </div>
  );
};

export const CardFooter: React.FC<HTMLAttributes<HTMLDivElement>> = ({
                                                                       children,
                                                                       className = '',
                                                                       ...props
                                                                     }) => {
  return (
    <div className={`mt-4 ${className}`} {...props}>
      {children}
    </div>
  );
};
