'use client';

import React, { useEffect, useRef } from 'react';
import { X } from 'lucide-react';

interface ModalProps {
  isOpen: boolean;
  onClose: () => void;
  title?: string;
  children: React.ReactNode;
  size?: 'sm' | 'md' | 'lg' | 'xl' | 'full';
}

export default function Modal({ 
  isOpen, 
  onClose, 
  title, 
  children, 
  size = 'lg' 
}: ModalProps) {
  const modalRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };

    if (isOpen) {
      document.addEventListener('keydown', handleEscape);
      document.body.style.overflow = 'hidden';
    }

    return () => {
      document.removeEventListener('keydown', handleEscape);
      document.body.style.overflow = 'unset';
    };
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  const sizeClasses = {
    sm: 'max-w-md',
    md: 'max-w-2xl',
    lg: 'max-w-4xl',
    xl: 'max-w-6xl',
    full: 'max-w-[95vw]',
  };

  return (
    <div className="fixed inset-0 z-50">
      {/* Backdrop */}
      <div 
        className="modal-backdrop fixed inset-0 bg-black bg-opacity-50"
        onClick={onClose}
      />
      
      {/* Modal Container */}
      <div className="flex items-center justify-center min-h-screen p-4">
        <div 
          ref={modalRef}
          className={`modal-content bg-white rounded-lg shadow-2xl ${sizeClasses[size]} w-full max-h-[90vh] overflow-y-auto relative`}
        >
          {/* Header */}
          <div className="sticky top-0 bg-white border-b border-gray-200 p-6 flex justify-between items-start">
            {title && (
              <h3 className="text-2xl font-bold text-conflixis-green">{title}</h3>
            )}
            <button
              onClick={onClose}
              className="ml-auto text-gray-500 hover:text-gray-700 transition-colors"
            >
              <X className="w-6 h-6" />
            </button>
          </div>
          
          {/* Content */}
          <div className="p-6">
            {children}
          </div>
        </div>
      </div>

      <style jsx>{`
        @keyframes modalFadeIn {
          from { opacity: 0; }
          to { opacity: 1; }
        }
        
        @keyframes modalSlideIn {
          from {
            opacity: 0;
            transform: translateY(-20px) scale(0.95);
          }
          to {
            opacity: 1;
            transform: translateY(0) scale(1);
          }
        }
        
        .modal-backdrop {
          animation: modalFadeIn 0.3s ease-out;
        }
        
        .modal-content {
          animation: modalSlideIn 0.3s ease-out;
        }
      `}</style>
    </div>
  );
}