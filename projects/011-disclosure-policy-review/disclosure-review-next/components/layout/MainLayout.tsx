'use client';

import React, { useState } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { UserButton } from '@clerk/nextjs';
import { 
  Home, 
  FileText, 
  Settings, 
  Book, 
  ChevronLeft,
  ChevronRight,
  BarChart3,
  Shield,
  ExternalLink
} from 'lucide-react';

interface MainLayoutProps {
  children: React.ReactNode;
}

export default function MainLayout({ children }: MainLayoutProps) {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const pathname = usePathname();

  const navigation = [
    { name: 'Disclosure Viewer', href: '/', icon: Home },
    { name: 'Dashboard', href: '/dashboard', icon: BarChart3 },
    { name: 'Configuration', href: '/configuration', icon: Settings },
    { name: 'Data Dictionary', href: '/data-dictionary', icon: Book },
  ];

  const adminLinks = [
    { name: 'API Docs', href: 'http://localhost:8000/api/docs', icon: FileText, external: true },
    { name: 'ReDoc', href: 'http://localhost:8000/api/redoc', icon: FileText, external: true },
  ];

  const isActive = (href: string) => pathname === href;

  return (
    <div className="flex h-screen bg-conflixis-ivory">
      {/* Sidebar */}
      <aside 
        className={`${
          sidebarCollapsed ? 'w-16' : 'w-64'
        } bg-white shadow-lg transition-all duration-300 flex flex-col`}
      >
        {/* Logo */}
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between">
            <div className={`${sidebarCollapsed ? 'hidden' : 'block'}`}>
              <h1 className="text-lg font-bold text-conflixis-green">
                Disclosure Review
              </h1>
              <p className="text-xs text-gray-600">Conflixis</p>
            </div>
            <button
              onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
              className="p-1 rounded hover:bg-gray-100"
            >
              {sidebarCollapsed ? (
                <ChevronRight className="w-5 h-5" />
              ) : (
                <ChevronLeft className="w-5 h-5" />
              )}
            </button>
          </div>
        </div>

        {/* Navigation */}
        <nav className="flex-1 p-4">
          <ul className="space-y-2">
            {navigation.map((item) => (
              <li key={item.name}>
                <Link
                  href={item.href}
                  className={`flex items-center gap-3 px-3 py-2 rounded-lg transition-colors ${
                    isActive(item.href)
                      ? 'bg-conflixis-green text-white'
                      : 'text-gray-700 hover:bg-gray-100'
                  }`}
                >
                  <item.icon className="w-5 h-5 flex-shrink-0" />
                  {!sidebarCollapsed && (
                    <span className="text-sm">{item.name}</span>
                  )}
                </Link>
              </li>
            ))}
          </ul>

          {/* Admin Section */}
          <div className="mt-8 pt-8 border-t border-gray-200">
            <div className={`${sidebarCollapsed ? 'hidden' : 'block'} mb-2`}>
              <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wider">
                Admin
              </h3>
            </div>
            <ul className="space-y-2">
              {adminLinks.map((item) => (
                <li key={item.name}>
                  <a
                    href={item.href}
                    target={item.external ? '_blank' : undefined}
                    rel={item.external ? 'noopener noreferrer' : undefined}
                    className="flex items-center gap-3 px-3 py-2 rounded-lg text-gray-700 hover:bg-gray-100 transition-colors"
                  >
                    <item.icon className="w-5 h-5 flex-shrink-0" />
                    {!sidebarCollapsed && (
                      <>
                        <span className="text-sm">{item.name}</span>
                        {item.external && (
                          <ExternalLink className="w-3 h-3 ml-auto" />
                        )}
                      </>
                    )}
                  </a>
                </li>
              ))}
            </ul>
          </div>
        </nav>

        {/* User Section */}
        <div className="p-4 border-t border-gray-200">
          <div className={`flex items-center ${sidebarCollapsed ? 'justify-center' : 'gap-3'}`}>
            <UserButton afterSignOutUrl="/sign-in" />
            {!sidebarCollapsed && (
              <div className="text-sm">
                <p className="font-medium text-gray-700">Account</p>
              </div>
            )}
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-y-auto">
        {children}
      </main>
    </div>
  );
}