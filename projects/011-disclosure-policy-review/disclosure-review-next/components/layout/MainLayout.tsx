'use client';

import React, { useState, useEffect } from 'react';
import Link from 'next/link';
import Image from 'next/image';
import { usePathname } from 'next/navigation';
import { UserButton } from '@clerk/nextjs';
import { 
  Home, 
  FileText, 
  Settings, 
  Book, 
  ChevronDown,
  ChevronUp,
  BarChart3,
  Shield,
  ExternalLink,
  FileText as FileIcon,
  Database
} from 'lucide-react';

interface MainLayoutProps {
  children: React.ReactNode;
}

export default function MainLayout({ children }: MainLayoutProps) {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [adminExpanded, setAdminExpanded] = useState(false);
  const [statsData, setStatsData] = useState<any>(null);
  const pathname = usePathname();

  // Fetch stats from API
  useEffect(() => {
    fetch('http://localhost:8000/api/stats/overview')
      .then(res => res.json())
      .then(data => setStatsData(data))
      .catch(err => console.error('Failed to load stats:', err));
  }, []);

  // Use stats from API
  const stats = {
    total: statsData?.total_records || 0,
    pending: statsData?.review_status_distribution?.pending || 0,
    critical: statsData?.risk_distribution?.critical || 0,
  };

  const navigation = [
    { name: 'Dashboard', href: '/dashboard', icon: Home },
    { name: 'Disclosure Viewer', href: '/', icon: Database },
  ];

  const adminLinks = [
    { name: 'API Docs', href: 'http://localhost:8000/api/docs', icon: FileText, external: true },
    { name: 'ReDoc', href: 'http://localhost:8000/api/redoc', icon: FileText, external: true },
    { name: 'Forms Showcase', href: 'http://localhost:8000/forms-showcase', icon: FileIcon, external: true },
    { name: 'Configuration', href: '/configuration', icon: Settings },
    { name: 'Data Dictionary', href: '/data-dictionary', icon: Book },
  ];

  const isActive = (href: string) => pathname === href;

  return (
    <div className="flex">
      {/* Sidebar */}
      <aside 
        id="sidebar"
        className={`fixed h-full bg-white shadow-lg z-50 transition-transform duration-300 ${
          sidebarCollapsed ? '-translate-x-[250px]' : 'translate-x-0'
        }`}
        style={{ width: '250px' }}
      >
        {/* Logo */}
        <div className="p-6 border-b">
          <div className="flex items-center space-x-3">
            <Image 
              src="/conflixis-logo.png" 
              alt="Conflixis" 
              width={40}
              height={40}
              className="h-10 w-10"
            />
            <div>
              <h2 className="text-lg font-soehneKraftig text-conflixis-green">Disclosure Review</h2>
              <p className="text-xs text-gray-600">Conflixis</p>
            </div>
          </div>
        </div>

        {/* Navigation */}
        <nav className="p-4">
          <ul className="space-y-2">
            {navigation.map((item) => (
              <li key={item.name}>
                <Link
                  href={item.href}
                  className={`flex items-center gap-3 px-4 py-3 rounded-lg transition-all ${
                    isActive(item.href)
                      ? 'bg-conflixis-green text-white'
                      : 'text-gray-700 hover:bg-gray-50 hover:pl-5'
                  }`}
                >
                  <item.icon className="w-5 h-5" />
                  <span>{item.name}</span>
                </Link>
              </li>
            ))}
            {/* Admin Menu */}
            <li>
              <div>
                <button 
                  onClick={() => setAdminExpanded(!adminExpanded)}
                  className="w-full flex items-center justify-between px-4 py-3 rounded-lg text-gray-700 hover:bg-gray-50 transition-all"
                >
                  <div className="flex items-center gap-3">
                    <Settings className="w-5 h-5" />
                    <span>Admin</span>
                  </div>
                  {adminExpanded ? (
                    <ChevronUp className="w-4 h-4" />
                  ) : (
                    <ChevronDown className="w-4 h-4" />
                  )}
                </button>
                <ul className={`ml-4 mt-2 space-y-1 overflow-hidden transition-all duration-300 ${
                  adminExpanded ? 'max-h-64' : 'max-h-0'
                }`}>
                  {adminLinks.map((item) => (
                    <li key={item.name}>
                      {item.external ? (
                        <a
                          href={item.href}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="flex items-center gap-3 px-4 py-2 text-sm rounded-lg text-gray-600 hover:bg-gray-50 hover:text-conflixis-green transition-colors"
                        >
                          <item.icon className="w-4 h-4" />
                          <span>{item.name}</span>
                        </a>
                      ) : (
                        <Link
                          href={item.href}
                          className="flex items-center gap-3 px-4 py-2 text-sm rounded-lg text-gray-600 hover:bg-gray-50 hover:text-conflixis-green transition-colors"
                        >
                          <item.icon className="w-4 h-4" />
                          <span>{item.name}</span>
                        </Link>
                      )}
                    </li>
                  ))}
                </ul>
              </div>
            </li>
          </ul>

          {/* Quick Stats */}
          <div className="mt-8 pt-8 border-t">
            <h3 className="px-4 text-xs font-semibold text-gray-500 uppercase mb-3">Quick Stats</h3>
            <div className="px-4 space-y-2">
              <div className="flex justify-between text-sm">
                <span className="text-gray-600">Total Records</span>
                <span className="font-semibold">{stats.total}</span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-gray-600">Pending Review</span>
                <span className="font-semibold text-conflixis-blue">{stats.pending}</span>
              </div>
              <div className="flex justify-between text-sm">
                <span className="text-gray-600">Critical Risk</span>
                <span className="font-semibold text-red-600">{stats.critical}</span>
              </div>
            </div>
          </div>
        </nav>

        {/* Collapse Button */}
        <div className="absolute bottom-0 w-full p-4 border-t">
          <button 
            onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
            className="w-full flex items-center justify-center gap-2 px-4 py-2 text-sm text-gray-600 hover:text-conflixis-green transition-colors"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 19l-7-7 7-7m8 14l-7-7 7-7" />
            </svg>
            <span>Collapse Sidebar</span>
          </button>
        </div>
      </aside>

      {/* Toggle Button (shown when sidebar is collapsed) */}
      {sidebarCollapsed && (
        <button 
          onClick={() => setSidebarCollapsed(false)}
          className="fixed left-0 top-20 z-40 p-2 bg-conflixis-green text-white rounded-r-lg shadow-lg"
        >
          <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
          </svg>
        </button>
      )}

      {/* Main Content */}
      <main className={`flex-1 min-h-screen bg-conflixis-ivory transition-all duration-300 ${
        sidebarCollapsed ? 'ml-0' : 'ml-[250px]'
      }`}>
        {/* Top Navigation Bar */}
        <nav className="bg-white shadow-lg sticky top-0 z-30">
          <div className="px-6 py-4">
            <div className="flex items-center justify-between">
              <div>
                <span className="text-xl font-soehneKraftig text-conflixis-green">Texas Health Resources</span>
                <span className="text-sm block text-conflixis-green">COI Policy Compliance System</span>
              </div>
              <div className="flex items-center space-x-4">
                <span className="text-sm text-gray-600">Policy Version: 06/19/2025</span>
                <button className="bg-conflixis-gold hover:bg-opacity-90 text-conflixis-green px-4 py-2 rounded-lg font-soehneKraftig transition-all">
                  Export Report
                </button>
                <UserButton afterSignOutUrl="/" />
              </div>
            </div>
          </div>
        </nav>
        {children}
      </main>
    </div>
  );
}