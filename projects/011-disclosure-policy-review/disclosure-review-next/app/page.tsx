'use client';

import React, { useState } from 'react';
import MainLayout from '@/components/layout/MainLayout';
import SearchBar from '@/components/ui/SearchBar';
import DataTable, { Column } from '@/components/ui/DataTable';
import RiskBadge from '@/components/ui/RiskBadge';
import Modal from '@/components/ui/Modal';
import { useDisclosures } from '@/lib/hooks/useDisclosures';
import { Disclosure } from '@/lib/api/types';
import { FileText, Filter, Download, RefreshCw } from 'lucide-react';

export default function DisclosureViewerPage() {
  const [searchTerm, setSearchTerm] = useState('');
  const [riskFilter, setRiskFilter] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [page, setPage] = useState(1);
  const [selectedDisclosure, setSelectedDisclosure] = useState<Disclosure | null>(null);
  const [modalOpen, setModalOpen] = useState(false);

  // Fetch disclosures with filters
  const { disclosures, total, pages, isLoading, mutate } = useDisclosures({
    page,
    page_size: 50,
    search: searchTerm,
    risk_tier: riskFilter,
    review_status: statusFilter,
  });

  const columns: Column<Disclosure>[] = [
    {
      key: 'provider_name',
      header: 'Provider Name',
      accessor: (row) => (
        <div>
          <p className="font-medium">{row.provider_name}</p>
          <p className="text-xs text-gray-500">{row.job_title || 'N/A'}</p>
        </div>
      ),
      sortable: true,
    },
    {
      key: 'category_label',
      header: 'Category',
      accessor: (row) => (
        <span className="text-sm">{row.category_label}</span>
      ),
      sortable: true,
    },
    {
      key: 'entity_name',
      header: 'Entity',
      accessor: (row) => (
        <span className="text-sm">{row.entity_name}</span>
      ),
    },
    {
      key: 'financial_amount',
      header: 'Amount',
      accessor: (row) => (
        <span className="font-mono text-sm">
          {row.financial_amount ? `$${row.financial_amount.toLocaleString()}` : '-'}
        </span>
      ),
      sortable: true,
    },
    {
      key: 'risk_tier',
      header: 'Risk Tier',
      accessor: (row) => <RiskBadge tier={row.risk_tier} size="sm" />,
      sortable: true,
    },
    {
      key: 'review_status',
      header: 'Status',
      accessor: (row) => (
        <span className={`text-xs px-2 py-1 rounded-full ${
          row.review_status === 'approved' 
            ? 'bg-green-100 text-green-700'
            : row.review_status === 'requires_management'
            ? 'bg-red-100 text-red-700'
            : 'bg-yellow-100 text-yellow-700'
        }`}>
          {row.review_status.replace('_', ' ')}
        </span>
      ),
    },
    {
      key: 'submission_date',
      header: 'Submitted',
      accessor: (row) => (
        <span className="text-sm text-gray-600">
          {new Date(row.submission_date).toLocaleDateString()}
        </span>
      ),
      sortable: true,
    },
  ];

  const handleRowClick = (disclosure: Disclosure) => {
    setSelectedDisclosure(disclosure);
    setModalOpen(true);
  };

  const handleApplyFilters = () => {
    setPage(1);
    mutate();
  };

  const handleClearFilters = () => {
    setSearchTerm('');
    setRiskFilter('');
    setStatusFilter('');
    setPage(1);
    mutate();
  };

  return (
    <MainLayout>
      <div className="min-h-screen bg-conflixis-ivory">
        {/* Header */}
        <div className="bg-white shadow-sm border-b border-gray-200">
          <div className="px-6 py-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <FileText className="w-6 h-6 text-conflixis-green" />
                <div>
                  <h1 className="text-2xl font-bold text-conflixis-green">Disclosure Viewer</h1>
                  <p className="text-sm text-gray-600">
                    Showing {disclosures.length} of {total} disclosures
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <button 
                  onClick={() => mutate()}
                  className="p-2 text-gray-600 hover:text-conflixis-green"
                >
                  <RefreshCw className="w-5 h-5" />
                </button>
                <button className="px-4 py-2 border border-conflixis-green text-conflixis-green rounded-lg hover:bg-conflixis-green hover:text-white transition-colors">
                  <Download className="w-4 h-4 inline mr-2" />
                  Export
                </button>
              </div>
            </div>
          </div>
        </div>

        {/* Filters */}
        <div className="bg-white border-b border-gray-200 p-6">
          <div className="flex flex-wrap gap-4 items-end">
            <div className="flex-1 min-w-[300px]">
              <label className="block text-sm font-medium text-gray-700 mb-1">Search</label>
              <SearchBar
                placeholder="Search by name, category, or entity..."
                value={searchTerm}
                onChange={setSearchTerm}
              />
            </div>
            
            <div className="min-w-[150px]">
              <label className="block text-sm font-medium text-gray-700 mb-1">Risk Tier</label>
              <select
                value={riskFilter}
                onChange={(e) => setRiskFilter(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-conflixis-green"
              >
                <option value="">All Tiers</option>
                <option value="low">Low Risk</option>
                <option value="moderate">Moderate Risk</option>
                <option value="high">High Risk</option>
                <option value="critical">Critical Risk</option>
              </select>
            </div>
            
            <div className="min-w-[150px]">
              <label className="block text-sm font-medium text-gray-700 mb-1">Review Status</label>
              <select
                value={statusFilter}
                onChange={(e) => setStatusFilter(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-conflixis-green"
              >
                <option value="">All Status</option>
                <option value="pending">Pending Review</option>
                <option value="in_review">In Review</option>
                <option value="approved">Approved</option>
                <option value="requires_management">Requires Management</option>
              </select>
            </div>
            
            <button
              onClick={handleApplyFilters}
              className="px-6 py-2 bg-conflixis-blue text-white rounded-lg hover:bg-opacity-90"
            >
              Apply Filters
            </button>
            
            <button
              onClick={handleClearFilters}
              className="px-4 py-2 text-conflixis-green hover:text-conflixis-blue"
            >
              Clear
            </button>
          </div>
        </div>

        {/* Data Table */}
        <div className="p-6">
          <DataTable
            columns={columns}
            data={disclosures}
            keyExtractor={(row) => row.disclosure_id}
            onRowClick={handleRowClick}
            loading={isLoading}
            emptyMessage="No disclosures found matching your filters"
          />
          
          {/* Pagination */}
          {pages > 1 && (
            <div className="mt-4 flex justify-center gap-2">
              <button
                onClick={() => setPage(Math.max(1, page - 1))}
                disabled={page === 1}
                className="px-4 py-2 border rounded-lg disabled:opacity-50"
              >
                Previous
              </button>
              <span className="px-4 py-2">
                Page {page} of {pages}
              </span>
              <button
                onClick={() => setPage(Math.min(pages, page + 1))}
                disabled={page === pages}
                className="px-4 py-2 border rounded-lg disabled:opacity-50"
              >
                Next
              </button>
            </div>
          )}
        </div>

        {/* Detail Modal */}
        <Modal
          isOpen={modalOpen}
          onClose={() => setModalOpen(false)}
          title={selectedDisclosure ? `${selectedDisclosure.category_label} - ${selectedDisclosure.relationship_type}` : ''}
          size="xl"
        >
          {selectedDisclosure && (
            <div className="space-y-4">
              {/* Section 1: Person Information */}
              <div className="bg-white border border-gray-200 rounded-lg p-4">
                <h5 className="font-semibold text-sm mb-3 text-gray-700 uppercase tracking-wide">
                  Person Information
                </h5>
                <div className="grid grid-cols-2 gap-x-4 gap-y-2">
                  <div>
                    <label className="text-xs text-gray-500">Name</label>
                    <p className="text-sm font-medium">{selectedDisclosure.provider_name}</p>
                  </div>
                  <div>
                    <label className="text-xs text-gray-500">Manager</label>
                    <p className="text-sm">{selectedDisclosure.manager_name || 'Not specified'}</p>
                  </div>
                  <div>
                    <label className="text-xs text-gray-500">Job Title</label>
                    <p className="text-sm">{selectedDisclosure.job_title || 'Not specified'}</p>
                  </div>
                  <div>
                    <label className="text-xs text-gray-500">NPI</label>
                    <p className="text-sm font-mono">{selectedDisclosure.provider_npi || 'N/A'}</p>
                  </div>
                </div>
              </div>

              {/* Section 2: Disclosure Information */}
              <div className="bg-white border border-gray-200 rounded-lg p-4">
                <h5 className="font-semibold text-sm mb-3 text-gray-700 uppercase tracking-wide">
                  Disclosure Information
                </h5>
                <div className="grid grid-cols-2 gap-x-4 gap-y-2">
                  <div>
                    <label className="text-xs text-gray-500">Entity</label>
                    <p className="text-sm">{selectedDisclosure.entity_name}</p>
                  </div>
                  <div>
                    <label className="text-xs text-gray-500">Type</label>
                    <p className="text-sm">{selectedDisclosure.disclosure_type || selectedDisclosure.relationship_type}</p>
                  </div>
                  <div>
                    <label className="text-xs text-gray-500">Amount</label>
                    <p className="text-sm font-semibold">
                      {selectedDisclosure.financial_amount ? 
                        `$${selectedDisclosure.financial_amount.toLocaleString()}` : 'N/A'}
                    </p>
                  </div>
                  <div>
                    <label className="text-xs text-gray-500">Date</label>
                    <p className="text-sm">{selectedDisclosure.submission_date}</p>
                  </div>
                </div>
              </div>

              {/* Section 3: Management & Review Status */}
              <div className="bg-white border border-gray-200 rounded-lg p-4">
                <h5 className="font-semibold text-sm mb-3 text-gray-700 uppercase tracking-wide">
                  Management & Review Status
                </h5>
                <div className="grid grid-cols-2 gap-x-4 gap-y-2">
                  <div>
                    <label className="text-xs text-gray-500">Risk Tier</label>
                    <div className="mt-1">
                      <RiskBadge tier={selectedDisclosure.risk_tier} size="sm" />
                    </div>
                  </div>
                  <div>
                    <label className="text-xs text-gray-500">Review Status</label>
                    <p className="text-sm capitalize">{selectedDisclosure.review_status.replace('_', ' ')}</p>
                  </div>
                  <div>
                    <label className="text-xs text-gray-500">Management Plan</label>
                    <p className={`text-sm font-semibold ${
                      selectedDisclosure.management_plan_required ? 'text-red-600' : 'text-green-600'
                    }`}>
                      {selectedDisclosure.management_plan_required ? 'Required' : 'Not Required'}
                    </p>
                  </div>
                  <div>
                    <label className="text-xs text-gray-500">Next Review</label>
                    <p className="text-sm">{selectedDisclosure.next_review_date}</p>
                  </div>
                </div>
              </div>
            </div>
          )}
        </Modal>
      </div>
    </MainLayout>
  );
}