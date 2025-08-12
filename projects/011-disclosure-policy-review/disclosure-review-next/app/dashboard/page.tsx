'use client';

import React, { useState, useEffect } from 'react';
import MainLayout from '@/components/layout/MainLayout';
import StatCard from '@/components/ui/StatCard';
import Modal from '@/components/ui/Modal';
import RiskBadge from '@/components/ui/RiskBadge';
import { useDisclosures } from '@/lib/hooks/useDisclosures';
import { Disclosure } from '@/lib/api/types';
import { 
  BarChart3, 
  TrendingUp, 
  Users, 
  AlertTriangle, 
  CheckCircle,
  Clock,
  DollarSign,
  FileText,
  PieChart,
  Activity
} from 'lucide-react';

export default function DashboardPage() {
  const [selectedDisclosure, setSelectedDisclosure] = useState<Disclosure | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [selectedCampaign, setSelectedCampaign] = useState('all');
  
  // Fetch all disclosures for statistics
  const { disclosures, isLoading } = useDisclosures({ page_size: 1000 });

  // Calculate statistics
  const stats = React.useMemo(() => {
    if (!disclosures || disclosures.length === 0) {
      return {
        total: 0,
        uniqueProviders: 0,
        approved: 0,
        pending: 0,
        inReview: 0,
        requiresManagement: 0,
        highRisk: 0,
        criticalRisk: 0,
        totalAmount: 0,
        avgAmount: 0,
      };
    }

    const uniqueProviders = new Set(disclosures.map(d => d.provider_name)).size;
    
    const statusCounts = {
      approved: disclosures.filter(d => d.review_status === 'approved').length,
      pending: disclosures.filter(d => d.review_status === 'pending').length,
      inReview: disclosures.filter(d => d.review_status === 'in_review').length,
      requiresManagement: disclosures.filter(d => d.review_status === 'requires_management').length,
    };

    const riskCounts = {
      high: disclosures.filter(d => d.risk_tier === 'high').length,
      critical: disclosures.filter(d => d.risk_tier === 'critical').length,
    };

    const totalAmount = disclosures.reduce((sum, d) => sum + (d.financial_amount || 0), 0);
    const avgAmount = totalAmount / disclosures.length;

    return {
      total: disclosures.length,
      uniqueProviders,
      ...statusCounts,
      ...riskCounts,
      totalAmount,
      avgAmount,
    };
  }, [disclosures]);

  // Get recent high-risk disclosures
  const highRiskDisclosures = React.useMemo(() => {
    return disclosures
      .filter(d => d.risk_tier === 'high' || d.risk_tier === 'critical')
      .sort((a, b) => new Date(b.submission_date).getTime() - new Date(a.submission_date).getTime())
      .slice(0, 5);
  }, [disclosures]);

  const handleViewDetails = (disclosure: Disclosure) => {
    setSelectedDisclosure(disclosure);
    setModalOpen(true);
  };

  const complianceRate = stats.total > 0 
    ? ((stats.approved / stats.total) * 100).toFixed(1)
    : '0';

  const pendingRate = stats.total > 0
    ? (((stats.pending + stats.inReview) / stats.total) * 100).toFixed(1)
    : '0';

  return (
    <MainLayout>
      <div className="min-h-screen bg-conflixis-ivory">
        {/* Header */}
        <div className="bg-white shadow-sm border-b border-gray-200">
          <div className="px-6 py-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <BarChart3 className="w-6 h-6 text-conflixis-green" />
                <div>
                  <h1 className="text-2xl font-bold text-conflixis-green">
                    COI Compliance Dashboard
                  </h1>
                  <p className="text-sm text-gray-600">
                    Real-time conflict of interest monitoring and compliance metrics
                  </p>
                </div>
              </div>
              <div className="flex items-center gap-4">
                <select
                  value={selectedCampaign}
                  onChange={(e) => setSelectedCampaign(e.target.value)}
                  className="px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-conflixis-green"
                >
                  <option value="all">All Campaigns</option>
                  <option value="2025-q1">2025 Q1 Survey</option>
                  <option value="2024-annual">2024 Annual Review</option>
                </select>
                <button className="px-4 py-2 bg-conflixis-blue text-white rounded-lg hover:bg-opacity-90">
                  Export Report
                </button>
              </div>
            </div>
          </div>
        </div>

        {/* Metrics Grid */}
        <div className="p-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
            <StatCard
              title="Total Disclosures"
              value={stats.total.toString()}
              icon={<FileText className="w-5 h-5" />}
              trend={{ value: 12, isPositive: true }}
              color="blue"
            />
            <StatCard
              title="Unique Providers"
              value={stats.uniqueProviders.toString()}
              icon={<Users className="w-5 h-5" />}
              color="green"
            />
            <StatCard
              title="Compliance Rate"
              value={`${complianceRate}%`}
              icon={<CheckCircle className="w-5 h-5" />}
              trend={{ value: 5, isPositive: true }}
              color="green"
            />
            <StatCard
              title="Pending Review"
              value={`${pendingRate}%`}
              icon={<Clock className="w-5 h-5" />}
              color="yellow"
            />
          </div>

          {/* Risk Overview */}
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mb-6">
            {/* Risk Distribution */}
            <div className="bg-white rounded-lg shadow p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
                <PieChart className="w-5 h-5 text-conflixis-green" />
                Risk Distribution
              </h3>
              <div className="space-y-3">
                <div className="flex justify-between items-center">
                  <span className="text-sm text-gray-600">Low Risk</span>
                  <span className="text-sm font-medium">
                    {disclosures.filter(d => d.risk_tier === 'low').length}
                  </span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm text-gray-600">Moderate Risk</span>
                  <span className="text-sm font-medium">
                    {disclosures.filter(d => d.risk_tier === 'moderate').length}
                  </span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm text-gray-600">High Risk</span>
                  <span className="text-sm font-medium text-orange-600">
                    {stats.highRisk}
                  </span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm text-gray-600">Critical Risk</span>
                  <span className="text-sm font-medium text-red-600">
                    {stats.criticalRisk}
                  </span>
                </div>
              </div>
            </div>

            {/* Financial Summary */}
            <div className="bg-white rounded-lg shadow p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
                <DollarSign className="w-5 h-5 text-conflixis-green" />
                Financial Summary
              </h3>
              <div className="space-y-3">
                <div>
                  <p className="text-sm text-gray-600">Total Disclosed Amount</p>
                  <p className="text-2xl font-bold text-conflixis-green">
                    ${stats.totalAmount.toLocaleString()}
                  </p>
                </div>
                <div>
                  <p className="text-sm text-gray-600">Average per Disclosure</p>
                  <p className="text-lg font-semibold text-gray-900">
                    ${stats.avgAmount.toLocaleString('en-US', { maximumFractionDigits: 0 })}
                  </p>
                </div>
              </div>
            </div>

            {/* Review Status */}
            <div className="bg-white rounded-lg shadow p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center gap-2">
                <Activity className="w-5 h-5 text-conflixis-green" />
                Review Status
              </h3>
              <div className="space-y-3">
                <div className="flex justify-between items-center">
                  <span className="text-sm text-gray-600">Approved</span>
                  <span className="text-sm font-medium text-green-600">{stats.approved}</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm text-gray-600">In Review</span>
                  <span className="text-sm font-medium text-blue-600">{stats.inReview}</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm text-gray-600">Pending</span>
                  <span className="text-sm font-medium text-yellow-600">{stats.pending}</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-sm text-gray-600">Requires Management</span>
                  <span className="text-sm font-medium text-red-600">{stats.requiresManagement}</span>
                </div>
              </div>
            </div>
          </div>

          {/* High Risk Disclosures Table */}
          <div className="bg-white rounded-lg shadow">
            <div className="px-6 py-4 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
                <AlertTriangle className="w-5 h-5 text-orange-600" />
                Recent High-Risk Disclosures
              </h3>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Provider
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Entity
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Amount
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Risk Tier
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Status
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Action
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {highRiskDisclosures.map((disclosure) => (
                    <tr key={disclosure.disclosure_id} className="hover:bg-gray-50">
                      <td className="px-6 py-4">
                        <div>
                          <p className="text-sm font-medium text-gray-900">
                            {disclosure.provider_name}
                          </p>
                          <p className="text-xs text-gray-500">{disclosure.job_title}</p>
                        </div>
                      </td>
                      <td className="px-6 py-4 text-sm text-gray-900">
                        {disclosure.entity_name}
                      </td>
                      <td className="px-6 py-4 text-sm font-mono text-gray-900">
                        ${disclosure.financial_amount?.toLocaleString() || 'N/A'}
                      </td>
                      <td className="px-6 py-4">
                        <RiskBadge tier={disclosure.risk_tier} size="sm" />
                      </td>
                      <td className="px-6 py-4">
                        <span className={`text-xs px-2 py-1 rounded-full ${
                          disclosure.review_status === 'approved' 
                            ? 'bg-green-100 text-green-700'
                            : disclosure.review_status === 'requires_management'
                            ? 'bg-red-100 text-red-700'
                            : 'bg-yellow-100 text-yellow-700'
                        }`}>
                          {disclosure.review_status.replace('_', ' ')}
                        </span>
                      </td>
                      <td className="px-6 py-4">
                        <button
                          onClick={() => handleViewDetails(disclosure)}
                          className="text-conflixis-blue hover:text-conflixis-green text-sm font-medium"
                        >
                          View Details
                        </button>
                      </td>
                    </tr>
                  ))}
                  {highRiskDisclosures.length === 0 && (
                    <tr>
                      <td colSpan={6} className="px-6 py-4 text-center text-sm text-gray-500">
                        No high-risk disclosures found
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
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