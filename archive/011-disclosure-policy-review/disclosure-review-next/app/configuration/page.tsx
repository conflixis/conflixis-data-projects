'use client';

import React, { useState, useEffect } from 'react';
import MainLayout from '@/components/layout/MainLayout';
import { Settings, Save, Upload, Download, AlertCircle, ChevronRight, Database, Code, Shield, History } from 'lucide-react';

interface Threshold {
  tier: string;
  min: number;
  max: number;
  name: string;
  risk_level: string;
  management_plan_required: boolean;
}

interface PolicyConfig {
  thresholds: Threshold[];
  scoring_weights?: {
    [key: string]: number;
  };
  last_updated?: string;
}

export default function ConfigurationPage() {
  const [activeTab, setActiveTab] = useState('thresholds');
  const [config, setConfig] = useState<PolicyConfig | null>(null);
  const [loading, setLoading] = useState(true);
  const [hasChanges, setHasChanges] = useState(false);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    loadConfiguration();
  }, []);

  const loadConfiguration = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/policies/configuration');
      if (response.ok) {
        const data = await response.json();
        setConfig(data);
      } else {
        // Use default configuration
        setConfig(getDefaultConfiguration());
      }
    } catch (error) {
      console.error('Failed to load configuration:', error);
      setConfig(getDefaultConfiguration());
    } finally {
      setLoading(false);
    }
  };

  const getDefaultConfiguration = (): PolicyConfig => ({
    thresholds: [
      {
        tier: 'tier_1_low',
        min: 0,
        max: 249,
        name: 'Tier 1 - Low Risk',
        risk_level: 'low',
        management_plan_required: false,
      },
      {
        tier: 'tier_2_moderate',
        min: 250,
        max: 4999,
        name: 'Tier 2 - Moderate Risk',
        risk_level: 'moderate',
        management_plan_required: false,
      },
      {
        tier: 'tier_3_high',
        min: 5000,
        max: 49999,
        name: 'Tier 3 - High Risk',
        risk_level: 'high',
        management_plan_required: true,
      },
      {
        tier: 'tier_4_critical',
        min: 50000,
        max: 999999999,
        name: 'Tier 4 - Critical Risk',
        risk_level: 'critical',
        management_plan_required: true,
      },
    ],
    scoring_weights: {
      financial_amount: 0.4,
      relationship_type: 0.3,
      entity_category: 0.2,
      duration: 0.1,
    },
  });

  const updateThreshold = (tier: string, field: keyof Threshold, value: any) => {
    if (!config) return;
    
    const updatedThresholds = config.thresholds.map(t => 
      t.tier === tier ? { ...t, [field]: value } : t
    );
    
    setConfig({ ...config, thresholds: updatedThresholds });
    setHasChanges(true);
  };

  const saveConfiguration = async () => {
    if (!config || !hasChanges) return;
    
    setSaving(true);
    try {
      const response = await fetch('http://localhost:8000/api/policies/configuration', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(config),
      });
      
      if (response.ok) {
        setHasChanges(false);
        // Show success message
      }
    } catch (error) {
      console.error('Failed to save configuration:', error);
    } finally {
      setSaving(false);
    }
  };

  const exportConfiguration = () => {
    if (!config) return;
    
    const dataStr = JSON.stringify(config, null, 2);
    const dataUri = 'data:application/json;charset=utf-8,'+ encodeURIComponent(dataStr);
    const exportFileDefaultName = `coi-config-${new Date().toISOString().slice(0,10)}.json`;
    
    const linkElement = document.createElement('a');
    linkElement.setAttribute('href', dataUri);
    linkElement.setAttribute('download', exportFileDefaultName);
    linkElement.click();
  };

  const importConfiguration = () => {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = '.json';
    
    input.onchange = async (e: any) => {
      const file = e.target.files[0];
      const text = await file.text();
      try {
        const importedConfig = JSON.parse(text);
        setConfig(importedConfig);
        setHasChanges(true);
      } catch (error) {
        console.error('Invalid configuration file:', error);
      }
    };
    
    input.click();
  };

  const tierColors: { [key: string]: string } = {
    'tier_1_low': 'border-green-300 bg-green-50',
    'tier_2_moderate': 'border-yellow-300 bg-yellow-50',
    'tier_3_high': 'border-orange-300 bg-orange-50',
    'tier_4_critical': 'border-red-300 bg-red-50',
  };

  const tierTextColors: { [key: string]: string } = {
    'tier_1_low': 'text-green-700',
    'tier_2_moderate': 'text-yellow-700',
    'tier_3_high': 'text-orange-700',
    'tier_4_critical': 'text-red-700',
  };

  if (loading) {
    return (
      <MainLayout>
        <div className="min-h-screen flex items-center justify-center">
          <div className="text-center">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-conflixis-green mx-auto"></div>
            <p className="mt-4 text-gray-600">Loading configuration...</p>
          </div>
        </div>
      </MainLayout>
    );
  }

  return (
    <MainLayout>
      <div className="min-h-screen bg-conflixis-ivory">
        {/* Header */}
        <div className="bg-white shadow-sm border-b border-gray-200">
          <div className="px-6 py-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <Settings className="w-6 h-6 text-conflixis-green" />
                <div>
                  <h1 className="text-2xl font-bold text-conflixis-green">Policy Configuration</h1>
                  <p className="text-sm text-gray-600">Manage risk thresholds and compliance policies</p>
                </div>
              </div>
              <div className="flex items-center gap-2">
                {hasChanges && (
                  <span className="text-sm text-amber-600 flex items-center gap-1">
                    <AlertCircle className="w-4 h-4" />
                    Unsaved changes
                  </span>
                )}
                <button
                  onClick={saveConfiguration}
                  disabled={!hasChanges || saving}
                  className="px-4 py-2 bg-conflixis-green text-white rounded-lg hover:bg-opacity-90 disabled:opacity-50 flex items-center gap-2"
                >
                  <Save className="w-4 h-4" />
                  {saving ? 'Saving...' : 'Save Changes'}
                </button>
                <button
                  onClick={exportConfiguration}
                  className="px-4 py-2 border border-conflixis-green text-conflixis-green rounded-lg hover:bg-conflixis-green hover:text-white flex items-center gap-2"
                >
                  <Download className="w-4 h-4" />
                  Export
                </button>
                <button
                  onClick={importConfiguration}
                  className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 flex items-center gap-2"
                >
                  <Upload className="w-4 h-4" />
                  Import
                </button>
              </div>
            </div>
          </div>
        </div>

        {/* Tabs */}
        <div className="bg-white border-b border-gray-200">
          <div className="px-6">
            <div className="flex gap-0">
              {['thresholds', 'mappings', 'triggers', 'scoring', 'audit'].map((tab) => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  className={`px-6 py-3 text-sm font-medium uppercase tracking-wide transition-all relative ${
                    activeTab === tab
                      ? 'text-conflixis-green bg-white border-b-3 border-conflixis-gold'
                      : 'text-gray-600 hover:text-conflixis-green hover:bg-gray-50'
                  }`}
                >
                  {tab.charAt(0).toUpperCase() + tab.slice(1)}
                  {activeTab === tab && (
                    <div className="absolute bottom-0 left-0 right-0 h-[3px] bg-gradient-to-r from-conflixis-gold to-yellow-400" />
                  )}
                </button>
              ))}
            </div>
          </div>
        </div>

        {/* Content */}
        <div className="p-6">
          {activeTab === 'thresholds' && config && (
            <div className="max-w-6xl mx-auto">
              <div className="mb-6">
                <h2 className="text-xl font-semibold text-gray-900 mb-2">Risk Thresholds</h2>
                <p className="text-sm text-gray-600">
                  Define financial thresholds for automatic risk tier assignment
                </p>
              </div>

              <div className="space-y-4">
                {config.thresholds.map((threshold) => (
                  <div
                    key={threshold.tier}
                    className={`p-6 rounded-lg border-l-4 shadow-sm ${tierColors[threshold.tier] || 'border-gray-300 bg-gray-50'}`}
                  >
                    <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                      <div>
                        <label className={`text-xs font-medium uppercase tracking-wide ${tierTextColors[threshold.tier] || 'text-gray-700'}`}>
                          Risk Tier
                        </label>
                        <p className="text-lg font-semibold mt-1">{threshold.name}</p>
                      </div>
                      
                      <div>
                        <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                          Minimum Amount ($)
                        </label>
                        <input
                          type="number"
                          value={threshold.min}
                          onChange={(e) => updateThreshold(threshold.tier, 'min', parseInt(e.target.value))}
                          className="mt-1 w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-conflixis-green"
                        />
                      </div>
                      
                      <div>
                        <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                          Maximum Amount ($)
                        </label>
                        <input
                          type="number"
                          value={threshold.max}
                          onChange={(e) => updateThreshold(threshold.tier, 'max', parseInt(e.target.value))}
                          className="mt-1 w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-conflixis-green"
                        />
                      </div>
                      
                      <div>
                        <label className="text-xs font-medium text-gray-500 uppercase tracking-wide">
                          Management Plan
                        </label>
                        <div className="mt-2">
                          <label className="flex items-center">
                            <input
                              type="checkbox"
                              checked={threshold.management_plan_required}
                              onChange={(e) => updateThreshold(threshold.tier, 'management_plan_required', e.target.checked)}
                              className="rounded border-gray-300 text-conflixis-green focus:ring-conflixis-green"
                            />
                            <span className="ml-2 text-sm text-gray-700">Required</span>
                          </label>
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {activeTab === 'mappings' && (
            <div className="max-w-6xl mx-auto">
              <div className="mb-6">
                <h2 className="text-xl font-semibold text-gray-900 mb-2">Policy Mappings</h2>
                <p className="text-sm text-gray-600">
                  Map disclosure categories to specific compliance policies
                </p>
              </div>
              <div className="bg-white rounded-lg shadow p-6">
                <p className="text-gray-500">Policy mapping configuration coming soon...</p>
              </div>
            </div>
          )}

          {activeTab === 'triggers' && (
            <div className="max-w-6xl mx-auto">
              <div className="mb-6">
                <h2 className="text-xl font-semibold text-gray-900 mb-2">Disclosure Triggers</h2>
                <p className="text-sm text-gray-600">
                  Configure automatic actions based on disclosure patterns
                </p>
              </div>
              <div className="bg-white rounded-lg shadow p-6">
                <p className="text-gray-500">Trigger configuration coming soon...</p>
              </div>
            </div>
          )}

          {activeTab === 'scoring' && (
            <div className="max-w-6xl mx-auto">
              <div className="mb-6">
                <h2 className="text-xl font-semibold text-gray-900 mb-2">Risk Scoring</h2>
                <p className="text-sm text-gray-600">
                  Adjust risk calculation weights and factors
                </p>
              </div>
              {config?.scoring_weights && (
                <div className="bg-white rounded-lg shadow p-6">
                  <div className="space-y-4">
                    {Object.entries(config.scoring_weights).map(([key, weight]) => (
                      <div key={key} className="flex items-center justify-between">
                        <label className="text-sm font-medium text-gray-700 capitalize">
                          {key.replace('_', ' ')}
                        </label>
                        <div className="flex items-center gap-2">
                          <input
                            type="range"
                            min="0"
                            max="1"
                            step="0.1"
                            value={weight}
                            className="w-32"
                            disabled
                          />
                          <span className="text-sm text-gray-600 w-12">{(weight * 100).toFixed(0)}%</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}

          {activeTab === 'audit' && (
            <div className="max-w-6xl mx-auto">
              <div className="mb-6">
                <h2 className="text-xl font-semibold text-gray-900 mb-2">Audit History</h2>
                <p className="text-sm text-gray-600">
                  View configuration change history
                </p>
              </div>
              <div className="bg-white rounded-lg shadow">
                <table className="w-full">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Date</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">User</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Action</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Details</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200">
                    <tr>
                      <td className="px-6 py-4 text-sm text-gray-500" colSpan={4}>
                        No audit history available
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      </div>
    </MainLayout>
  );
}