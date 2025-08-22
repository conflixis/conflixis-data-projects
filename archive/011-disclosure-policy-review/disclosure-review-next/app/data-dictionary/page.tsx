'use client';

import React, { useState } from 'react';
import MainLayout from '@/components/layout/MainLayout';
import SearchBar from '@/components/ui/SearchBar';
import { Book, Database, FileText, ChevronDown, ChevronRight } from 'lucide-react';

interface FieldDefinition {
  field: string;
  type: string;
  description: string;
  origin?: string;
  example?: string;
}

interface TableSection {
  title: string;
  description: string;
  tablePath?: string;
  fields: FieldDefinition[];
}

const dataDictionary: TableSection[] = [
  {
    title: 'Common Fields',
    description: 'Fields that appear across all disclosure forms',
    fields: [
      {
        field: 'disclosure_id',
        type: 'STRING',
        description: 'Unique identifier for each disclosure submission',
        origin: 'System Generated',
        example: 'DISC-2025-001234'
      },
      {
        field: 'campaign_name',
        type: 'STRING',
        description: 'Name of the disclosure campaign',
        origin: 'COI Survey',
        example: '2025 Texas Health COI Survey - Leaders/Providers'
      },
      {
        field: 'provider_name',
        type: 'STRING',
        description: 'Full name of the healthcare provider',
        origin: 'COI Form',
        example: 'Dr. Jane Smith'
      },
      {
        field: 'provider_npi',
        type: 'STRING',
        description: 'National Provider Identifier',
        origin: 'member_shards',
        example: '1234567890'
      },
      {
        field: 'job_title',
        type: 'STRING',
        description: 'Provider\'s job title or specialty',
        origin: 'member_shards',
        example: 'Cardiologist'
      },
      {
        field: 'entity',
        type: 'STRING',
        description: 'Healthcare entity affiliation',
        origin: 'member_shards',
        example: 'Texas Health Resources'
      },
      {
        field: 'manager_name',
        type: 'STRING',
        description: 'Name of the provider\'s manager',
        origin: 'member_shards',
        example: 'John Manager'
      },
      {
        field: 'submission_date',
        type: 'DATE',
        description: 'Date when disclosure was submitted',
        origin: 'COI Form',
        example: '2025-01-15'
      },
      {
        field: 'risk_tier',
        type: 'STRING',
        description: 'Calculated risk level based on disclosure',
        origin: 'Policy Engine',
        example: 'low | moderate | high | critical'
      },
      {
        field: 'review_status',
        type: 'STRING',
        description: 'Current review status of the disclosure',
        origin: 'Review System',
        example: 'pending | in_review | approved | requires_management'
      },
      {
        field: 'management_plan_required',
        type: 'BOOLEAN',
        description: 'Whether a management plan is required',
        origin: 'Policy Engine',
        example: 'true | false'
      }
    ]
  },
  {
    title: 'External Roles & Relationships',
    description: 'Fields specific to external role disclosures',
    tablePath: 'conflixis-web.thr_conflict_of_interest_disclosure_member_forms.external_roles',
    fields: [
      {
        field: 'relationship_type',
        type: 'STRING',
        description: 'Type of external relationship',
        example: 'Board Member | Consultant | Speaker'
      },
      {
        field: 'entity_name',
        type: 'STRING',
        description: 'Name of external organization',
        example: 'Medical Device Company XYZ'
      },
      {
        field: 'role_description',
        type: 'STRING',
        description: 'Description of role or responsibilities',
        example: 'Advisory Board Member'
      },
      {
        field: 'compensation_type',
        type: 'STRING',
        description: 'Type of compensation received',
        example: 'Honorarium | Equity | Salary'
      },
      {
        field: 'annual_value',
        type: 'NUMERIC',
        description: 'Annual value of compensation',
        example: '50000.00'
      },
      {
        field: 'start_date',
        type: 'DATE',
        description: 'Start date of relationship',
        example: '2024-01-01'
      },
      {
        field: 'end_date',
        type: 'DATE',
        description: 'End date of relationship (if applicable)',
        example: '2025-12-31'
      }
    ]
  },
  {
    title: 'Healthcare Industry Relationships',
    description: 'Fields for healthcare industry relationships',
    tablePath: 'conflixis-web.thr_conflict_of_interest_disclosure_member_forms.industry_relationships',
    fields: [
      {
        field: 'company_name',
        type: 'STRING',
        description: 'Name of healthcare company',
        example: 'Pharma Corp'
      },
      {
        field: 'relationship_category',
        type: 'STRING',
        description: 'Category of industry relationship',
        example: 'Speaker Bureau | Consulting | Research'
      },
      {
        field: 'product_area',
        type: 'STRING',
        description: 'Product or therapy area',
        example: 'Cardiovascular Devices'
      },
      {
        field: 'financial_amount',
        type: 'NUMERIC',
        description: 'Total financial amount received',
        example: '25000.00'
      },
      {
        field: 'payment_frequency',
        type: 'STRING',
        description: 'Frequency of payments',
        example: 'Monthly | Quarterly | Annual'
      }
    ]
  },
  {
    title: 'Research Interests',
    description: 'Fields related to research activities',
    tablePath: 'conflixis-web.thr_conflict_of_interest_disclosure_member_forms.research',
    fields: [
      {
        field: 'study_title',
        type: 'STRING',
        description: 'Title of research study',
        example: 'Phase III Clinical Trial ABC'
      },
      {
        field: 'sponsor_name',
        type: 'STRING',
        description: 'Study sponsor organization',
        example: 'National Institutes of Health'
      },
      {
        field: 'principal_investigator',
        type: 'STRING',
        description: 'Name of principal investigator',
        example: 'Dr. Research Lead'
      },
      {
        field: 'study_budget',
        type: 'NUMERIC',
        description: 'Total study budget',
        example: '500000.00'
      },
      {
        field: 'personal_compensation',
        type: 'NUMERIC',
        description: 'Personal compensation from study',
        example: '50000.00'
      },
      {
        field: 'study_phase',
        type: 'STRING',
        description: 'Clinical trial phase',
        example: 'Phase I | Phase II | Phase III | Phase IV'
      }
    ]
  },
  {
    title: 'Open Payments Data',
    description: 'Fields from CMS Open Payments integration',
    tablePath: 'analytics.open_payments_matched',
    fields: [
      {
        field: 'open_payments_id',
        type: 'STRING',
        description: 'CMS Open Payments record ID',
        origin: 'CMS Open Payments',
        example: 'OP-2025-123456'
      },
      {
        field: 'payment_date',
        type: 'DATE',
        description: 'Date of payment',
        origin: 'CMS Open Payments',
        example: '2025-01-10'
      },
      {
        field: 'payer_name',
        type: 'STRING',
        description: 'Name of paying entity',
        origin: 'CMS Open Payments',
        example: 'Medical Device Inc'
      },
      {
        field: 'payment_amount',
        type: 'NUMERIC',
        description: 'Amount of payment',
        origin: 'CMS Open Payments',
        example: '5000.00'
      },
      {
        field: 'payment_category',
        type: 'STRING',
        description: 'Category of payment',
        origin: 'CMS Open Payments',
        example: 'Consulting Fee | Speaking Fee | Research'
      },
      {
        field: 'matched_confidence',
        type: 'NUMERIC',
        description: 'Confidence score of provider match (0-1)',
        origin: 'Matching Algorithm',
        example: '0.95'
      }
    ]
  }
];

export default function DataDictionaryPage() {
  const [searchTerm, setSearchTerm] = useState('');
  const [expandedSections, setExpandedSections] = useState<Set<string>>(new Set(['Common Fields']));

  const toggleSection = (title: string) => {
    const newExpanded = new Set(expandedSections);
    if (newExpanded.has(title)) {
      newExpanded.delete(title);
    } else {
      newExpanded.add(title);
    }
    setExpandedSections(newExpanded);
  };

  const filteredDictionary = dataDictionary.map(section => ({
    ...section,
    fields: section.fields.filter(field =>
      searchTerm === '' ||
      field.field.toLowerCase().includes(searchTerm.toLowerCase()) ||
      field.description.toLowerCase().includes(searchTerm.toLowerCase()) ||
      field.example?.toLowerCase().includes(searchTerm.toLowerCase())
    )
  })).filter(section => section.fields.length > 0);

  return (
    <MainLayout>
      <div className="min-h-screen bg-conflixis-ivory">
        {/* Header */}
        <div className="bg-white shadow-sm border-b border-gray-200">
          <div className="px-6 py-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <Book className="w-6 h-6 text-conflixis-green" />
                <div>
                  <h1 className="text-2xl font-bold text-conflixis-green">Data Dictionary</h1>
                  <p className="text-sm text-gray-600">Field definitions and data structures</p>
                </div>
              </div>
              <div className="flex items-center gap-4">
                <SearchBar
                  placeholder="Search fields..."
                  value={searchTerm}
                  onChange={setSearchTerm}
                  className="w-80"
                />
              </div>
            </div>
          </div>
        </div>

        {/* Content */}
        <div className="p-6">
          <div className="max-w-7xl mx-auto space-y-6">
            {filteredDictionary.map((section) => (
              <div key={section.title} className="bg-white rounded-lg shadow">
                {/* Section Header */}
                <div
                  className="p-4 border-b border-gray-200 cursor-pointer hover:bg-gray-50"
                  onClick={() => toggleSection(section.title)}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      {expandedSections.has(section.title) ? (
                        <ChevronDown className="w-5 h-5 text-gray-500" />
                      ) : (
                        <ChevronRight className="w-5 h-5 text-gray-500" />
                      )}
                      <div>
                        <h2 className="text-lg font-semibold text-gray-900">{section.title}</h2>
                        <p className="text-sm text-gray-600">{section.description}</p>
                        {section.tablePath && (
                          <div className="flex items-center gap-2 mt-1">
                            <Database className="w-4 h-4 text-conflixis-blue" />
                            <code className="text-xs bg-gray-100 px-2 py-0.5 rounded">
                              {section.tablePath}
                            </code>
                          </div>
                        )}
                      </div>
                    </div>
                    <span className="text-sm text-gray-500">
                      {section.fields.length} fields
                    </span>
                  </div>
                </div>

                {/* Section Content */}
                {expandedSections.has(section.title) && (
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Field</th>
                          <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
                          <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Description</th>
                          <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Origin</th>
                          <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Example</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-200">
                        {section.fields.map((field) => (
                          <tr key={field.field} className="hover:bg-gray-50">
                            <td className="px-4 py-3">
                              <code className="text-sm font-mono bg-conflixis-tan px-2 py-1 rounded">
                                {field.field}
                              </code>
                            </td>
                            <td className="px-4 py-3">
                              <span className="text-sm text-conflixis-blue font-medium">
                                {field.type}
                              </span>
                            </td>
                            <td className="px-4 py-3 text-sm text-gray-700">
                              {field.description}
                            </td>
                            <td className="px-4 py-3">
                              {field.origin && (
                                <span className="text-xs bg-gray-100 px-2 py-1 rounded">
                                  {field.origin}
                                </span>
                              )}
                            </td>
                            <td className="px-4 py-3">
                              {field.example && (
                                <code className="text-xs text-gray-600">
                                  {field.example}
                                </code>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>
    </MainLayout>
  );
}