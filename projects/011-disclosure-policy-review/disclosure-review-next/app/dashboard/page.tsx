'use client';

import React, { useState, useEffect } from 'react';
import MainLayout from '@/components/layout/MainLayout';
import { useDisclosures } from '@/lib/hooks/useDisclosures';
import Modal from '@/components/ui/Modal';
import { Disclosure } from '@/lib/api/types';

export default function DashboardPage() {
  const [selectedCampaign, setSelectedCampaign] = useState('2025');
  const [selectedDisclosure, setSelectedDisclosure] = useState<Disclosure | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  
  // Fetch all disclosures
  const { disclosures = [], isLoading } = useDisclosures({ page_size: 1000 });

  // Filter disclosures by campaign
  const filteredDisclosures = disclosures.filter(d => {
    if (selectedCampaign === 'all') return true;
    // Filter logic based on campaign
    return d.campaign_name?.includes(selectedCampaign);
  });

  // Calculate statistics
  const stats = {
    uniqueProviders: new Set(filteredDisclosures.map(d => d.provider_name)).size,
    totalDisclosures: filteredDisclosures.length,
    compliant: filteredDisclosures.filter(d => d.review_status === 'approved').length,
    pendingReview: filteredDisclosures.filter(d => d.review_status === 'pending' || d.review_status === 'in_review').length,
    overdue: 261, // Sample data
    notRequired: 93, // Sample data
  };

  const totalPersons = 3847; // Sample total
  const compliancePercentage = totalPersons > 0 ? ((stats.compliant / totalPersons) * 100).toFixed(1) : '0';
  const pendingPercentage = totalPersons > 0 ? ((stats.pendingReview / totalPersons) * 100).toFixed(1) : '0';

  return (
    <MainLayout>
      <div className="min-h-screen bg-conflixis-ivory">
        {/* Top Navigation */}
        <nav className="bg-white shadow-lg sticky top-0 z-30">
          <div className="px-6 py-4">
            <div className="flex items-center justify-between">
              <div>
                <span className="text-xl font-bold text-conflixis-green">Texas Health Resources</span>
                <span className="text-sm block text-conflixis-green">COI Policy Compliance System</span>
              </div>
              <div className="flex items-center space-x-4">
                <span className="text-sm text-gray-600">Policy Version: 06/19/2025</span>
                <button className="bg-conflixis-gold hover:bg-opacity-90 text-conflixis-green px-4 py-2 rounded-lg font-bold transition-all">
                  Export Report
                </button>
              </div>
            </div>
          </div>
        </nav>

        {/* Summary Statistics and Compliance Overview */}
        <section className="py-6">
          <div className="container mx-auto px-6">
            <h1 className="text-3xl font-bold mb-6 text-conflixis-green">COI Policy Compliance Dashboard</h1>
            
            <div className="grid lg:grid-cols-8 gap-6 mb-4">
              {/* Left Column: Campaign Selection and Metrics */}
              <div className="lg:col-span-2 flex flex-col gap-4">
                {/* Campaign Selection Card */}
                <div className="bg-white rounded-lg p-4 shadow">
                  <label className="text-sm font-bold text-conflixis-green block mb-2">Campaign Filter</label>
                  <select 
                    value={selectedCampaign}
                    onChange={(e) => setSelectedCampaign(e.target.value)}
                    className="w-full text-xs border border-gray-300 rounded px-2 py-1.5 focus:outline-none focus:border-conflixis-green"
                  >
                    <option value="all">All Campaigns</option>
                    <option value="2025">2025 Texas Health COI Survey - Leaders/Providers</option>
                    <option value="2024-workforce">2024 Texas Health COI Self-Identification Follow-Up - Workforce</option>
                    <option value="2024-leadership">2024 Texas Health COI Self-Identification Follow-Up - Leadership</option>
                    <option value="new-hires">New Hires Conflict of Interest Disclosure</option>
                    <option value="research">Conflict of Interest Disclosure (COI) - Research</option>
                  </select>
                </div>
                
                {/* Metrics Row */}
                <div className="grid grid-cols-2 gap-4 flex-1">
                  {/* Total Covered Persons Card */}
                  <div className="bg-white rounded-lg p-4 shadow flex flex-col justify-center items-center text-center">
                    <h2 className="text-sm font-bold mb-2">Total Covered Persons</h2>
                    <div className="text-3xl font-bold text-conflixis-blue bg-blue-100 rounded-lg px-6 py-2 mb-2">
                      {stats.uniqueProviders}
                    </div>
                    <div className="text-xs text-blue-600 font-medium">Unique providers</div>
                  </div>
                  
                  {/* Total Disclosures Card */}
                  <div className="bg-white rounded-lg p-4 shadow flex flex-col justify-center items-center text-center">
                    <h2 className="text-sm font-bold mb-2">Total Disclosures</h2>
                    <div className="text-3xl font-bold text-conflixis-green bg-green-50 rounded-lg px-6 py-2 mb-2">
                      {stats.totalDisclosures}
                    </div>
                    <div className="text-xs text-green-600 font-medium">Submitted for campaign</div>
                  </div>
                </div>
              </div>
              
              {/* Compliance Status Breakdown */}
              <div className="lg:col-span-3 bg-white rounded-lg shadow p-6 flex flex-col h-full">
                <h2 className="text-lg font-bold mb-4">Annual Disclosure Compliance Status</h2>
                
                <div className="flex-1 flex flex-col justify-center space-y-3">
                  {/* Compliant */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-bold">Compliant</div>
                    <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                      <div className="bg-green-500 h-6 rounded-full transition-all duration-1000" 
                           style={{ width: '72.3%' }}></div>
                    </div>
                    <div className="w-24 text-right text-xs">2,781 (72.3%)</div>
                  </div>
                  
                  {/* Pending Review */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-bold">Pending Review</div>
                    <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                      <div className="bg-conflixis-gold h-6 rounded-full transition-all duration-1000" 
                           style={{ width: '18.5%' }}></div>
                    </div>
                    <div className="w-24 text-right text-xs">712 (18.5%)</div>
                  </div>
                  
                  {/* Overdue */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-bold">Overdue</div>
                    <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                      <div className="bg-red-500 h-6 rounded-full transition-all duration-1000" 
                           style={{ width: '6.8%' }}></div>
                    </div>
                    <div className="w-24 text-right text-xs">261 (6.8%)</div>
                  </div>
                  
                  {/* Not Required */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-bold">Not Required</div>
                    <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                      <div className="bg-gray-400 h-6 rounded-full transition-all duration-1000" 
                           style={{ width: '2.4%' }}></div>
                    </div>
                    <div className="w-24 text-right text-xs">93 (2.4%)</div>
                  </div>
                </div>
              </div>
              
              {/* Covered Persons by Category */}
              <div className="lg:col-span-3 bg-white rounded-lg shadow p-6 flex flex-col h-full">
                <h2 className="text-lg font-bold mb-4">Covered Persons by Category</h2>
                
                <div className="flex-1 flex flex-col justify-center">
                  <div className="space-y-3">
                    <div className="flex items-center justify-between">
                      <span className="text-sm">Physicians</span>
                      <span className="text-sm font-bold">1,847</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-sm">Leadership</span>
                      <span className="text-sm font-bold">423</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-sm">Board Members</span>
                      <span className="text-sm font-bold">87</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-sm">Researchers</span>
                      <span className="text-sm font-bold">215</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-sm">Other</span>
                      <span className="text-sm font-bold">1,275</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Insurmountable Conflicts Section */}
        <section className="pb-6">
          <div className="container mx-auto px-6">
            <div className="mb-4 flex justify-between items-start">
              <div>
                <h2 className="text-2xl font-bold text-conflixis-green">Insurmountable Conflicts Identified</h2>
                <p className="text-sm text-gray-600 mt-1">
                  Conflicts requiring immediate resolution or disqualification from board/committee positions
                </p>
              </div>
              <a href="/disclosures" 
                 className="bg-conflixis-blue hover:bg-opacity-90 text-white px-4 py-2 rounded-lg font-bold transition-all text-sm">
                View All Disclosures →
              </a>
            </div>
            
            <div className="bg-white rounded-lg shadow overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-conflixis-tan">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 uppercase">Person</th>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 uppercase">Position</th>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 uppercase">THR Entity</th>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 uppercase">Disclosure Category</th>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 uppercase">Status</th>
                      <th className="px-4 py-3 text-left text-xs font-bold text-gray-700 uppercase">Action</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200">
                    <tr className="hover:bg-gray-50">
                      <td className="px-4 py-3">
                        <div className="text-sm font-bold">Robert Anderson</div>
                        <div className="text-xs text-gray-500">randerson@example.com</div>
                      </td>
                      <td className="px-4 py-3 text-sm">Board Committee Candidate</td>
                      <td className="px-4 py-3">
                        <span className="px-2 py-1 text-xs rounded-full bg-red-500 text-white">Major Supplier</span>
                      </td>
                      <td className="px-4 py-3 text-sm">
                        <div className="flex items-center gap-2">
                          <span>MedDevice Corp</span>
                          <span className="px-2 py-0.5 text-xs rounded-full bg-gray-200 text-gray-700">Supplier Match</span>
                        </div>
                      </td>
                      <td className="px-4 py-3">
                        <span className="px-2 py-1 text-xs rounded-full bg-red-100 text-red-800">Disqualified</span>
                      </td>
                      <td className="px-4 py-3">
                        <button className="text-conflixis-blue hover:text-conflixis-green text-sm font-bold">
                          Review →
                        </button>
                      </td>
                    </tr>
                    
                    <tr className="hover:bg-gray-50">
                      <td className="px-4 py-3">
                        <div className="text-sm font-bold">Jennifer Martinez</div>
                        <div className="text-xs text-gray-500">jmartinez@example.com</div>
                      </td>
                      <td className="px-4 py-3 text-sm">Medical Director</td>
                      <td className="px-4 py-3">
                        <span className="px-2 py-1 text-xs rounded-full bg-red-500 text-white">Competing Healthcare</span>
                      </td>
                      <td className="px-4 py-3 text-sm">Regional Health System</td>
                      <td className="px-4 py-3">
                        <span className="px-2 py-1 text-xs rounded-full bg-yellow-100 text-yellow-800">Under Review</span>
                      </td>
                      <td className="px-4 py-3">
                        <button className="text-conflixis-blue hover:text-conflixis-green text-sm font-bold">
                          Review →
                        </button>
                      </td>
                    </tr>
                    
                    <tr className="hover:bg-gray-50">
                      <td className="px-4 py-3">
                        <div className="text-sm font-bold">Thomas Wilson</div>
                        <div className="text-xs text-gray-500">twilson@example.com</div>
                      </td>
                      <td className="px-4 py-3 text-sm">Board Member Candidate</td>
                      <td className="px-4 py-3">
                        <span className="px-2 py-1 text-xs rounded-full bg-red-500 text-white">Elected Official</span>
                      </td>
                      <td className="px-4 py-3 text-sm">State Legislature</td>
                      <td className="px-4 py-3">
                        <span className="px-2 py-1 text-xs rounded-full bg-red-100 text-red-800">Disqualified</span>
                      </td>
                      <td className="px-4 py-3">
                        <button className="text-conflixis-blue hover:text-conflixis-green text-sm font-bold">
                          Review →
                        </button>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </section>

        {/* Policy Requirements Overview */}
        <section className="pb-8">
          <div className="container mx-auto px-6">
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-bold mb-4">Policy Requirements & Timelines</h2>
              
              <div className="grid md:grid-cols-2 gap-6">
                <div>
                  <h3 className="font-bold text-sm text-gray-700 mb-3">Disclosure Timelines</h3>
                  <div className="space-y-2">
                    <div className="flex items-start">
                      <div className="w-4 h-4 bg-conflixis-green rounded-full mt-0.5 flex-shrink-0"></div>
                      <div className="ml-3">
                        <p className="text-sm font-bold">Initial Disclosure</p>
                        <p className="text-xs text-gray-600">Promptly upon becoming subject to policy</p>
                      </div>
                    </div>
                    <div className="flex items-start">
                      <div className="w-4 h-4 bg-conflixis-gold rounded-full mt-0.5 flex-shrink-0"></div>
                      <div className="ml-3">
                        <p className="text-sm font-bold">New Situation</p>
                        <p className="text-xs text-gray-600">Immediately upon awareness</p>
                      </div>
                    </div>
                    <div className="flex items-start">
                      <div className="w-4 h-4 bg-conflixis-blue rounded-full mt-0.5 flex-shrink-0"></div>
                      <div className="ml-3">
                        <p className="text-sm font-bold">Annual Review</p>
                        <p className="text-xs text-gray-600">At least annually</p>
                      </div>
                    </div>
                  </div>
                </div>
                
                <div>
                  <h3 className="font-bold text-sm text-gray-700 mb-3">Management Actions Required</h3>
                  <div className="space-y-2">
                    <div className="flex items-center justify-between p-2 bg-conflixis-ivory rounded">
                      <span className="text-sm">Pending Initial Reviews</span>
                      <span className="text-sm font-bold text-red-600">47</span>
                    </div>
                    <div className="flex items-center justify-between p-2 bg-conflixis-ivory rounded">
                      <span className="text-sm">Management Plans Due</span>
                      <span className="text-sm font-bold text-conflixis-gold">23</span>
                    </div>
                    <div className="flex items-center justify-between p-2 bg-conflixis-ivory rounded">
                      <span className="text-sm">Committee Reviews Scheduled</span>
                      <span className="text-sm font-bold text-conflixis-blue">8</span>
                    </div>
                    <div className="flex items-center justify-between p-2 bg-conflixis-ivory rounded">
                      <span className="text-sm">Resolutions Completed</span>
                      <span className="text-sm font-bold text-green-600">156</span>
                    </div>
                  </div>
                </div>
              </div>
              
              <div className="mt-6 pt-6 border-t border-gray-200">
                <h4 className="font-bold text-sm text-gray-700 mb-3">Key Policy Thresholds</h4>
                <div className="bg-conflixis-ivory rounded-lg p-4">
                  <div className="grid md:grid-cols-3 gap-4">
                    <div>
                      <p className="text-xs text-gray-600">Financial Interest Threshold</p>
                      <p className="text-sm font-bold text-conflixis-green">{"< 1% Public Securities"}</p>
                    </div>
                    <div>
                      <p className="text-xs text-gray-600">Review Authority</p>
                      <p className="text-sm font-bold text-conflixis-green">Chief Compliance Officer</p>
                    </div>
                    <div>
                      <p className="text-xs text-gray-600">Approval Required</p>
                      <p className="text-sm font-bold text-conflixis-green">Audit & Compliance Committee</p>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Sample COI Disclosure Detail Review */}
        <section className="pb-8">
          <div className="container mx-auto px-6">
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-bold mb-4">Sample COI Disclosure Detail Review</h2>
              
              <div className="border border-gray-200 rounded-lg p-6">
                <div className="flex justify-between items-start mb-4">
                  <div>
                    <h3 className="text-lg font-bold">Dr. Michael Thompson</h3>
                    <p className="text-sm text-gray-600">Document ID: COI-2025-MT-0847</p>
                    <p className="text-sm text-gray-600">Campaign: 2025 Texas Health COI Survey - Leaders/Providers</p>
                    <p className="text-sm text-gray-600">Position: Medical Director, Cardiovascular Services</p>
                  </div>
                  <div className="text-right">
                    <span className="px-3 py-1 bg-conflixis-gold text-white rounded-full text-sm font-bold">
                      Management Plan Required
                    </span>
                  </div>
                </div>
                
                <div className="grid md:grid-cols-2 gap-6">
                  <div>
                    <h4 className="font-bold text-sm text-gray-700 mb-2">Disclosed Interests</h4>
                    <dl className="space-y-2">
                      <div className="flex">
                        <dt className="text-sm text-gray-600 w-32">Financial Interest:</dt>
                        <dd className="text-sm font-bold">2.5% Ownership - CardioTech Solutions</dd>
                      </div>
                      <div className="flex">
                        <dt className="text-sm text-gray-600 w-32">Governance Role:</dt>
                        <dd className="text-sm font-bold">Board Member - Regional Heart Foundation</dd>
                      </div>
                      <div className="flex">
                        <dt className="text-sm text-gray-600 w-32">Compensation:</dt>
                        <dd className="text-sm font-bold">$45,000 annually</dd>
                      </div>
                      <div className="flex">
                        <dt className="text-sm text-gray-600 w-32">Service Period:</dt>
                        <dd className="text-sm font-bold">01/2024 - Present</dd>
                      </div>
                      <div className="flex">
                        <dt className="text-sm text-gray-600 w-32">Family Member:</dt>
                        <dd className="text-sm">
                          <span className="bg-yellow-100 text-yellow-800 px-2 py-1 rounded text-xs">
                            Spouse - Vendor Relationship
                          </span>
                        </dd>
                      </div>
                    </dl>
                  </div>
                  
                  <div>
                    <h4 className="font-bold text-sm text-gray-700 mb-2">Policy Compliance Assessment</h4>
                    <ul className="space-y-2">
                      <li className="flex items-center">
                        <span className="text-green-500 mr-2">✓</span>
                        <span className="text-sm">Annual disclosure submitted on time</span>
                      </li>
                      <li className="flex items-center">
                        <span className="text-red-500 mr-2">⚠</span>
                        <span className="text-sm">Financial interest exceeds 1% threshold</span>
                      </li>
                      <li className="flex items-center">
                        <span className="text-green-500 mr-2">✓</span>
                        <span className="text-sm">Governance relationship disclosed</span>
                      </li>
                      <li className="flex items-center">
                        <span className="text-yellow-500 mr-2">⚠</span>
                        <span className="text-sm">Family member vendor relationship</span>
                      </li>
                      <li className="flex items-center">
                        <span className="text-green-500 mr-2">✓</span>
                        <span className="text-sm">Not an insurmountable conflict</span>
                      </li>
                    </ul>
                  </div>
                </div>
                
                <div className="mt-6 pt-6 border-t">
                  <h4 className="font-bold text-sm text-gray-700 mb-2">Management Plan Requirements</h4>
                  <ol className="list-decimal list-inside space-y-1 text-sm text-gray-600">
                    <li>Recusal from all purchasing decisions related to cardiovascular equipment and supplies</li>
                    <li>Cannot participate in vendor selection committees for cardiology services</li>
                    <li>Disclosure required in all research publications and presentations</li>
                    <li>Quarterly reporting of any changes in ownership percentage or compensation</li>
                    <li>Annual review by Audit and Compliance Committee</li>
                  </ol>
                </div>
                
                <div className="mt-6 flex gap-2">
                  <button className="px-4 py-2 bg-conflixis-green text-white rounded-lg hover:bg-opacity-90">
                    Download Documentation
                  </button>
                  <button className="px-4 py-2 bg-conflixis-blue text-white rounded-lg hover:bg-opacity-90">
                    Send for Committee Review
                  </button>
                  <button className="px-4 py-2 bg-conflixis-gold text-white rounded-lg hover:bg-opacity-90">
                    Approve Management Plan
                  </button>
                </div>
              </div>
            </div>
          </div>
        </section>

        {/* Footer */}
        <footer className="bg-white border-t border-gray-200">
          <div className="container mx-auto px-6 py-4">
            <div className="text-center text-sm text-gray-600">
              <p>Texas Health Resources COI Policy Compliance System</p>
              <p>Powered by Conflixis Analytics Platform</p>
            </div>
          </div>
        </footer>
      </div>
    </MainLayout>
  );
}