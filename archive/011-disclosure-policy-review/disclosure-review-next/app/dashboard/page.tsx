'use client';

import React, { useState, useEffect } from 'react';
import MainLayout from '@/components/layout/MainLayout';
import Modal from '@/components/ui/Modal';
import { Disclosure } from '@/lib/api/types';

export default function DashboardPage() {
  const [selectedCampaign, setSelectedCampaign] = useState('2025');
  const [selectedDisclosure, setSelectedDisclosure] = useState<Disclosure | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [statsData, setStatsData] = useState<any>(null);
  const [disclosures, setDisclosures] = useState<any[]>([]);
  
  // Fetch stats from API
  useEffect(() => {
    fetch('http://localhost:8000/api/stats/overview')
      .then(res => res.json())
      .then(data => setStatsData(data))
      .catch(err => console.error('Failed to load stats:', err));
  }, []);

  // Use mock data to match HTML version exactly
  useEffect(() => {
    // Mock data matching the HTML version
    const mockDisclosures = [
      {
        provider_name: 'Robert Anderson',
        email: 'randerson@example.com',
        job_title: 'Board Committee Candidate',
        entity_name: 'Major Supplier',
        category_label: 'MedDevice Corp',
        disclosure_type: 'Supplier Match',
        review_status: 'Disqualified'
      },
      {
        provider_name: 'Jennifer Martinez',
        email: 'jmartinez@example.com',
        job_title: 'Medical Director',
        entity_name: 'Competing Healthcare',
        category_label: 'Regional Health System',
        disclosure_type: '',
        review_status: 'Under Review'
      },
      {
        provider_name: 'Thomas Wilson',
        email: 'twilson@example.com',
        job_title: 'Board Member Candidate',
        entity_name: 'Elected Official',
        category_label: 'State Legislature',
        disclosure_type: '',
        review_status: 'Disqualified'
      }
    ];
    setDisclosures(mockDisclosures);
  }, []);

  // Use stats from API
  const stats = {
    uniqueProviders: statsData?.unique_providers || 0,
    totalDisclosures: statsData?.total_records || 0,
    compliant: 0, // API doesn't have approved count
    pendingReview: statsData?.review_status_distribution?.pending || 0,
    overdue: 261, // Sample data
    notRequired: 93, // Sample data
  };

  const totalPersons = 3847; // Sample total
  const compliancePercentage = totalPersons > 0 ? ((stats.compliant / totalPersons) * 100).toFixed(1) : '0';
  const pendingPercentage = totalPersons > 0 ? ((stats.pendingReview / totalPersons) * 100).toFixed(1) : '0';

  return (
    <MainLayout>
      <div className="min-h-screen bg-conflixis-ivory">

        {/* Summary Statistics and Compliance Overview */}
        <section className="py-6">
          <div className="container mx-auto px-6">
            <h1 className="text-3xl font-bold mb-6">COI Policy Compliance Dashboard</h1>
            
            <div className="grid lg:grid-cols-8 gap-6 mb-4">
              {/* Left Column: Campaign Selection and Metrics */}
              <div className="lg:col-span-2 flex flex-col gap-4">
                {/* Campaign Selection Card */}
                <div className="bg-white rounded-lg p-4 shadow">
                  <label className="text-sm font-soehneKraftig text-conflixis-green block mb-2">Campaign Filter</label>
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
                    <div className="text-3xl font-soehneKraftig text-conflixis-blue bg-blue-100 rounded-lg px-6 py-2 mb-2">
                      {stats.uniqueProviders}
                    </div>
                    <div className="text-xs text-blue-600 font-medium">Unique providers</div>
                  </div>
                  
                  {/* Total Disclosures Card */}
                  <div className="bg-white rounded-lg p-4 shadow flex flex-col justify-center items-center text-center">
                    <h2 className="text-sm font-bold mb-2">Total Disclosures</h2>
                    <div className="text-3xl font-soehneKraftig text-conflixis-green rounded-lg px-6 py-2 mb-2" style={{ backgroundColor: '#93baab' }}>
                      {stats.totalDisclosures}
                    </div>
                    <div className="text-xs text-conflixis-mint font-medium">Submitted for campaign</div>
                  </div>
                </div>
              </div>
              
              {/* Compliance Status Breakdown */}
              <div className="lg:col-span-3 bg-white rounded-lg shadow p-6 flex flex-col h-full">
                <h2 className="text-lg font-bold mb-4">Annual Disclosure Compliance Status</h2>
                
                <div className="flex-1 flex flex-col justify-center space-y-3">
                  {/* Compliant */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-soehneKraftig">Compliant</div>
                    <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                      <div className="h-6 rounded-full transition-all duration-1000" 
                           style={{ width: '72.3%', backgroundColor: '#008000' }}></div>
                    </div>
                    <div className="w-24 text-right text-xs">2,781 (72.3%)</div>
                  </div>
                  
                  {/* Pending Review */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-soehneKraftig">Pending Review</div>
                    <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                      <div className="bg-conflixis-gold h-6 rounded-full transition-all duration-1000" 
                           style={{ width: '18.5%' }}></div>
                    </div>
                    <div className="w-24 text-right text-xs">712 (18.5%)</div>
                  </div>
                  
                  {/* Overdue */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-soehneKraftig">Overdue</div>
                    <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                      <div className="bg-conflixis-red h-6 rounded-full transition-all duration-1000" 
                           style={{ width: '6.8%' }}></div>
                    </div>
                    <div className="w-24 text-right text-xs">261 (6.8%)</div>
                  </div>
                  
                  {/* Not Required */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-soehneKraftig">Not Required</div>
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
                
                <div className="flex-1 flex flex-col justify-center space-y-3">
                  {/* Physicians */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-soehneKraftig">Physicians</div>
                    <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                      <div className="bg-conflixis-blue h-6 rounded-full transition-all duration-1000" 
                           style={{ width: '48%' }}></div>
                    </div>
                    <div className="w-24 text-right text-xs">1,847 (48.0%)</div>
                  </div>
                  
                  {/* Leadership */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-soehneKraftig">Leadership</div>
                    <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                      <div className="bg-conflixis-green h-6 rounded-full transition-all duration-1000" 
                           style={{ width: '11%' }}></div>
                    </div>
                    <div className="w-24 text-right text-xs">423 (11.0%)</div>
                  </div>
                  
                  {/* Board Members */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-soehneKraftig">Board Members</div>
                    <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                      <div className="bg-conflixis-gold h-6 rounded-full transition-all duration-1000" 
                           style={{ width: '2.3%' }}></div>
                    </div>
                    <div className="w-24 text-right text-xs">87 (2.3%)</div>
                  </div>
                  
                  {/* Researchers */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-soehneKraftig">Researchers</div>
                    <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                      <div className="bg-purple-500 h-6 rounded-full transition-all duration-1000" 
                           style={{ width: '5.6%' }}></div>
                    </div>
                    <div className="w-24 text-right text-xs">215 (5.6%)</div>
                  </div>
                  
                  {/* Other */}
                  <div className="flex items-center gap-3">
                    <div className="w-24 text-xs font-soehneKraftig">Other</div>
                    <div className="flex-1 bg-gray-200 rounded-full h-6 relative overflow-hidden">
                      <div className="bg-gray-500 h-6 rounded-full transition-all duration-1000" 
                           style={{ width: '33.1%' }}></div>
                    </div>
                    <div className="w-24 text-right text-xs">1,275 (33.1%)</div>
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
                 className="bg-conflixis-blue hover:bg-opacity-90 text-white px-4 py-2 rounded-lg font-soehneKraftig transition-all text-sm">
                View All Disclosures →
              </a>
            </div>
            
            <div className="bg-white rounded-lg shadow overflow-hidden">
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-conflixis-tan">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-soehneKraftig text-gray-700 uppercase">Person</th>
                      <th className="px-4 py-3 text-left text-xs font-soehneKraftig text-gray-700 uppercase">Position</th>
                      <th className="px-4 py-3 text-left text-xs font-soehneKraftig text-gray-700 uppercase">THR Entity</th>
                      <th className="px-4 py-3 text-left text-xs font-soehneKraftig text-gray-700 uppercase">Disclosure Category</th>
                      <th className="px-4 py-3 text-left text-xs font-soehneKraftig text-gray-700 uppercase">Status</th>
                      <th className="px-4 py-3 text-left text-xs font-soehneKraftig text-gray-700 uppercase"></th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200">
                    {disclosures.length > 0 ? (
                      disclosures.map((disclosure, index) => (
                        <tr key={index} className="hover:bg-gray-50">
                          <td className="px-4 py-3">
                            <div className="text-sm font-soehneKraftig">{disclosure.provider_name}</div>
                            <div className="text-xs text-gray-500">{disclosure.email}</div>
                          </td>
                          <td className="px-4 py-3 text-sm">{disclosure.job_title}</td>
                          <td className="px-4 py-3">
                            <div className="text-sm font-medium text-red-600">{disclosure.entity_name}</div>
                          </td>
                          <td className="px-4 py-3">
                            <div className="text-sm">
                              <div className="font-medium">{disclosure.category_label}</div>
                              {disclosure.disclosure_type && (
                                <div className="text-xs text-gray-500">{disclosure.disclosure_type}</div>
                              )}
                            </div>
                          </td>
                          <td className="px-4 py-3">
                            <span className={`px-2 py-1 text-xs rounded-full ${
                              disclosure.review_status === 'Disqualified' 
                                ? 'bg-red-100 text-red-800' 
                                : 'bg-yellow-100 text-yellow-800'
                            }`}>
                              {disclosure.review_status}
                            </span>
                          </td>
                          <td className="px-4 py-3">
                            <button 
                              onClick={() => setSelectedDisclosure(disclosure)}
                              className="text-conflixis-blue hover:text-conflixis-green font-medium text-sm"
                            >
                              Review
                              <span className="ml-1">→</span>
                            </button>
                          </td>
                        </tr>
                      ))
                    ) : (
                      <tr>
                        <td colSpan={6} className="px-4 py-8 text-center text-gray-500">
                          Loading disclosures...
                        </td>
                      </tr>
                    )}
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
                  <h3 className="font-soehneKraftig text-sm text-gray-700 mb-3">Disclosure Timelines</h3>
                  <div className="space-y-2">
                    <div className="flex items-start">
                      <div className="w-4 h-4 bg-conflixis-green rounded-full mt-0.5 flex-shrink-0"></div>
                      <div className="ml-3">
                        <p className="text-sm font-soehneKraftig">Initial Disclosure</p>
                        <p className="text-xs text-gray-600">Promptly upon becoming subject to policy</p>
                      </div>
                    </div>
                    <div className="flex items-start">
                      <div className="w-4 h-4 bg-conflixis-gold rounded-full mt-0.5 flex-shrink-0"></div>
                      <div className="ml-3">
                        <p className="text-sm font-soehneKraftig">New Situation</p>
                        <p className="text-xs text-gray-600">Immediately upon awareness</p>
                      </div>
                    </div>
                    <div className="flex items-start">
                      <div className="w-4 h-4 bg-conflixis-blue rounded-full mt-0.5 flex-shrink-0"></div>
                      <div className="ml-3">
                        <p className="text-sm font-soehneKraftig">Annual Review</p>
                        <p className="text-xs text-gray-600">At least annually</p>
                      </div>
                    </div>
                  </div>
                </div>
                
                <div>
                  <h3 className="font-soehneKraftig text-sm text-gray-700 mb-3">Management Actions Required</h3>
                  <div className="space-y-2">
                    <div className="flex items-center justify-between p-2 bg-conflixis-ivory rounded">
                      <span className="text-sm">Pending Initial Reviews</span>
                      <span className="text-sm font-soehneKraftig text-red-600">47</span>
                    </div>
                    <div className="flex items-center justify-between p-2 bg-conflixis-ivory rounded">
                      <span className="text-sm">Management Plans Due</span>
                      <span className="text-sm font-soehneKraftig text-conflixis-gold">23</span>
                    </div>
                    <div className="flex items-center justify-between p-2 bg-conflixis-ivory rounded">
                      <span className="text-sm">Committee Reviews Scheduled</span>
                      <span className="text-sm font-soehneKraftig text-conflixis-blue">8</span>
                    </div>
                    <div className="flex items-center justify-between p-2 bg-conflixis-ivory rounded">
                      <span className="text-sm">Resolutions Completed</span>
                      <span className="text-sm font-soehneKraftig text-green-600">156</span>
                    </div>
                  </div>
                </div>
              </div>
              
              <div className="mt-6 pt-6 border-t border-gray-200">
                <h4 className="font-soehneKraftig text-sm text-gray-700 mb-3">Key Policy Thresholds</h4>
                <div className="bg-conflixis-ivory rounded-lg p-4">
                  <div className="grid md:grid-cols-3 gap-4">
                    <div>
                      <p className="text-xs text-gray-600">Financial Interest Threshold</p>
                      <p className="text-sm font-soehneKraftig text-conflixis-green">{"< 1% Public Securities"}</p>
                    </div>
                    <div>
                      <p className="text-xs text-gray-600">Review Authority</p>
                      <p className="text-sm font-soehneKraftig text-conflixis-green">Chief Compliance Officer</p>
                    </div>
                    <div>
                      <p className="text-xs text-gray-600">Approval Required</p>
                      <p className="text-sm font-soehneKraftig text-conflixis-green">Audit & Compliance Committee</p>
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
                    <h3 className="text-lg font-soehneKraftig">Dr. Michael Thompson</h3>
                    <p className="text-sm text-gray-600">Document ID: COI-2025-MT-0847</p>
                    <p className="text-sm text-gray-600">Campaign: 2025 Texas Health COI Survey - Leaders/Providers</p>
                    <p className="text-sm text-gray-600">Position: Medical Director, Cardiovascular Services</p>
                  </div>
                  <div className="text-right">
                    <span className="px-3 py-1 bg-conflixis-gold text-white rounded-full text-sm font-soehneKraftig">
                      Management Plan Required
                    </span>
                  </div>
                </div>
                
                <div className="grid md:grid-cols-2 gap-6">
                  <div>
                    <h4 className="font-soehneKraftig text-sm text-gray-700 mb-2">Disclosed Interests</h4>
                    <dl className="space-y-2">
                      <div className="flex">
                        <dt className="text-sm text-gray-600 w-32">Financial Interest:</dt>
                        <dd className="text-sm font-soehneKraftig">2.5% Ownership - CardioTech Solutions</dd>
                      </div>
                      <div className="flex">
                        <dt className="text-sm text-gray-600 w-32">Governance Role:</dt>
                        <dd className="text-sm font-soehneKraftig">Board Member - Regional Heart Foundation</dd>
                      </div>
                      <div className="flex">
                        <dt className="text-sm text-gray-600 w-32">Compensation:</dt>
                        <dd className="text-sm font-soehneKraftig">$45,000 annually</dd>
                      </div>
                      <div className="flex">
                        <dt className="text-sm text-gray-600 w-32">Service Period:</dt>
                        <dd className="text-sm font-soehneKraftig">01/2024 - Present</dd>
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
                    <h4 className="font-soehneKraftig text-sm text-gray-700 mb-2">Policy Compliance Assessment</h4>
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
                  <h4 className="font-soehneKraftig text-sm text-gray-700 mb-2">Management Plan Requirements</h4>
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