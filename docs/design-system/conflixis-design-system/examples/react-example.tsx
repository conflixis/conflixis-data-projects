import React from 'react';

// Example React component using Conflixis Design System
// This assumes you have Tailwind CSS configured with Conflixis colors

const ConflixisExample: React.FC = () => {
  const [activeTab, setActiveTab] = React.useState('overview');

  return (
    <div className="min-h-screen bg-conflixis-ivory">
      {/* Header */}
      <header className="bg-conflixis-green text-white shadow-lg">
        <div className="container mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <img 
                src="/assets/logos/conflixis-logo.png" 
                alt="Conflixis" 
                className="h-10 w-10"
              />
              <span className="text-xl font-soehneKraftig">Conflixis Dashboard</span>
            </div>
            <nav className="flex space-x-6">
              <a href="#" className="hover:text-conflixis-gold transition-colors">Dashboard</a>
              <a href="#" className="hover:text-conflixis-gold transition-colors">Analytics</a>
              <a href="#" className="hover:text-conflixis-gold transition-colors">Reports</a>
              <a href="#" className="hover:text-conflixis-gold transition-colors">Settings</a>
            </nav>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-6 py-8">
        {/* Page Title */}
        <div className="mb-8 animate-fade-in">
          <h1 className="text-4xl font-ivarDisplay font-bold text-conflixis-green mb-2">
            Healthcare Analytics Dashboard
          </h1>
          <p className="text-gray-600">
            Monitor key metrics and performance indicators
          </p>
        </div>

        {/* KPI Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          <KPICard 
            title="Total Providers" 
            value="2,847" 
            change="+12%" 
            color="green"
          />
          <KPICard 
            title="Compliance Rate" 
            value="94.2%" 
            change="+3.1%" 
            color="blue"
          />
          <KPICard 
            title="Risk Score" 
            value="Low" 
            change="Stable" 
            color="gold"
          />
          <KPICard 
            title="Active Reviews" 
            value="142" 
            change="-5" 
            color="red"
          />
        </div>

        {/* Tabs Section */}
        <div className="bg-white rounded-lg shadow-sm">
          <div className="border-b border-gray-200">
            <nav className="flex space-x-8 px-6" aria-label="Tabs">
              {['overview', 'details', 'analytics', 'reports'].map((tab) => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  className={`
                    py-4 px-1 border-b-2 font-soehneKraftig text-sm capitalize
                    ${activeTab === tab 
                      ? 'border-conflixis-green text-conflixis-green' 
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                    }
                  `}
                >
                  {tab}
                </button>
              ))}
            </nav>
          </div>
          
          <div className="p-6">
            <div className="animate-fade-in">
              {activeTab === 'overview' && <OverviewContent />}
              {activeTab === 'details' && <DetailsContent />}
              {activeTab === 'analytics' && <AnalyticsContent />}
              {activeTab === 'reports' && <ReportsContent />}
            </div>
          </div>
        </div>

        {/* Action Buttons */}
        <div className="mt-8 flex justify-end space-x-4">
          <button className="px-6 py-2 border-2 border-conflixis-green text-conflixis-green rounded-lg font-soehneKraftig hover:bg-conflixis-green hover:text-white transition-all">
            Export Report
          </button>
          <button className="px-6 py-2 bg-conflixis-green text-white rounded-lg font-soehneKraftig hover:bg-opacity-90 transition-all shadow-sm hover:shadow-md">
            Generate Analysis
          </button>
        </div>
      </main>
    </div>
  );
};

// KPI Card Component
const KPICard: React.FC<{
  title: string;
  value: string;
  change: string;
  color: 'green' | 'blue' | 'gold' | 'red';
}> = ({ title, value, change, color }) => {
  const colorClasses = {
    green: 'bg-conflixis-green',
    blue: 'bg-conflixis-blue',
    gold: 'bg-conflixis-gold',
    red: 'bg-conflixis-red',
  };

  return (
    <div className="bg-white rounded-lg p-6 shadow-sm hover:shadow-md transition-shadow">
      <div className="flex items-center justify-between mb-4">
        <div className={`w-12 h-12 ${colorClasses[color]} rounded-lg opacity-20`}></div>
        <span className="text-sm text-gray-500">{change}</span>
      </div>
      <h3 className="text-sm font-soehneKraftig text-gray-500 mb-1">{title}</h3>
      <p className="text-2xl font-soehneKraftig text-conflixis-green">{value}</p>
    </div>
  );
};

// Content Components
const OverviewContent = () => (
  <div>
    <h2 className="text-2xl font-ivarDisplay mb-4">Overview</h2>
    <p className="text-gray-600 mb-4">
      This dashboard provides a comprehensive view of healthcare provider compliance 
      and disclosure analysis across your organization.
    </p>
    <div className="grid grid-cols-2 gap-6">
      <div className="bg-conflixis-ivory rounded-lg p-4">
        <h3 className="font-soehneKraftig text-conflixis-green mb-2">Recent Activity</h3>
        <ul className="space-y-2 text-sm">
          <li className="flex justify-between">
            <span>New disclosures reviewed</span>
            <span className="font-soehneKraftig">24</span>
          </li>
          <li className="flex justify-between">
            <span>Policies updated</span>
            <span className="font-soehneKraftig">3</span>
          </li>
          <li className="flex justify-between">
            <span>Risk assessments completed</span>
            <span className="font-soehneKraftig">18</span>
          </li>
        </ul>
      </div>
      <div className="bg-conflixis-ivory rounded-lg p-4">
        <h3 className="font-soehneKraftig text-conflixis-green mb-2">Upcoming Tasks</h3>
        <ul className="space-y-2 text-sm">
          <li className="flex items-center space-x-2">
            <div className="w-2 h-2 bg-conflixis-gold rounded-full"></div>
            <span>Quarterly compliance review</span>
          </li>
          <li className="flex items-center space-x-2">
            <div className="w-2 h-2 bg-conflixis-blue rounded-full"></div>
            <span>Policy training session</span>
          </li>
          <li className="flex items-center space-x-2">
            <div className="w-2 h-2 bg-conflixis-green rounded-full"></div>
            <span>Annual audit preparation</span>
          </li>
        </ul>
      </div>
    </div>
  </div>
);

const DetailsContent = () => (
  <div>
    <h2 className="text-2xl font-ivarDisplay mb-4">Details</h2>
    <p className="text-gray-600">Detailed information will be displayed here.</p>
  </div>
);

const AnalyticsContent = () => (
  <div>
    <h2 className="text-2xl font-ivarDisplay mb-4">Analytics</h2>
    <p className="text-gray-600">Analytics and charts will be displayed here.</p>
  </div>
);

const ReportsContent = () => (
  <div>
    <h2 className="text-2xl font-ivarDisplay mb-4">Reports</h2>
    <p className="text-gray-600">Generated reports will be displayed here.</p>
  </div>
);

export default ConflixisExample;