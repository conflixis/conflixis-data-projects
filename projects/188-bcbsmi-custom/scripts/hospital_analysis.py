#!/usr/bin/env python3
"""
BCBSMI Hospital Analysis Script
Analyzes pharmaceutical payments and prescribing patterns at Michigan hospitals
"""

import pandas as pd
import numpy as np
from google.cloud import bigquery
from pathlib import Path
import json
import os
from datetime import datetime
from typing import Dict, List, Tuple

class HospitalAnalyzer:
    """Analyze hospital-level pharmaceutical influence patterns"""
    
    def __init__(self):
        """Initialize BigQuery client and configuration"""
        self.project_id = "data-analytics-389803"
        self.dataset = "conflixis_data_projects"
        self.results = {}
        self.client = self._setup_bigquery_client()
        
    def _setup_bigquery_client(self):
        """Setup BigQuery client with credentials"""
        env_file = Path("/home/incent/conflixis-data-projects/.env")
        if env_file.exists():
            with open(env_file, 'r') as f:
                content = f.read()
                lines = content.split('\n')
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        if key == 'GCP_SERVICE_ACCOUNT_KEY':
                            if value.startswith("'") and value.endswith("'"):
                                value = value[1:-1]
                        else:
                            value = value.strip().strip("'\"")
                        os.environ[key] = value
        
        from google.oauth2 import service_account
        service_account_json = os.getenv('GCP_SERVICE_ACCOUNT_KEY')
        service_account_json = service_account_json.replace('\\\\n', '\\n')
        service_account_info = json.loads(service_account_json)
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        return bigquery.Client(credentials=credentials, project=self.project_id)
    
    def load_bcbsmi_npis(self) -> pd.DataFrame:
        """Load BCBSMI provider NPIs"""
        npi_file = Path(__file__).parent.parent.parent / "182-healthcare-coi-analytics-report-template" / "data" / "inputs" / "bcbsmi-npis.csv"
        df = pd.read_csv(npi_file)
        df.columns = df.columns.str.upper()
        df['NPI'] = df['NPI'].astype(str)
        print(f"Loaded {len(df):,} BCBSMI provider NPIs")
        return df
    
    def get_michigan_hospitals(self) -> pd.DataFrame:
        """Get all Michigan hospitals with affiliated providers"""
        query = """
        WITH bcbsmi_npis AS (
            SELECT DISTINCT CAST(NPI AS INT64) as NPI 
            FROM `data-analytics-389803.temp.bcbsmi_provider_npis`
        ),
        hospital_affiliations AS (
            SELECT 
                f.AFFILIATED_NAME as Facility_Name,
                f.AFFILIATED_HQ_CITY as City,
                f.AFFILIATED_HQ_STATE as State,
                f.NPI,
                CASE 
                    WHEN b.NPI IS NOT NULL THEN 1 
                    ELSE 0 
                END as is_bcbsmi_provider
            FROM `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized` f
            LEFT JOIN bcbsmi_npis b ON f.NPI = b.NPI
            WHERE f.AFFILIATED_HQ_STATE = 'MI'
        )
        SELECT 
            Facility_Name,
            City,
            COUNT(DISTINCT NPI) as total_providers,
            SUM(is_bcbsmi_provider) as bcbsmi_providers,
            ROUND(SUM(is_bcbsmi_provider) * 100.0 / COUNT(DISTINCT NPI), 2) as bcbsmi_coverage_pct
        FROM hospital_affiliations
        GROUP BY Facility_Name, City
        HAVING total_providers >= 10  -- Focus on hospitals with meaningful provider counts
        ORDER BY bcbsmi_providers DESC
        """
        
        print("Fetching Michigan hospital data...")
        df = self.client.query(query).to_dataframe()
        print(f"Found {len(df):,} Michigan hospitals with 10+ providers")
        self.results['hospitals'] = df
        return df
    
    def analyze_hospital_payments(self) -> pd.DataFrame:
        """Analyze pharmaceutical payments by hospital"""
        query = """
        WITH bcbsmi_npis AS (
            SELECT DISTINCT CAST(NPI AS INT64) as NPI 
            FROM `data-analytics-389803.temp.bcbsmi_provider_npis`
        ),
        -- Get PRIMARY facility for each provider (using rank by affiliation)
        primary_facilities AS (
            SELECT 
                f.NPI,
                f.AFFILIATED_NAME as Facility_Name,
                f.AFFILIATED_HQ_CITY as City,
                ROW_NUMBER() OVER (PARTITION BY f.NPI ORDER BY f.AFFILIATED_NAME) as facility_rank
            FROM `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized` f
            INNER JOIN bcbsmi_npis b ON f.NPI = b.NPI
            WHERE f.AFFILIATED_HQ_STATE = 'MI'
        ),
        -- Get payments for BCBSMI providers  
        provider_payments AS (
            SELECT 
                b.NPI,
                SUM(op.total_amount_of_payment_usdollars) as total_provider_payments,
                COUNT(DISTINCT op.record_id) as payment_count,
                COUNT(DISTINCT op.applicable_manufacturer_or_applicable_gpo_making_payment_name) as unique_manufacturers
            FROM bcbsmi_npis b
            LEFT JOIN `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized` op
                ON b.NPI = op.covered_recipient_npi
                AND op.program_year BETWEEN 2020 AND 2024
            GROUP BY b.NPI
        ),
        -- Join payments with PRIMARY facility only
        hospital_payments AS (
            SELECT 
                pf.Facility_Name,
                pf.City,
                pf.NPI,
                p.total_provider_payments as provider_payments,
                p.payment_count,
                p.unique_manufacturers
            FROM primary_facilities pf
            INNER JOIN provider_payments p ON pf.NPI = p.NPI
            WHERE pf.facility_rank = 1  -- PRIMARY facility only
        )
        SELECT 
            Facility_Name,
            City,
            COUNT(DISTINCT NPI) as providers_analyzed,
            COUNT(DISTINCT CASE WHEN provider_payments > 0 THEN NPI END) as providers_with_payments,
            ROUND(COUNT(DISTINCT CASE WHEN provider_payments > 0 THEN NPI END) * 100.0 / 
                  COUNT(DISTINCT NPI), 2) as payment_penetration_pct,
            SUM(provider_payments) as total_payments,
            AVG(CASE WHEN provider_payments > 0 THEN provider_payments END) as avg_payment_per_provider,
            MAX(provider_payments) as max_provider_payment,
            SUM(payment_count) as total_transactions,
            AVG(unique_manufacturers) as avg_manufacturers_per_provider
        FROM hospital_payments
        GROUP BY Facility_Name, City
        HAVING COUNT(DISTINCT NPI) >= 10
        ORDER BY total_payments DESC
        """
        
        print("Analyzing hospital payment patterns...")
        df = self.client.query(query).to_dataframe()
        print(f"Analyzed payments for {len(df):,} hospitals")
        self.results['hospital_payments'] = df
        return df
    
    def analyze_prescription_patterns(self) -> pd.DataFrame:
        """Analyze prescription patterns by hospital"""
        query = """
        WITH bcbsmi_npis AS (
            SELECT DISTINCT CAST(NPI AS INT64) as NPI 
            FROM `data-analytics-389803.temp.bcbsmi_provider_npis`
        ),
        -- Get PRIMARY facility for each provider
        primary_facilities AS (
            SELECT 
                f.NPI,
                f.AFFILIATED_NAME as Facility_Name,
                f.AFFILIATED_HQ_CITY as City,
                ROW_NUMBER() OVER (PARTITION BY f.NPI ORDER BY f.AFFILIATED_NAME) as facility_rank
            FROM `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized` f
            INNER JOIN bcbsmi_npis b ON f.NPI = b.NPI
            WHERE f.AFFILIATED_HQ_STATE = 'MI'
        ),
        -- Get prescriptions for providers at their PRIMARY facility
        hospital_prescriptions AS (
            SELECT 
                pf.Facility_Name,
                pf.City,
                rx.BRAND_NAME,
                SUM(rx.PAYMENTS) as total_cost,
                SUM(rx.PRESCRIPTIONS) as total_prescriptions,
                COUNT(DISTINCT pf.NPI) as prescribing_providers
            FROM primary_facilities pf
            INNER JOIN `data-analytics-389803.conflixis_data_projects.PHYSICIAN_RX_2020_2024_optimized` rx
                ON pf.NPI = rx.NPI
            WHERE pf.facility_rank = 1  -- PRIMARY facility only
                AND rx.CLAIM_YEAR BETWEEN 2020 AND 2024
                AND rx.BRAND_NAME IN ('ELIQUIS', 'HUMIRA', 'OZEMPIC', 'TRULICITY', 
                                      'JARDIANCE', 'XARELTO', 'ENBREL', 'STELARA')
            GROUP BY pf.Facility_Name, pf.City, rx.BRAND_NAME
        )
        SELECT 
            Facility_Name,
            City,
            SUM(total_cost) as total_rx_cost,
            SUM(total_prescriptions) as total_rx_count,
            COUNT(DISTINCT BRAND_NAME) as unique_drugs,
            STRING_AGG(BRAND_NAME || ': $' || CAST(ROUND(total_cost/1000000, 1) AS STRING) || 'M', 
                      ', ' ORDER BY total_cost DESC LIMIT 3) as top_drugs,
            MAX(prescribing_providers) as max_drug_prescribers
        FROM hospital_prescriptions
        GROUP BY Facility_Name, City
        HAVING SUM(total_cost) > 1000000  -- Focus on hospitals with significant prescription volume
        ORDER BY total_rx_cost DESC
        """
        
        print("Analyzing prescription patterns by hospital...")
        df = self.client.query(query).to_dataframe()
        print(f"Analyzed prescriptions for {len(df):,} hospitals")
        self.results['hospital_prescriptions'] = df
        return df
    
    def analyze_city_patterns(self) -> pd.DataFrame:
        """Analyze pharmaceutical influence patterns by city"""
        query = """
        WITH bcbsmi_npis AS (
            SELECT DISTINCT CAST(NPI AS INT64) as NPI 
            FROM `data-analytics-389803.temp.bcbsmi_provider_npis`
        ),
        -- Get PRIMARY facility for each provider
        primary_facilities AS (
            SELECT 
                f.NPI,
                f.AFFILIATED_NAME as Facility_Name,
                f.AFFILIATED_HQ_CITY as City,
                ROW_NUMBER() OVER (PARTITION BY f.NPI ORDER BY f.AFFILIATED_NAME) as facility_rank
            FROM `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized` f
            INNER JOIN bcbsmi_npis b ON f.NPI = b.NPI
            WHERE f.AFFILIATED_HQ_STATE = 'MI'
        ),
        -- Get provider payments
        provider_payments AS (
            SELECT 
                b.NPI,
                SUM(op.total_amount_of_payment_usdollars) as total_payments,
                COUNT(DISTINCT op.record_id) as transactions
            FROM bcbsmi_npis b
            LEFT JOIN `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized` op
                ON b.NPI = op.covered_recipient_npi
                AND op.program_year BETWEEN 2020 AND 2024
            GROUP BY b.NPI
        ),
        -- Get provider prescriptions
        provider_rx AS (
            SELECT 
                b.NPI,
                SUM(rx.PAYMENTS) as total_rx_cost,
                SUM(rx.PRESCRIPTIONS) as total_rx_count
            FROM bcbsmi_npis b
            INNER JOIN `data-analytics-389803.conflixis_data_projects.PHYSICIAN_RX_2020_2024_optimized` rx
                ON b.NPI = rx.NPI
            WHERE rx.CLAIM_YEAR BETWEEN 2020 AND 2024
                AND rx.BRAND_NAME IN ('ELIQUIS', 'HUMIRA', 'OZEMPIC', 'TRULICITY', 
                                      'JARDIANCE', 'XARELTO', 'ENBREL', 'STELARA')
            GROUP BY b.NPI
        ),
        -- Aggregate by city using PRIMARY facilities only
        city_aggregates AS (
            SELECT 
                pf.City,
                COUNT(DISTINCT pf.NPI) as bcbsmi_providers,
                COUNT(DISTINCT CASE WHEN pp.total_payments > 0 THEN pf.NPI END) as providers_with_payments,
                SUM(pp.total_payments) as total_payments,
                SUM(pp.transactions) as total_transactions,
                SUM(pr.total_rx_cost) as total_rx_cost,
                SUM(pr.total_rx_count) as total_rx_count,
                COUNT(DISTINCT pf.Facility_Name) as hospital_count
            FROM primary_facilities pf
            LEFT JOIN provider_payments pp ON pf.NPI = pp.NPI
            LEFT JOIN provider_rx pr ON pf.NPI = pr.NPI
            WHERE pf.facility_rank = 1  -- PRIMARY facility only
            GROUP BY pf.City
        ),
        -- Get total providers in each city (not just BCBSMI)
        total_providers AS (
            SELECT 
                AFFILIATED_HQ_CITY as City,
                COUNT(DISTINCT NPI) as total_providers
            FROM `data-analytics-389803.conflixis_data_projects.PHYSICIANS_FACILITY_AFFILIATIONS_CURRENT_optimized`
            WHERE AFFILIATED_HQ_STATE = 'MI'
            GROUP BY AFFILIATED_HQ_CITY
        )
        SELECT 
            ca.City,
            tp.total_providers,
            ca.bcbsmi_providers,
            ca.providers_with_payments,
            ROUND(ca.providers_with_payments * 100.0 / NULLIF(ca.bcbsmi_providers, 0), 2) as payment_penetration_pct,
            ca.total_payments,
            ROUND(ca.total_payments / NULLIF(ca.providers_with_payments, 0), 2) as avg_payment,
            ca.total_transactions,
            COALESCE(ca.total_rx_cost, 0) as total_rx_cost,
            COALESCE(ca.total_rx_count, 0) as total_rx_count,
            ca.hospital_count
        FROM city_aggregates ca
        LEFT JOIN total_providers tp ON ca.City = tp.City
        WHERE ca.bcbsmi_providers >= 10
        ORDER BY ca.total_payments DESC
        """
        
        print("Analyzing city-level patterns...")
        df = self.client.query(query).to_dataframe()
        print(f"Analyzed {len(df):,} Michigan cities")
        self.results['city_analysis'] = df
        return df
    
    def classify_rural_urban(self) -> pd.DataFrame:
        """Classify cities as rural or urban and compare patterns"""
        # Major Michigan cities - population > 50,000
        major_cities = [
            'Detroit', 'Grand Rapids', 'Warren', 'Sterling Heights', 'Lansing',
            'Ann Arbor', 'Flint', 'Dearborn', 'Livonia', 'Troy', 'Westland',
            'Farmington Hills', 'Kalamazoo', 'Wyoming', 'Southfield', 'Rochester Hills',
            'Taylor', 'St. Clair Shores', 'Pontiac', 'Royal Oak', 'Novi', 'Battle Creek',
            'Dearborn Heights', 'East Lansing', 'Roseville', 'Kentwood', 'Saginaw',
            'Portage', 'Forest Hills', 'Birmingham', 'Holland'
        ]
        
        city_df = self.results.get('city_analysis')
        if city_df is None:
            print("Need to run city analysis first")
            return pd.DataFrame()
        
        # Classify cities
        city_df['area_type'] = city_df['City'].apply(
            lambda x: 'Urban' if x in major_cities else 'Rural'
        )
        
        # Calculate rural vs urban aggregates
        rural_urban_comparison = city_df.groupby('area_type').agg({
            'total_providers': 'sum',
            'bcbsmi_providers': 'sum',
            'providers_with_payments': 'sum',
            'total_payments': 'sum',
            'total_transactions': 'sum',
            'total_rx_cost': 'sum',
            'total_rx_count': 'sum',
            'hospital_count': 'sum',
            'City': 'count'
        }).rename(columns={'City': 'city_count'})
        
        # Calculate derived metrics
        rural_urban_comparison['avg_payment_per_provider'] = (
            rural_urban_comparison['total_payments'] / 
            rural_urban_comparison['providers_with_payments']
        )
        rural_urban_comparison['payment_penetration_pct'] = (
            rural_urban_comparison['providers_with_payments'] * 100.0 / 
            rural_urban_comparison['total_providers']
        )
        rural_urban_comparison['avg_rx_cost_per_provider'] = (
            rural_urban_comparison['total_rx_cost'] / 
            rural_urban_comparison['bcbsmi_providers']
        )
        
        self.results['rural_urban_comparison'] = rural_urban_comparison
        self.results['city_classification'] = city_df
        return rural_urban_comparison
    
    def calculate_risk_scores(self) -> pd.DataFrame:
        """Calculate composite risk scores for hospitals"""
        # Merge payment and prescription data
        payments = self.results.get('hospital_payments')
        prescriptions = self.results.get('hospital_prescriptions')
        hospitals = self.results.get('hospitals')
        
        if payments is None or prescriptions is None or hospitals is None:
            print("Need to run analysis first")
            return pd.DataFrame()
        
        # Merge datasets
        risk_df = hospitals.merge(
            payments, 
            on=['Facility_Name', 'City'], 
            how='left'
        ).merge(
            prescriptions,
            on=['Facility_Name', 'City'],
            how='left'
        )
        
        # Fill NaN values and convert Decimal to float
        risk_df = risk_df.fillna(0)
        
        # Convert Decimal columns to float
        for col in risk_df.columns:
            if risk_df[col].dtype == 'object':
                try:
                    risk_df[col] = pd.to_numeric(risk_df[col], errors='ignore')
                except:
                    pass
        
        # Calculate risk components (normalize to 0-100 scale)
        risk_df['payment_intensity_score'] = (
            pd.to_numeric(risk_df['avg_payment_per_provider'], errors='coerce').fillna(0) / 
            pd.to_numeric(risk_df['avg_payment_per_provider'], errors='coerce').fillna(0).max() * 30
        )
        
        risk_df['payment_penetration_score'] = (
            pd.to_numeric(risk_df['payment_penetration_pct'], errors='coerce').fillna(0) / 100 * 30
        )
        
        risk_df['prescription_volume_score'] = (
            pd.to_numeric(risk_df['total_rx_cost'], errors='coerce').fillna(0) / 
            pd.to_numeric(risk_df['total_rx_cost'], errors='coerce').fillna(0).max() * 20
        )
        
        risk_df['provider_concentration_score'] = (
            pd.to_numeric(risk_df['bcbsmi_coverage_pct'], errors='coerce').fillna(0) / 100 * 20
        )
        
        # Calculate composite risk score
        risk_df['composite_risk_score'] = (
            risk_df['payment_intensity_score'] +
            risk_df['payment_penetration_score'] +
            risk_df['prescription_volume_score'] +
            risk_df['provider_concentration_score']
        )
        
        # Rank hospitals by risk
        risk_df['risk_rank'] = risk_df['composite_risk_score'].rank(ascending=False, method='dense')
        
        # Categorize risk levels
        risk_df['risk_category'] = pd.cut(
            risk_df['composite_risk_score'],
            bins=[0, 30, 60, 80, 100],
            labels=['Low', 'Medium', 'High', 'Critical']
        )
        
        # Sort by risk score
        risk_df = risk_df.sort_values('composite_risk_score', ascending=False)
        
        self.results['risk_scores'] = risk_df
        return risk_df
    
    def generate_summary_statistics(self) -> Dict:
        """Generate summary statistics for the analysis"""
        risk_df = self.results.get('risk_scores')
        if risk_df is None:
            return {}
        
        summary = {
            'total_hospitals_analyzed': len(risk_df),
            'total_bcbsmi_providers': risk_df['bcbsmi_providers'].sum(),
            'total_payments': risk_df['total_payments'].sum(),
            'total_prescription_cost': risk_df['total_rx_cost'].sum(),
            'high_risk_hospitals': len(risk_df[risk_df['risk_category'] == 'Critical']),
            'medium_risk_hospitals': len(risk_df[risk_df['risk_category'] == 'High']),
            'avg_payment_penetration': risk_df['payment_penetration_pct'].mean(),
            'top_5_hospitals_by_payments': risk_df.nlargest(5, 'total_payments')[
                ['Facility_Name', 'City', 'total_payments']
            ].to_dict('records'),
            'top_5_hospitals_by_risk': risk_df.nlargest(5, 'composite_risk_score')[
                ['Facility_Name', 'City', 'composite_risk_score', 'risk_category']
            ].to_dict('records')
        }
        
        self.results['summary'] = summary
        return summary
    
    def save_results(self):
        """Save analysis results to files"""
        output_dir = Path(__file__).parent.parent / "data"
        output_dir.mkdir(exist_ok=True)
        
        # Save risk scores
        if 'risk_scores' in self.results:
            risk_file = output_dir / f"hospital_risk_scores_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            self.results['risk_scores'].to_csv(risk_file, index=False)
            print(f"Saved risk scores to: {risk_file}")
        
        # Save summary
        if 'summary' in self.results:
            summary_file = output_dir / f"analysis_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(summary_file, 'w') as f:
                json.dump(self.results['summary'], f, indent=2, default=str)
            print(f"Saved summary to: {summary_file}")
    
    def run_full_analysis(self):
        """Run complete hospital analysis pipeline"""
        print("=" * 80)
        print("BCBSMI HOSPITAL ANALYSIS - ENHANCED WITH CITY & RURAL/URBAN INSIGHTS")
        print("=" * 80)
        
        # Load NPIs
        npis = self.load_bcbsmi_npis()
        
        # Run analyses
        self.get_michigan_hospitals()
        self.analyze_hospital_payments()
        self.analyze_prescription_patterns()
        
        # NEW: City-level analysis
        self.analyze_city_patterns()
        self.classify_rural_urban()
        
        # Calculate risk scores
        risk_df = self.calculate_risk_scores()
        
        # Generate summary
        summary = self.generate_summary_statistics()
        
        # Add city and rural/urban summary
        if 'city_analysis' in self.results:
            city_df = self.results['city_analysis']
            # Convert to numeric for nlargest operation
            city_df['total_payments'] = pd.to_numeric(city_df['total_payments'], errors='coerce').fillna(0)
            summary['total_cities_analyzed'] = len(city_df)
            summary['top_5_cities_by_payments'] = city_df.nlargest(5, 'total_payments')[
                ['City', 'total_payments', 'bcbsmi_providers', 'payment_penetration_pct']
            ].to_dict('records')
        
        if 'rural_urban_comparison' in self.results:
            summary['rural_urban_comparison'] = self.results['rural_urban_comparison'].to_dict()
        
        self.results['summary'] = summary
        
        # Save results
        self.save_results()
        
        # Save additional city data
        if 'city_classification' in self.results:
            output_dir = Path(__file__).parent.parent / "data"
            city_file = output_dir / f"city_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            self.results['city_classification'].to_csv(city_file, index=False)
            print(f"Saved city analysis to: {city_file}")
        
        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE")
        print("=" * 80)
        
        # Print key findings
        print(f"\n📊 Key Findings:")
        print(f"  Hospitals analyzed: {summary['total_hospitals_analyzed']}")
        print(f"  Cities analyzed: {summary.get('total_cities_analyzed', 0)}")
        print(f"  Total BCBSMI providers: {summary['total_bcbsmi_providers']:,}")
        print(f"  Total payments: ${summary['total_payments']:,.0f}")
        print(f"  Total prescription cost: ${summary['total_prescription_cost']:,.0f}")
        print(f"  High-risk hospitals: {summary['high_risk_hospitals']}")
        
        print(f"\n🏥 Top 5 Hospitals by Risk Score:")
        for hosp in summary['top_5_hospitals_by_risk']:
            print(f"  - {hosp['Facility_Name']}, {hosp['City']}: Score {hosp['composite_risk_score']:.1f} ({hosp['risk_category']})")
        
        if 'top_5_cities_by_payments' in summary:
            print(f"\n🏙️ Top 5 Cities by Pharmaceutical Payments:")
            for city in summary['top_5_cities_by_payments']:
                print(f"  - {city['City']}: ${city['total_payments']:,.0f} ({city['payment_penetration_pct']:.1f}% penetration)")
        
        if 'rural_urban_comparison' in self.results:
            rural_urban = self.results['rural_urban_comparison']
            print(f"\n🌾 Rural vs Urban Comparison:")
            for area_type in rural_urban.index:
                print(f"  {area_type}:")
                print(f"    - Cities: {int(rural_urban.loc[area_type, 'city_count'])}")
                print(f"    - Total payments: ${rural_urban.loc[area_type, 'total_payments']:,.0f}")
                print(f"    - Payment penetration: {rural_urban.loc[area_type, 'payment_penetration_pct']:.1f}%")
                print(f"    - Avg payment per provider: ${rural_urban.loc[area_type, 'avg_payment_per_provider']:,.0f}")
        
        return self.results


def main():
    """Main execution"""
    analyzer = HospitalAnalyzer()
    results = analyzer.run_full_analysis()
    return results


if __name__ == "__main__":
    main()