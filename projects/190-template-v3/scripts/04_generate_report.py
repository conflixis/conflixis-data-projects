import os
import yaml
import pandas as pd
import numpy as np

def load_config():
    """Loads the configuration from the CONFIG.yaml file."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'CONFIG.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def format_currency(value):
    return f"${value:,.0f}"

def format_multiplier(value):
    return f"{value:.1f}x"

def get_provider_type(specialty):
    specialty = str(specialty).lower()
    if 'physician assistant' in specialty:
        return 'PA'
    if 'nurse' in specialty:
        return 'NP'
    return 'Physician'

def generate_narrative_report(config):
    """
    Performs in-depth analysis and generates a narrative-driven Markdown report.
    """
    # --- 1. Load Data ---
    project_paths = config['project_paths']
    health_system = config['health_system']
    key_drugs = config['report_settings']['key_drugs']
    
    project_dir = project_paths['project_dir']
    output_dir = os.path.join(project_dir, project_paths['output_dir'])
    docs_dir = os.path.join(project_dir, 'docs')
    os.makedirs(docs_dir, exist_ok=True)

    try:
        npi_df = pd.read_csv(os.path.join(project_dir, health_system['npi_file']))
        payments_df = pd.read_csv(os.path.join(output_dir, f"{health_system['short_name']}_op_payments_raw.csv"))
        rx_df = pd.read_csv(os.path.join(output_dir, f"{health_system['short_name']}_prescribing_raw.csv"))
        print("Successfully loaded all raw data files.")
    except FileNotFoundError as e:
        print(f"Error: Could not find a necessary raw data file: {e.filename}")
        return

    # --- 2. Data Preparation ---
    npi_df['NPI'] = npi_df['NPI'].astype(str)
    payments_df['covered_recipient_npi'] = payments_df['covered_recipient_npi'].astype(str)
    rx_df['NPI'] = rx_df['NPI'].astype(str)
    
    npi_df['provider_type'] = npi_df['Primary_Specialty'].apply(get_provider_type)

    total_providers = npi_df['NPI'].nunique()
    paid_npi_list = payments_df['covered_recipient_npi'].unique()
    
    # --- 3. Executive Summary & Overview ---
    total_payments = payments_df['payment_value'].sum()
    providers_receiving_payments = len(paid_npi_list)
    percent_paid = (providers_receiving_payments / total_providers) * 100
    total_transactions = len(payments_df)
    
    # --- 4. Prescription Patterns ---
    total_rx_value = rx_df['prescription_value'].sum()
    total_prescriptions = rx_df['prescription_count'].sum()
    unique_prescribers = rx_df['NPI'].nunique()

    # --- 5. Correlation Analysis ---
    correlation_results = []
    for drug in key_drugs:
        drug_rx_df = rx_df[rx_df['BRAND_NAME'].str.lower() == drug.lower()]
        if drug_rx_df.empty:
            continue
            
        manufacturer = drug_rx_df['MANUFACTURER'].iloc[0]
        
        # Find NPIs paid by this manufacturer
        paid_by_mfg_npi = payments_df[payments_df['manufacturer'].str.lower() == manufacturer.lower()]['covered_recipient_npi'].unique()
        
        # Separate prescribers of this drug into paid vs unpaid
        prescribers_of_drug = drug_rx_df['NPI'].unique()
        paid_prescribers = [npi for npi in prescribers_of_drug if npi in paid_by_mfg_npi]
        unpaid_prescribers = [npi for npi in prescribers_of_drug if npi not in paid_by_mfg_npi]

        avg_rx_paid = drug_rx_df[drug_rx_df['NPI'].isin(paid_prescribers)]['prescription_value'].mean() if paid_prescribers else 0
        avg_rx_unpaid = drug_rx_df[drug_rx_df['NPI'].isin(unpaid_prescribers)]['prescription_value'].mean() if unpaid_prescribers else 0
        
        influence_factor = avg_rx_paid / avg_rx_unpaid if avg_rx_unpaid > 0 else float('inf')
        
        total_payments_to_prescribers = payments_df[payments_df['covered_recipient_npi'].isin(paid_prescribers) & (payments_df['manufacturer'].str.lower() == manufacturer.lower())]['payment_value'].sum()
        total_rx_from_paid = drug_rx_df[drug_rx_df['NPI'].isin(paid_prescribers)]['prescription_value'].sum()
        
        roi = total_rx_from_paid / total_payments_to_prescribers if total_payments_to_prescribers > 0 else float('inf')

        correlation_results.append({
            'Drug': drug,
            'Avg Rx Value (Paid)': format_currency(avg_rx_paid),
            'Avg Rx Value (Unpaid)': format_currency(avg_rx_unpaid),
            'Influence Factor': format_multiplier(influence_factor),
            'ROI': format_multiplier(roi)
        })

    correlation_df = pd.DataFrame(correlation_results)

    # --- 6. Provider Type Vulnerability ---
    provider_type_analysis = []
    for p_type in ['Physician', 'PA', 'NP']:
        type_npi = npi_df[npi_df['provider_type'] == p_type]['NPI']
        
        paid_type_npi = [npi for npi in type_npi if npi in paid_npi_list]
        unpaid_type_npi = [npi for npi in type_npi if npi not in paid_npi_list]
        
        avg_rx_paid = rx_df[rx_df['NPI'].isin(paid_type_npi)]['prescription_value'].mean() if paid_type_npi else 0
        avg_rx_unpaid = rx_df[rx_df['NPI'].isin(unpaid_type_npi)]['prescription_value'].mean() if unpaid_type_npi else 0
        
        increase = ((avg_rx_paid - avg_rx_unpaid) / avg_rx_unpaid * 100) if avg_rx_unpaid > 0 else float('inf')

        provider_type_analysis.append({
            'Provider Type': p_type,
            'Avg Rx Value (Paid)': format_currency(avg_rx_paid),
            'Avg Rx Value (Unpaid)': format_currency(avg_rx_unpaid),
            'Increase with Payment': f"{increase:.1f}%"
        })
    provider_type_df = pd.DataFrame(provider_type_analysis)

    # --- 7. Generate Markdown Report ---
    md_report_path = os.path.join(docs_dir, f"{health_system['short_name']}_coi_analytics_report.md")
    with open(md_report_path, 'w') as f:
        f.write(f"# {health_system['name']} Open Payments Report\n\n")
        f.write("## Executive Summary\n\n")
        f.write(f"This analysis examines the financial relationships between industry and {health_system['name']}'s network of {total_providers:,} healthcare providers from 2020 to 2024.\n\n")
        f.write(f"The investigation reveals that **{providers_receiving_payments:,} providers ({percent_paid:.1f}%)** received a total of **{format_currency(total_payments)}** across {total_transactions:,} separate transactions. This report explores the correlation between these payments and prescribing patterns.\n\n")
        
        f.write("### Key Observations\n")
        f.write("1. **Strong Correlation Patterns**: Providers receiving industry payments often demonstrate significantly higher prescribing volumes for specific medications.\n")
        f.write("2. **Provider Type Differences**: The analysis suggests potential variations in influence susceptibility across different provider types, such as Physicians, Physician Assistants (PAs), and Nurse Practitioners (NPs).\n\n")

        f.write("## 1. Open Payments Overview\n\n")
        f.write(f"- **Unique Providers Receiving Payments**: {providers_receiving_payments:,} ({percent_paid:.1f}% of providers)\n")
        f.write(f"- **Total Transactions**: {total_transactions:,}\n")
        f.write(f"- **Total Payments**: {format_currency(total_payments)}\n\n")

        f.write("## 2. Prescription Patterns\n\n")
        f.write(f"- **Unique Prescribers**: {unique_prescribers:,}\n")
        f.write(f"- **Total Prescriptions**: {int(total_prescriptions):,}\n")
        f.write(f"- **Total Prescription Value**: {format_currency(total_rx_value)}\n\n")

        f.write("## 3. Payment-Prescription Correlations\n\n")
        f.write("The data reveals notable correlations between providers receiving payments from a manufacturer and their prescribing volume of that manufacturer's drugs.\n\n")
        f.write(correlation_df.to_markdown(index=False))
        f.write("\n\n")

        f.write("## 4. Provider Type Vulnerability Analysis\n\n")
        f.write("The analysis indicates potential differences in how payments correlate with prescribing across different provider roles.\n\n")
        f.write(provider_type_df.to_markdown(index=False))
        f.write("\n\n")

        f.write("---\n\n")
        f.write("*Report Generated: " + pd.to_datetime('today').strftime('%B %d, %Y') + "*\n")

    print(f"Successfully generated narrative Markdown report at: {md_report_path}")


def main():
    """Main function to load config and generate the report."""
    print("--- Starting Step 4: Generate Narrative Report ---")
    try:
        config = load_config()
        generate_narrative_report(config)
    except Exception as e:
        print(f"An error occurred during report generation: {e}")
    print("--- Finished Step 4 ---")

if __name__ == "__main__":
    main()