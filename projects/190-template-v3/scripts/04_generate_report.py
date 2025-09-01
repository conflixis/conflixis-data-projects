
import os
import yaml
import pandas as pd

def load_config():
    """Loads the configuration from the CONFIG.yaml file."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'CONFIG.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def generate_report(config):
    """
    Generates an Excel report from the processed analysis files.
    """
    project_paths = config['project_paths']
    health_system = config['health_system']
    
    # Define paths
    project_dir = project_paths['project_dir']
    output_dir = os.path.join(project_dir, project_paths['output_dir'])
    reports_dir = os.path.join(project_dir, project_paths['reports_dir'])
    os.makedirs(reports_dir, exist_ok=True)

    # Define input file names
    npi_file = os.path.join(project_dir, health_system['npi_file'])
    op_payments_file = os.path.join(output_dir, f"{health_system['short_name']}_op_payments_summary.csv")
    prescribing_file = os.path.join(output_dir, f"{health_system['short_name']}_prescribing_summary.csv")

    # Load dataframes
    try:
        npi_df = pd.read_csv(npi_file)
        op_df = pd.read_csv(op_payments_file)
        rx_df = pd.read_csv(prescribing_file)
        print("Successfully loaded all summary CSV files.")
    except FileNotFoundError as e:
        print(f"Error: Could not find a necessary analysis file: {e.filename}")
        print("Please ensure steps 1, 2, and 3 ran successfully.")
        return

    # Merge the dataframes
    # Ensure NPI columns are of the same type for merging
    npi_df['NPI'] = npi_df['NPI'].astype(str)
    op_df['NPI'] = op_df['NPI'].astype(str)
    rx_df['npi'] = rx_df['npi'].astype(str)
    rx_df.rename(columns={'npi': 'NPI'}, inplace=True)

    # Merge NPI list with Open Payments data
    merged_df = pd.merge(npi_df, op_df, on='NPI', how='left')

    # Merge the result with prescribing data
    final_df = pd.merge(merged_df, rx_df, on='NPI', how='left')
    
    # Fill NaN values for providers who might not have payments or prescriptions
    final_df.fillna(0, inplace=True)

    # Create a summary sheet
    summary = {
        'Total Providers': [final_df['NPI'].nunique()],
        'Providers with Payments': [final_df[final_df['total_payments'] > 0]['NPI'].nunique()],
        'Total Payments Value': [final_df['total_payments'].sum()],
        'Providers with Prescriptions': [final_df[final_df['total_prescriptions'] > 0]['NPI'].nunique()],
        'Total Prescription Value': [final_df['total_prescribing_cost'].sum()]
    }
    summary_df = pd.DataFrame(summary)

    # Create Excel report
    report_filename = f"{health_system['short_name']}_coi_analytics_report.xlsx"
    report_path = os.path.join(reports_dir, report_filename)
    
    with pd.ExcelWriter(report_path, engine='xlsxwriter') as writer:
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        final_df.to_excel(writer, sheet_name='Detailed_Provider_Data', index=False)
        op_df.to_excel(writer, sheet_name='Open_Payments_Summary', index=False)
        rx_df.to_excel(writer, sheet_name='Prescribing_Summary', index=False)
    
    print(f"Successfully generated Excel report at: {report_path}")

def main():
    """Main function to load config and generate the report."""
    print("--- Starting Step 4: Generate Report ---")
    try:
        config = load_config()
        generate_report(config)
    except Exception as e:
        print(f"An error occurred during report generation: {e}")
    print("--- Finished Step 4 ---")

if __name__ == "__main__":
    main()
