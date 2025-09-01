
import os
import yaml
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json

def load_config():
    """Loads the configuration from the CONFIG.yaml file."""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'CONFIG.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def get_gcp_credentials(env_var='GCP_SERVICE_ACCOUNT_KEY'):
    """Gets GCP credentials from environment variable."""
    key_json = os.getenv(env_var)
    if not key_json:
        raise ValueError(f"Environment variable {env_var} not set.")
    try:
        key_dict = json.loads(key_json)
        return service_account.Credentials.from_service_account_info(key_dict)
    except json.JSONDecodeError:
        raise ValueError("Failed to decode GCP service account key from JSON.")
    except Exception as e:
        raise RuntimeError(f"Failed to create credentials from service account info: {e}")

def format_currency(value):
    if value is None or not pd.notna(value):
        return "$0"
    return f"${value:,.0f}"

def format_multiplier(value):
    if value is None or not pd.notna(value):
        return "N/A"
    return f"{value:.1f}x"

def get_provider_type_sql_case():
    """Returns a SQL CASE statement for classifying provider types."""
    return """
        CASE
            WHEN LOWER(Primary_Specialty) LIKE '%physician assistant%' THEN 'PA'
            WHEN LOWER(Primary_Specialty) LIKE '%nurse%' THEN 'NP'
            ELSE 'Physician'
        END
    """

def build_main_query(config):
    """Builds the main BigQuery SQL query to perform all analyses."""
    bq_config = config['bigquery']
    health_system = config['health_system']
    key_drugs = config['report_settings']['key_drugs']

    npi_table_name = bq_config['npi_table'].replace('[abbrev]', health_system['short_name'])
    npi_table_id = f"`{bq_config['project_id']}.{bq_config['dataset_id']}.{npi_table_name}`"
    op_table_id = f"`{bq_config['project_id']}.{bq_config['dataset_id']}.{bq_config['op_table']}`"
    rx_table_id = f"`{bq_config['project_id']}.{bq_config['dataset_id']}.{bq_config['rx_table']}`"

    # Create a subquery for each key drug
    drug_queries = []
    for drug in key_drugs:
        drug_queries.append(f"""
        (
            SELECT
                '{drug}' AS drug_name,
                AVG(CASE WHEN payment_status.paid_by_manufacturer THEN rx.PAYMENTS ELSE NULL END) AS avg_rx_paid,
                AVG(CASE WHEN payment_status.paid_by_manufacturer IS NULL THEN rx.PAYMENTS ELSE NULL END) AS avg_rx_unpaid,
                SUM(CASE WHEN payment_status.paid_by_manufacturer THEN rx.PAYMENTS ELSE 0 END) / NULLIF(SUM(p.payment_value), 0) AS roi
            FROM {rx_table_id} rx
            LEFT JOIN (
                SELECT DISTINCT CAST(covered_recipient_npi AS STRING) AS npi, applicable_manufacturer_or_applicable_gpo_making_payment_name as manufacturer, SUM(total_amount_of_payment_usdollars) as payment_value
                FROM {op_table_id}
                GROUP BY 1, 2
            ) p ON CAST(rx.NPI AS STRING) = p.npi AND LOWER(rx.MANUFACTURER) = LOWER(p.manufacturer)
            LEFT JOIN (
                SELECT DISTINCT CAST(NPI AS STRING) AS NPI, TRUE as paid_by_manufacturer
                FROM {op_table_id} op_inner
                JOIN {rx_table_id} rx_inner ON CAST(op_inner.covered_recipient_npi AS STRING) = CAST(rx_inner.NPI AS STRING)
                WHERE LOWER(op_inner.applicable_manufacturer_or_applicable_gpo_making_payment_name) = LOWER(rx_inner.MANUFACTURER)
            ) AS payment_status ON CAST(rx.NPI AS STRING) = payment_status.NPI
            WHERE LOWER(rx.BRAND_NAME) = LOWER('{drug}') AND CAST(rx.NPI AS STRING) IN (SELECT NPI FROM {npi_table_id})
            GROUP BY drug_name
        )
        """)

    drug_union_query = "\nUNION ALL\n".join(drug_queries)

    query = f"""
    WITH ProviderNPIs AS (
        SELECT NPI, Primary_Specialty, {get_provider_type_sql_case()} AS provider_type
        FROM {npi_table_id}
    ),
    ProviderPayments AS (
        SELECT CAST(covered_recipient_npi AS STRING) AS covered_recipient_npi, total_amount_of_payment_usdollars
        FROM {op_table_id}
        WHERE CAST(covered_recipient_npi AS STRING) IN (SELECT NPI FROM ProviderNPIs)
    ),
    ProviderPrescriptions AS (
        SELECT CAST(NPI AS STRING) AS NPI, PAYMENTS, PRESCRIPTIONS
        FROM {rx_table_id}
        WHERE CAST(NPI AS STRING) IN (SELECT NPI FROM ProviderNPIs)
    ),
    SummaryStats AS (
        SELECT
            (SELECT COUNT(DISTINCT NPI) FROM ProviderNPIs) AS total_providers,
            (SELECT COUNT(DISTINCT covered_recipient_npi) FROM ProviderPayments) AS providers_receiving_payments,
            (SELECT SUM(total_amount_of_payment_usdollars) FROM ProviderPayments) AS total_payments,
            (SELECT COUNT(*) FROM ProviderPayments) AS total_transactions,
            (SELECT COUNT(DISTINCT NPI) FROM ProviderPrescriptions) AS unique_prescribers,
            (SELECT SUM(PRESCRIPTIONS) FROM ProviderPrescriptions) AS total_prescriptions,
            (SELECT SUM(PAYMENTS) FROM ProviderPrescriptions) AS total_rx_value
    ),
    DrugCorrelation AS (
        {drug_union_query}
    ),
    ProviderTypeVulnerability AS (
        SELECT
            npi.provider_type,
            AVG(CASE WHEN p.covered_recipient_npi IS NOT NULL THEN rx.PAYMENTS ELSE NULL END) AS avg_rx_paid,
            AVG(CASE WHEN p.covered_recipient_npi IS NULL THEN rx.PAYMENTS ELSE NULL END) AS avg_rx_unpaid
        FROM ProviderNPIs npi
        LEFT JOIN ProviderPrescriptions rx ON npi.NPI = rx.NPI
        LEFT JOIN (SELECT DISTINCT covered_recipient_npi FROM ProviderPayments) p ON npi.NPI = p.covered_recipient_npi
        GROUP BY npi.provider_type
    )
    SELECT
        (SELECT TO_JSON_STRING(t) FROM SummaryStats t) AS summary_stats,
        (SELECT TO_JSON_STRING(ARRAY_AGG(t)) FROM DrugCorrelation t) AS drug_correlation,
        (SELECT TO_JSON_STRING(ARRAY_AGG(t)) FROM ProviderTypeVulnerability t) AS provider_type_vulnerability;
    """
    return query
    

def generate_narrative_report(config, credentials):
    """
    Performs in-depth analysis via a single BigQuery query and generates a narrative report.
    """
    print("--- Building and executing main analysis query in BigQuery ---")
    query = build_main_query(config)
    client = bigquery.Client(credentials=credentials, project=config['bigquery']['project_id'])

    try:
        # This single query returns all our analysis results in one row
        query_job = client.query(query)
        results = query_job.to_dataframe()
        print("--- Successfully executed main analysis query ---")
    except Exception as e:
        print(f"An error occurred during the BigQuery query: {e}")
        # print(f"Query was:\n{query}") # Uncomment for debugging
        return

    # Extract JSON results from the single row dataframe
    summary_stats = json.loads(results['summary_stats'][0])
    drug_correlation_data = json.loads(results['drug_correlation'][0])
    provider_type_data = json.loads(results['provider_type_vulnerability'][0])

    # Convert to DataFrames for easy formatting
    correlation_df = pd.DataFrame(drug_correlation_data)
    provider_type_df = pd.DataFrame(provider_type_data)

    # --- Robust Type Conversion ---
    # Ensure all relevant columns are numeric, coercing errors to NaN
    cols_to_convert = ['avg_rx_paid', 'avg_rx_unpaid', 'roi']
    for col in cols_to_convert:
        if col in correlation_df.columns:
            correlation_df[col] = pd.to_numeric(correlation_df[col], errors='coerce')
    
    cols_to_convert = ['avg_rx_paid', 'avg_rx_unpaid']
    for col in cols_to_convert:
        if col in provider_type_df.columns:
            provider_type_df[col] = pd.to_numeric(provider_type_df[col], errors='coerce')

    # --- Generate Markdown Report ---
    health_system = config['health_system']
    docs_dir = os.path.join(config['project_paths']['project_dir'], 'docs')
    os.makedirs(docs_dir, exist_ok=True)
    md_report_path = os.path.join(docs_dir, f"{health_system['short_name']}_coi_analytics_report.md")

    with open(md_report_path, 'w') as f:
        total_providers = summary_stats['total_providers']
        providers_receiving_payments = summary_stats['providers_receiving_payments']
        percent_paid = (providers_receiving_payments / total_providers * 100) if total_providers > 0 else 0

        f.write(f"# {health_system['name']} Open Payments Report\n\n")
        f.write("## Executive Summary\n\n")
        f.write(f"This analysis examines financial relationships between industry and {health_system['name']}'s network of {total_providers:,} providers.\n\n")
        f.write(f"**{providers_receiving_payments:,} providers ({percent_paid:.1f}%)** received **{format_currency(summary_stats['total_payments'])}** across {summary_stats['total_transactions']:,} transactions.\n\n")

        f.write("## Payment-Prescription Correlations\n\n")
        correlation_df['Influence Factor'] = (correlation_df['avg_rx_paid'] / correlation_df['avg_rx_unpaid']).apply(format_multiplier)
        correlation_df['ROI'] = correlation_df['roi'].apply(format_multiplier)
        correlation_df['Avg Rx Value (Paid)'] = correlation_df['avg_rx_paid'].apply(format_currency)
        correlation_df['Avg Rx Value (Unpaid)'] = correlation_df['avg_rx_unpaid'].apply(format_currency)
        f.write(correlation_df[['drug_name', 'Avg Rx Value (Paid)', 'Avg Rx Value (Unpaid)', 'Influence Factor', 'ROI']].to_markdown(index=False))
        f.write("\n\n")

        f.write("## Provider Type Vulnerability Analysis\n\n")
        provider_type_df['Increase with Payment'] = ((provider_type_df['avg_rx_paid'] - provider_type_df['avg_rx_unpaid']) / provider_type_df['avg_rx_unpaid'] * 100).apply(lambda x: f"{x:.1f}%")
        provider_type_df['Avg Rx Value (Paid)'] = provider_type_df['avg_rx_paid'].apply(format_currency)
        provider_type_df['Avg Rx Value (Unpaid)'] = provider_type_df['avg_rx_unpaid'].apply(format_currency)
        f.write(provider_type_df[['provider_type', 'Avg Rx Value (Paid)', 'Avg Rx Value (Unpaid)', 'Increase with Payment']].to_markdown(index=False))
        f.write("\n\n")

    print(f"Successfully generated narrative Markdown report at: {md_report_path}")

def main():
    print("--- Starting Step 4: Generate Narrative Report (Server-Side) ---")
    try:
        config = load_config()
        credentials = get_gcp_credentials()
        generate_narrative_report(config, credentials)
    except Exception as e:
        print(f"An error occurred: {e}")
    print("--- Finished Step 4 ---")

if __name__ == "__main__":
    main()
