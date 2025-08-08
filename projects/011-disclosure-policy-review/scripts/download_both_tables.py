#!/usr/bin/env python3
"""
Download disclosure tables and member shards to get complete provider information including NPI
"""

import os
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

# Load environment variables
load_dotenv()

def download_tables():
    """Download disclosure tables and member shards with group_id filter"""
    
    print("=" * 80)
    print("DOWNLOADING DISCLOSURE TABLES AND MEMBER SHARDS")
    print("=" * 80)
    
    try:
        # Load credentials
        service_account_info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        
        # Initialize BigQuery client
        project_id = 'data-analytics-389803'
        client = bigquery.Client(credentials=credentials, project=project_id)
        
        group_id = 'gcO9AHYlNSzFeGTRSFRa'
        
        # Table 1: disclosures_raw_latest_parse
        table1_id = "conflixis-engine.firestore_export.disclosures_raw_latest_parse"
        
        print(f"\n1. Downloading {table1_id}")
        print("-" * 60)
        
        query1 = f"""
        SELECT *
        FROM `{table1_id}`
        WHERE group_id = '{group_id}'
        ORDER BY timestamp DESC
        """
        
        df1 = client.query(query1).to_dataframe()
        print(f"   Downloaded {len(df1)} rows")
        print(f"   Columns ({len(df1.columns)}): {', '.join(df1.columns)}")
        
        # Save Table 1
        os.makedirs("data/raw/disclosures", exist_ok=True)
        output_file1 = f"data/raw/disclosures/disclosures_raw_latest_parse_{datetime.now().strftime('%Y%m%d')}.csv"
        df1.to_csv(output_file1, index=False)
        print(f"   Saved to: {output_file1}")
        
        # Check for NPI in table 1
        npi_cols1 = [col for col in df1.columns if 'npi' in col.lower()]
        if npi_cols1:
            print(f"   Found NPI columns: {npi_cols1}")
            for col in npi_cols1:
                sample = df1[col].dropna().head(3).tolist()
                print(f"     {col} samples: {sample}")
        else:
            print("   No NPI column found in this table")
        
        # Table 2: disclosure_forms_raw_latest_v3  
        table2_id = "conflixis-engine.firestore_export.disclosure_forms_raw_latest_v3"
        
        print(f"\n2. Downloading {table2_id}")
        print("-" * 60)
        
        query2 = f"""
        SELECT *
        FROM `{table2_id}`
        WHERE group_id = '{group_id}'
        ORDER BY timestamp DESC
        """
        
        df2 = client.query(query2).to_dataframe()
        print(f"   Downloaded {len(df2)} rows")
        print(f"   Columns ({len(df2.columns)}): {', '.join(df2.columns)}")
        
        # Save Table 2
        output_file2 = f"data/raw/disclosures/disclosure_forms_raw_latest_v3_{datetime.now().strftime('%Y%m%d')}.csv"
        df2.to_csv(output_file2, index=False)
        print(f"   Saved to: {output_file2}")
        
        # Check for NPI in table 2
        npi_cols2 = [col for col in df2.columns if 'npi' in col.lower()]
        if npi_cols2:
            print(f"   Found NPI columns: {npi_cols2}")
            for col in npi_cols2:
                sample = df2[col].dropna().head(3).tolist()
                print(f"     {col} samples: {sample}")
        else:
            print("   No NPI column found in this table")
        
        # Table 3: member_shards_raw_latest_parsed
        table3_id = "conflixis-engine.firestore_export.member_shards_raw_latest_parsed"
        
        print(f"\n3. Downloading {table3_id}")
        print("-" * 60)
        
        query3 = f"""
        SELECT *
        FROM `{table3_id}`
        WHERE group_id = '{group_id}'
        """
        
        df3 = client.query(query3).to_dataframe()
        print(f"   Downloaded {len(df3)} rows")
        print(f"   Columns ({len(df3.columns)}): {', '.join(df3.columns)}")
        
        # Save Table 3
        output_file3 = f"data/raw/disclosures/member_shards_raw_latest_parsed_{datetime.now().strftime('%Y%m%d')}.csv"
        df3.to_csv(output_file3, index=False)
        print(f"   Saved to: {output_file3}")
        
        # Check for key member columns
        member_cols = ['npi', 'email', 'entity', 'job_title', 'manager', 'display_name', 'first_name', 'last_name']
        found_member_cols = [col for col in df3.columns if any(k in col.lower() for k in member_cols)]
        print(f"   Found member columns: {found_member_cols}")
        
        for col in found_member_cols[:5]:  # Show first 5 relevant columns
            if col in df3.columns:
                sample = df3[col].dropna().head(3).tolist()
                if sample:
                    print(f"     {col} samples: {sample}")
        
        # Show sample data from all tables
        print("\n" + "=" * 80)
        print("SAMPLE DATA FROM ALL TABLES")
        print("=" * 80)
        
        print("\nTable 1 (disclosures_raw_latest_parse) - First row:")
        print("-" * 60)
        if len(df1) > 0:
            first_row = df1.iloc[0]
            for col, val in first_row.items():
                if pd.notna(val) and str(val).strip():
                    val_str = str(val)[:100] + "..." if len(str(val)) > 100 else str(val)
                    print(f"  {col}: {val_str}")
        
        print("\nTable 2 (disclosure_forms_raw_latest_v3) - First row:")
        print("-" * 60)
        if len(df2) > 0:
            first_row = df2.iloc[0]
            for col, val in first_row.items():
                if pd.notna(val) and str(val).strip():
                    val_str = str(val)[:100] + "..." if len(str(val)) > 100 else str(val)
                    print(f"  {col}: {val_str}")
        
        print("\nTable 3 (member_shards_raw_latest_parsed) - First row:")
        print("-" * 60)
        if len(df3) > 0:
            first_row = df3.iloc[0]
            for col, val in first_row.items():
                if pd.notna(val) and str(val).strip():
                    val_str = str(val)[:100] + "..." if len(str(val)) > 100 else str(val)
                    print(f"  {col}: {val_str}")
        
        # Check if we can join the tables
        print("\n" + "=" * 80)
        print("ANALYSIS: How tables might relate")
        print("=" * 80)
        
        # Check common identifiers
        common_cols = set(df1.columns) & set(df2.columns)
        print(f"\nCommon columns: {common_cols}")
        
        # Check if document_id matches
        if 'document_id' in df1.columns and 'document_id' in df2.columns:
            common_ids = set(df1['document_id']) & set(df2['document_id'])
            print(f"\nCommon document_ids: {len(common_ids)} out of {len(df1)} in table1 and {len(df2)} in table2")
        
        # Look for member/provider identifiers
        print("\nSearching for member/provider identifiers:")
        for df, table_name in [(df1, "Table 1"), (df2, "Table 2")]:
            member_cols = [col for col in df.columns if any(k in col.lower() for k in ['member', 'provider', 'physician', 'doctor', 'npi', 'reporter', 'name'])]
            if member_cols:
                print(f"\n{table_name} potential identifier columns:")
                for col in member_cols:
                    non_null = df[col].dropna()
                    if len(non_null) > 0:
                        sample = non_null.iloc[0]
                        print(f"  {col}: {sample}")
        
        # Check member shards matching with disclosure data
        print("\nChecking member shards matching:")
        if 'display_name' in df3.columns:
            unique_members = df3['display_name'].nunique()
            print(f"  Unique members in member_shards: {unique_members}")
            
            # Check how many match with disclosures
            if 'reporter_name' in df1.columns:
                disclosure_names = set(df1['reporter_name'].dropna())
                member_names = set(df3['display_name'].dropna())
                matching_names = disclosure_names & member_names
                print(f"  Matching names between disclosures and members: {len(matching_names)} out of {len(disclosure_names)}")
        
        return df1, df2, df3
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None

if __name__ == "__main__":
    df1, df2, df3 = download_tables()
    
    if df1 is not None and df2 is not None and df3 is not None:
        print("\n" + "=" * 80)
        print("DOWNLOAD COMPLETE")
        print(f"Table 1 (disclosures): {len(df1)} rows, {len(df1.columns)} columns")
        print(f"Table 2 (forms): {len(df2)} rows, {len(df2.columns)} columns")
        print(f"Table 3 (members): {len(df3)} rows, {len(df3.columns)} columns")
        print("=" * 80)