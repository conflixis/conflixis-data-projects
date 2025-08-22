#!/usr/bin/env python3
"""
Excel Export Module for THR Disclosures
Creates a multi-sheet Excel workbook with formatted, category-specific views
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Conflixis Brand Colors
COLORS = {
    'header_bg': '#0c343a',  # Conflixis green
    'header_text': '#FFFFFF',
    'subheader_bg': '#eab96d',  # Conflixis gold
    'risk_none': '#E8E8E8',
    'risk_low': '#90EE90',
    'risk_moderate': '#FFD700',
    'risk_high': '#FFA500',
    'risk_critical': '#FF6B6B',
    'alternate_row': '#F5F5F5'
}

# Business-friendly column name mappings
COLUMN_RENAMES = {
    'document_id': 'Disclosure ID',
    'provider_name': 'Provider Name',
    'provider_email': 'Email',
    'provider_npi': 'NPI',
    'person_with_interest': 'Person With Interest',
    'job_title': 'Job Title',
    'department': 'Department',
    'manager_name': 'Manager',
    'category_label': 'Category',
    'relationship_type': 'Disclosure Type',
    'entity_name': 'Entity/Company',
    'interest_type': 'Interest Type',
    'financial_amount': 'Amount (USD)',
    'risk_tier': 'Risk Level',
    'risk_score': 'Risk Score',
    'compensation_type': 'Compensation Type',
    'compensation_received_by': 'Received By',
    'compensation_received_by_self': 'Self Received',
    'service_provided': 'Services Provided',
    'relationship_start_date': 'Start Date',
    'relationship_end_date': 'End Date',
    'relationship_ongoing': 'Ongoing',
    'related_party_first_name': 'Related Party First Name',
    'related_party_last_name': 'Related Party Last Name',
    'related_party_entity_location': 'Related Party Location',
    'related_party_job_title': 'Related Party Job Title',
    'jurisdiction_location': 'Jurisdiction',
    'resolution_date': 'Resolution Date',
    'entity_where_occurred': 'Location of Issue',
    'disclosure_date': 'Disclosure Date',
    'disclosure_timeframe_start': 'Period Start',
    'disclosure_timeframe_end': 'Period End',
    'signature_date': 'Signed Date',
    'status': 'Status',
    'disputed': 'Disputed',
    'notes': 'Notes',
    'signature_name': 'Signature',
    'campaign_title': 'Campaign'
}


def format_currency(val):
    """Format numeric values as currency."""
    if pd.isna(val) or val == 0:
        return '-'
    return f'${val:,.2f}'


def format_boolean(val):
    """Format boolean values as Yes/No."""
    if pd.isna(val):
        return ''
    return 'Yes' if val else 'No'


def format_date(val):
    """Format dates consistently."""
    if pd.isna(val):
        return ''
    try:
        if isinstance(val, str):
            dt = pd.to_datetime(val)
        else:
            dt = val
        # Remove timezone info for Excel compatibility
        if hasattr(dt, 'tz_localize') and dt.tz is not None:
            dt = dt.tz_localize(None)
        return dt.strftime('%m/%d/%Y')
    except:
        return str(val)


def create_summary_sheet(df):
    """Create executive summary data."""
    
    summary_data = {
        'Metric': [],
        'Value': []
    }
    
    # Overall metrics
    summary_data['Metric'].extend([
        'Total Disclosures',
        'Unique Providers', 
        'Total Financial Amount',
        'Average Financial Amount',
        'Providers with NPI',
        'Disclosures with Risk'
    ])
    
    summary_data['Value'].extend([
        f'{len(df):,}',
        f'{df["provider_name"].nunique():,}',
        format_currency(df['financial_amount'].sum()),
        format_currency(df['financial_amount'].mean()),
        f'{(df["provider_npi"].notna()).sum():,}',
        f'{(df["risk_tier"] != "none").sum():,}'
    ])
    
    summary_df = pd.DataFrame(summary_data)
    
    # Category breakdown
    category_summary = df.groupby('category_label').agg({
        'document_id': 'count',
        'financial_amount': 'sum',
        'provider_name': 'nunique'
    }).reset_index()
    category_summary.columns = ['Category', 'Count', 'Total Amount', 'Unique Providers']
    
    # Risk distribution
    risk_summary = df['risk_tier'].value_counts().reset_index()
    risk_summary.columns = ['Risk Level', 'Count']
    
    # Top 10 by amount
    top_amounts = df.nlargest(10, 'financial_amount')[
        ['provider_name', 'entity_name', 'financial_amount', 'risk_tier']
    ].copy()
    
    return summary_df, category_summary, risk_summary, top_amounts


def filter_columns_for_category(df, category):
    """Select relevant columns for each category."""
    
    base_columns = [
        'document_id', 'provider_name', 'provider_email', 'provider_npi',
        'job_title', 'department', 'manager_name', 'disclosure_date',
        'signature_date', 'status', 'notes'
    ]
    
    category_specific = {
        'External Roles & Relationships': base_columns + [
            'relationship_type', 'person_with_interest', 'entity_name',
            'service_provided', 'relationship_start_date', 'relationship_end_date',
            'relationship_ongoing', 'related_party_first_name', 'related_party_last_name',
            'related_party_entity_location', 'related_party_job_title'
        ],
        'Financial & Investment Interests': base_columns + [
            'relationship_type', 'person_with_interest', 'entity_name',
            'interest_type', 'financial_amount', 'risk_tier', 'risk_score',
            'compensation_type', 'compensation_received_by', 'compensation_received_by_self',
            'service_provided', 'relationship_start_date', 'relationship_end_date'
        ],
        'Open Payments (CMS Imports)': base_columns + [
            'entity_name', 'financial_amount', 'risk_tier', 'risk_score',
            'disclosure_timeframe_start', 'disclosure_timeframe_end'
        ],
        'Political, Community, and Advocacy Activities': base_columns + [
            'relationship_type', 'person_with_interest', 'entity_name',
            'jurisdiction_location', 'service_provided',
            'relationship_start_date', 'relationship_end_date', 'relationship_ongoing'
        ],
        'Legal, Regulatory, Ethical, and Compliance Matters': base_columns + [
            'relationship_type', 'person_with_interest', 'entity_name',
            'entity_where_occurred', 'resolution_date', 'disputed'
        ]
    }
    
    columns = category_specific.get(category, base_columns)
    # Only include columns that exist in the dataframe
    return [col for col in columns if col in df.columns]


def export_to_excel(df, transactions_df=None, output_path=None):
    """
    Export disclosure data to a formatted Excel workbook.
    
    Args:
        df: Main disclosures dataframe
        transactions_df: Optional Open Payments transactions dataframe
        output_path: Output file path
    """
    
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f'thr_disclosures_analysis_{timestamp}.xlsx'
    
    logger.info(f"Creating Excel workbook: {output_path}")
    
    # Create Excel writer with xlsxwriter engine
    with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Define formats
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': COLORS['header_bg'],
            'font_color': COLORS['header_text'],
            'border': 1
        })
        
        currency_format = workbook.add_format({'num_format': '$#,##0.00'})
        date_format = workbook.add_format({'num_format': 'mm/dd/yyyy'})
        
        risk_formats = {
            'none': workbook.add_format({'bg_color': COLORS['risk_none']}),
            'low': workbook.add_format({'bg_color': COLORS['risk_low']}),
            'moderate': workbook.add_format({'bg_color': COLORS['risk_moderate']}),
            'high': workbook.add_format({'bg_color': COLORS['risk_high']}),
            'critical': workbook.add_format({'bg_color': COLORS['risk_critical']})
        }
        
        # 1. Executive Summary Sheet
        logger.info("Creating Executive Summary sheet...")
        summary_df, category_summary, risk_summary, top_amounts = create_summary_sheet(df)
        
        summary_df.to_excel(writer, sheet_name='Executive Summary', index=False, startrow=1)
        worksheet = writer.sheets['Executive Summary']
        worksheet.write('A1', 'THR Disclosure Analysis - Executive Summary', 
                       workbook.add_format({'bold': True, 'size': 14}))
        
        # Write category summary
        category_summary.to_excel(writer, sheet_name='Executive Summary', 
                                 index=False, startrow=len(summary_df) + 4)
        
        # Write risk summary
        risk_summary.to_excel(writer, sheet_name='Executive Summary',
                            index=False, startrow=len(summary_df) + len(category_summary) + 7)
        
        # Write top amounts
        top_amounts['financial_amount'] = top_amounts['financial_amount'].apply(format_currency)
        top_amounts.to_excel(writer, sheet_name='Executive Summary',
                            index=False, startrow=len(summary_df) + len(category_summary) + len(risk_summary) + 10)
        
        # 2. All Disclosures Sheet
        logger.info("Creating All Disclosures sheet...")
        all_df = df.copy()
        
        # Apply column renames
        all_df = all_df.rename(columns=COLUMN_RENAMES)
        
        # Format specific columns
        if 'Amount (USD)' in all_df.columns:
            all_df['Amount (USD)'] = all_df['Amount (USD)'].apply(format_currency)
        
        bool_columns = ['Ongoing', 'Self Received', 'Disputed']
        for col in bool_columns:
            if col in all_df.columns:
                all_df[col] = all_df[col].apply(format_boolean)
        
        # Format date columns and remove timezones for Excel compatibility
        date_columns = ['Disclosure Date', 'Signed Date', 'Start Date', 'End Date', 
                       'Resolution Date', 'Period Start', 'Period End']
        for col in date_columns:
            if col in all_df.columns:
                all_df[col] = pd.to_datetime(all_df[col], errors='coerce')
                # Remove timezone info if present
                if hasattr(all_df[col], 'dt') and hasattr(all_df[col].dt, 'tz_localize'):
                    all_df[col] = all_df[col].dt.tz_localize(None)
                all_df[col] = all_df[col].apply(format_date)
        
        all_df.to_excel(writer, sheet_name='All Disclosures', index=False)
        
        worksheet = writer.sheets['All Disclosures']
        # Format header row
        for col_num, value in enumerate(all_df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Add autofilter
        worksheet.autofilter(0, 0, len(all_df), len(all_df.columns) - 1)
        
        # Freeze panes
        worksheet.freeze_panes(1, 1)
        
        # 3. Category-specific sheets
        categories = df['category_label'].unique()
        
        for category in categories:
            if pd.notna(category):
                # Clean sheet name (Excel has 31 char limit)
                sheet_name = category.replace('External Roles & Relationships', 'External Roles')
                sheet_name = sheet_name.replace('Financial & Investment Interests', 'Financial')
                sheet_name = sheet_name.replace('Political, Community, and Advocacy Activities', 'Political')
                sheet_name = sheet_name.replace('Legal, Regulatory, Ethical, and Compliance Matters', 'Legal')
                sheet_name = sheet_name.replace('Open Payments (CMS Imports)', 'Open Payments')
                
                logger.info(f"Creating {sheet_name} sheet...")
                
                # Filter data for this category
                cat_df = df[df['category_label'] == category].copy()
                
                # Select relevant columns
                relevant_cols = filter_columns_for_category(cat_df, category)
                cat_df = cat_df[relevant_cols]
                
                # Rename columns
                cat_df = cat_df.rename(columns=COLUMN_RENAMES)
                
                # Format columns
                if 'Amount (USD)' in cat_df.columns:
                    cat_df['Amount (USD)'] = cat_df['Amount (USD)'].apply(format_currency)
                
                for col in ['Ongoing', 'Self Received', 'Disputed']:
                    if col in cat_df.columns:
                        cat_df[col] = cat_df[col].apply(format_boolean)
                
                # Format date columns and remove timezones
                date_columns = ['Disclosure Date', 'Signed Date', 'Start Date', 'End Date', 
                               'Resolution Date', 'Period Start', 'Period End']
                for col in date_columns:
                    if col in cat_df.columns:
                        cat_df[col] = pd.to_datetime(cat_df[col], errors='coerce')
                        if hasattr(cat_df[col], 'dt') and hasattr(cat_df[col].dt, 'tz_localize'):
                            cat_df[col] = cat_df[col].dt.tz_localize(None)
                        cat_df[col] = cat_df[col].apply(format_date)
                
                # Write to Excel
                cat_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                worksheet = writer.sheets[sheet_name]
                # Format header row
                for col_num, value in enumerate(cat_df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                
                worksheet.autofilter(0, 0, len(cat_df), len(cat_df.columns) - 1)
                worksheet.freeze_panes(1, 1)
        
        # 4. Open Payments Transactions (if provided)
        if transactions_df is not None:
            logger.info("Creating Open Payments Transactions sheet...")
            trans_df = transactions_df.copy()
            
            # Rename transaction columns
            trans_rename = {
                'document_id': 'Disclosure ID',
                'reporter_name': 'Provider Name',
                'reporter_email': 'Email',
                'provider_npi': 'NPI',
                'company_name': 'Company',
                'payment_date': 'Payment Date',
                'payment_amount': 'Amount (USD)',
                'payment_nature': 'Payment Type',
                'payment_form': 'Payment Form',
                'record_id': 'CMS Record ID'
            }
            trans_df = trans_df.rename(columns=trans_rename)
            
            if 'Amount (USD)' in trans_df.columns:
                trans_df['Amount (USD)'] = trans_df['Amount (USD)'].apply(format_currency)
            
            trans_df.to_excel(writer, sheet_name='OP Transactions', index=False)
            
            worksheet = writer.sheets['OP Transactions']
            for col_num, value in enumerate(trans_df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            worksheet.autofilter(0, 0, len(trans_df), len(trans_df.columns) - 1)
            worksheet.freeze_panes(1, 1)
        
        # 5. Data Dictionary Sheet
        logger.info("Creating Data Dictionary sheet...")
        dict_data = {
            'Field Name': list(COLUMN_RENAMES.keys()),
            'Display Name': list(COLUMN_RENAMES.values()),
            'Description': [
                'Unique identifier for each disclosure',
                'Name of the healthcare provider',
                'Provider email address',
                'National Provider Identifier',
                'Person the disclosure is about (may differ from provider)',
                'Provider job title',
                'Provider department',
                'Provider manager name',
                'Disclosure category',
                'Type of disclosure within category',
                'Company or entity involved',
                'Specific type of interest',
                'Dollar amount of financial interest',
                'Risk level based on amount',
                'Calculated risk score (0-100)',
                'Type of compensation received',
                'Who received the compensation',
                'Whether provider received compensation directly',
                'Description of services provided',
                'Start date of relationship',
                'End date of relationship',
                'Whether relationship is ongoing',
                'First name of related party',
                'Last name of related party',
                'Location of related party',
                'Job title of related party',
                'Jurisdiction for political disclosures',
                'Date issue was resolved',
                'Where compliance issue occurred',
                'Date disclosure was made',
                'Reporting period start',
                'Reporting period end',
                'Date disclosure was signed',
                'Disclosure status',
                'Whether disclosure is disputed',
                'Additional notes',
                'Signature on disclosure',
                'Campaign name'
            ][:len(COLUMN_RENAMES)]
        }
        
        dict_df = pd.DataFrame(dict_data)
        dict_df.to_excel(writer, sheet_name='Data Dictionary', index=False)
        
        worksheet = writer.sheets['Data Dictionary']
        for col_num, value in enumerate(dict_df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Auto-adjust column widths for all sheets
        for sheet_name in writer.sheets:
            worksheet = writer.sheets[sheet_name]
            worksheet.set_column('A:Z', 15)  # Default width
            worksheet.set_column('A:A', 20)  # ID column
            worksheet.set_column('B:B', 25)  # Name column
            
    logger.info(f"✓ Excel workbook created: {output_path}")
    return output_path


# Test function
if __name__ == "__main__":
    # This would be called from fetch_thr_disclosures.py
    print("Excel exporter module loaded successfully")