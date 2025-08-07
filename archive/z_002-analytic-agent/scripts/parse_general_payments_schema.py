#!/usr/bin/env python3
"""Parse General Payments schema from the extracted PDF text."""

import json
import re
from typing import Dict, List, Optional

def parse_field_line(line: str) -> Optional[Dict[str, str]]:
    """Parse a field definition line from the PDF table."""
    # Pattern to match field entries
    # Field name often has underscores and may be split across lines
    pattern = r'^([A-Za-z][A-Za-z0-9_]+(?:_[A-Za-z0-9]+)*)\s+'
    
    match = re.match(pattern, line)
    if match:
        return {'field_name': match.group(1), 'line': line}
    return None

def clean_field_name(name: str) -> str:
    """Clean up field names that may be split across lines."""
    # Remove extra spaces and normalize
    return name.replace(' ', '').replace('\n', '')

def map_to_bigquery_type(pdf_type: str, format_info: str = '') -> str:
    """Map PDF data types to BigQuery data types."""
    pdf_type = pdf_type.upper()
    
    if 'VARCHAR' in pdf_type or 'CHAR' in pdf_type:
        return 'STRING'
    elif 'NUMBER' in pdf_type:
        if 'decimal' in format_info.lower() or ',' in pdf_type:
            return 'FLOAT64'
        else:
            return 'INT64'
    elif 'DATE' in pdf_type:
        return 'DATE'
    else:
        return 'STRING'  # Default to STRING

def extract_max_length(data_type: str) -> Optional[int]:
    """Extract max length from data type definition."""
    match = re.search(r'\((\d+)', data_type)
    if match:
        return int(match.group(1))
    return None

# Read the extracted text
with open('/home/incent/conflixis-analytics/projects/002-analytic-agent/appendix_b_section.txt', 'r') as f:
    content = f.read()

# Split into lines
lines = content.split('\n')

# Parse fields
fields = []
current_field = None
i = 0

while i < len(lines):
    line = lines[i].strip()
    
    # Skip empty lines and headers
    if not line or line.startswith('Field Name') or line.startswith('Appendix'):
        i += 1
        continue
    
    # Check if this is a field name line
    field_match = parse_field_line(line)
    if field_match:
        # Save previous field if exists
        if current_field:
            fields.append(current_field)
        
        # Start new field
        field_name = field_match['field_name']
        
        # Look for the description and other details in subsequent lines
        description_lines = []
        sample_data = ''
        data_type = ''
        format_type = ''
        max_length = ''
        
        # Continue reading until we find the sample data and type info
        j = i + 1
        while j < len(lines) and j < i + 10:  # Look ahead up to 10 lines
            next_line = lines[j].strip()
            
            # Check if this line contains data type info (usually has VARCHAR, NUMBER, etc.)
            if re.search(r'VARCHAR|CHAR|NUMBER|DATE', next_line, re.IGNORECASE):
                # This line likely contains sample data, data type, format, and length
                parts = next_line.split()
                
                # Try to identify parts
                for k, part in enumerate(parts):
                    if re.match(r'VARCHAR|CHAR|NUMBER|DATE', part, re.IGNORECASE):
                        # Found data type
                        data_type = part
                        # Sample data is everything before this
                        if k > 0:
                            sample_data = ' '.join(parts[:k])
                        # Format and length come after
                        if k + 1 < len(parts):
                            format_type = parts[k + 1]
                        if k + 2 < len(parts):
                            max_length = parts[k + 2]
                        break
                break
            else:
                # This is part of the description
                if next_line and not re.match(r'^[A-Z][A-Za-z0-9_]+', next_line):
                    description_lines.append(next_line)
            j += 1
        
        # Clean up field name
        field_name = clean_field_name(field_name)
        
        # Combine description
        description = ' '.join(description_lines).strip()
        
        # Create field object
        current_field = {
            'field_name': field_name,
            'description': description,
            'sample_data': sample_data,
            'original_data_type': data_type,
            'format': format_type,
            'max_length': max_length,
            'bigquery_type': map_to_bigquery_type(data_type, format_type)
        }
    
    i += 1

# Add last field
if current_field:
    fields.append(current_field)

# Manual extraction for fields that might have been missed or need correction
# Based on the PDF content, here are all the General Payments fields:

general_payments_fields = [
    {
        'field_name': 'Change_Type',
        'description': 'An indicator showing if the payment record is New, Added, Changed, or Unchanged in the current publication compared to the previous publication.',
        'sample_data': 'NEW',
        'original_data_type': 'VARCHAR2(20)',
        'bigquery_type': 'STRING',
        'max_length': 20
    },
    {
        'field_name': 'Covered_Recipient_Type',
        'description': 'An indicator showing if the recipient of the payment or transfer of value is a physician-covered recipient or non-physician practitioner or a teaching hospital.',
        'sample_data': 'Physician',
        'original_data_type': 'VARCHAR2(50)',
        'bigquery_type': 'STRING',
        'max_length': 50
    },
    {
        'field_name': 'Teaching_Hospital_CCN',
        'description': 'A unique identifying number (CMS Certification Number) of the Teaching Hospital receiving the payment or other transfer of value.',
        'sample_data': '330024',
        'original_data_type': 'VARCHAR2(06)',
        'bigquery_type': 'STRING',
        'max_length': 6
    },
    {
        'field_name': 'Teaching_Hospital_ID',
        'description': 'The system generated a unique identifier of the Teaching Hospital receiving the payment or other transfer of value.',
        'sample_data': '1000000999',
        'original_data_type': 'NUMBER(38,0)',
        'bigquery_type': 'INT64',
        'max_length': 38
    },
    {
        'field_name': 'Teaching_Hospital_Name',
        'description': 'The name of the Teaching Hospital receiving the payment or other transfer of value. The name displayed is as listed in CMS teaching hospital list under Hospital name.',
        'sample_data': 'Healthy Heart Hospital',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Covered_Recipient_Profile_ID',
        'description': 'System generated unique identifier for covered recipient physician or covered recipient non-physician practitioner profile receiving the payment or other transfer of value.',
        'sample_data': '1000000378',
        'original_data_type': 'NUMBER(38,0)',
        'bigquery_type': 'INT64',
        'max_length': 38
    },
    {
        'field_name': 'Covered_Recipient_NPI',
        'description': 'National Provider Identifier is a unique identification number for covered recipient physician or covered recipient non-physician practitioner (and not the NPI of a group the physician/non-physician practitioner belongs to).',
        'sample_data': '2495351826',
        'original_data_type': 'NUMBER(10,0)',
        'bigquery_type': 'INT64',
        'max_length': 10
    },
    {
        'field_name': 'Covered_Recipient_First_Name',
        'description': 'First name of the covered recipient physician or covered recipient non-physician practitioner receiving the payment or transfer of value, as reported by the submitting entity.',
        'sample_data': 'John',
        'original_data_type': 'VARCHAR2(20)',
        'bigquery_type': 'STRING',
        'max_length': 20
    },
    {
        'field_name': 'Covered_Recipient_Middle_Name',
        'description': 'Middle name of the covered recipient physician or covered recipient non-physician practitioner receiving the payment or transfer of value, as reported by the submitting entity.',
        'sample_data': 'A',
        'original_data_type': 'VARCHAR2(20)',
        'bigquery_type': 'STRING',
        'max_length': 20
    },
    {
        'field_name': 'Covered_Recipient_Last_Name',
        'description': 'Last name of the covered recipient physician or covered recipient non-physician practitioner receiving the payment or transfer of value, as reported by the submitting entity.',
        'sample_data': 'Smith',
        'original_data_type': 'VARCHAR2(35)',
        'bigquery_type': 'STRING',
        'max_length': 35
    },
    {
        'field_name': 'Covered_Recipient_Name_Suffix',
        'description': 'Name suffix of the covered recipient physician or covered recipient non-physician practitioner receiving the payment or transfer of value, as reported by the submitting entity.',
        'sample_data': 'III',
        'original_data_type': 'VARCHAR2(5)',
        'bigquery_type': 'STRING',
        'max_length': 5
    },
    {
        'field_name': 'Recipient_Primary_Business_Street_Address_Line1',
        'description': 'The first line of the primary practice/business street address of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value.',
        'sample_data': '7500 Security Blvd.',
        'original_data_type': 'VARCHAR2(55)',
        'bigquery_type': 'STRING',
        'max_length': 55
    },
    {
        'field_name': 'Recipient_Primary_Business_Street_Address_Line2',
        'description': 'The second line of the primary practice/business street address of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value.',
        'sample_data': 'Suite 100',
        'original_data_type': 'VARCHAR2(55)',
        'bigquery_type': 'STRING',
        'max_length': 55
    },
    {
        'field_name': 'Recipient_City',
        'description': 'The primary practice/business city of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value.',
        'sample_data': 'Baltimore',
        'original_data_type': 'VARCHAR2(40)',
        'bigquery_type': 'STRING',
        'max_length': 40
    },
    {
        'field_name': 'Recipient_State',
        'description': 'The primary practice/business state or territory abbreviation of the physician or teaching hospital (covered recipient) receiving the payment or transfer of value, if the primary practice/business address is in United States.',
        'sample_data': 'MD',
        'original_data_type': 'CHAR(2)',
        'bigquery_type': 'STRING',
        'max_length': 2
    },
    {
        'field_name': 'Recipient_Zip_Code',
        'description': 'The 9-digit zip code for the primary practice/business location of the physician or teaching hospital (covered recipient) receiving the payment or transfer of value.',
        'sample_data': '21244-3712',
        'original_data_type': 'VARCHAR2(10)',
        'bigquery_type': 'STRING',
        'max_length': 10
    },
    {
        'field_name': 'Recipient_Country',
        'description': 'The primary practice/business address country name of the physician or teaching hospital (covered recipient) receiving the payment or transfer of value.',
        'sample_data': 'US',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Recipient_Province',
        'description': 'The primary practice/business province name of the physician (covered recipient) receiving the payment or other transfer of value, if the primary practice/business address is outside the United States, and if applicable.',
        'sample_data': 'Manitoba',
        'original_data_type': 'VARCHAR2(20)',
        'bigquery_type': 'STRING',
        'max_length': 20
    },
    {
        'field_name': 'Recipient_Postal_Code',
        'description': 'The international postal code for the primary practice/business location of the physician (covered recipient) receiving the payment or other transfer of value, if the primary practice/business address is outside the United States.',
        'sample_data': '5600098',
        'original_data_type': 'VARCHAR2(20)',
        'bigquery_type': 'STRING',
        'max_length': 20
    },
    {
        'field_name': 'Covered_Recipient_Primary_Type_1',
        'description': 'Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient).',
        'sample_data': 'Medical Doctor (MD)/ Physician Assistant (PA)',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Covered_Recipient_Primary_Type_2',
        'description': 'Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021.',
        'sample_data': 'Medical Doctor (MD)/ Physician Assistant (PA)',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Covered_Recipient_Primary_Type_3',
        'description': 'Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021.',
        'sample_data': 'Medical Doctor (MD)/ Physician Assistant (PA)',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Covered_Recipient_Primary_Type_4',
        'description': 'Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021.',
        'sample_data': 'Medical Doctor (MD)/ Physician Assistant (PA)',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Covered_Recipient_Primary_Type_5',
        'description': 'Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021.',
        'sample_data': 'Medical Doctor (MD)/ Physician Assistant (PA)',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Covered_Recipient_Primary_Type_6',
        'description': 'Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021.',
        'sample_data': 'Medical Doctor (MD)/ Physician Assistant (PA)',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Covered_Recipient_Specialty_1',
        'description': 'Physician\'s or non-physician practitioner\'s specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021.',
        'sample_data': 'Allopathic & Osteopathic Physicians|Obstetrics & Gynecology',
        'original_data_type': 'VARCHAR2(300)',
        'bigquery_type': 'STRING',
        'max_length': 300
    },
    {
        'field_name': 'Covered_Recipient_Specialty_2',
        'description': 'Physician\'s or non-physician practitioner\'s specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021.',
        'sample_data': 'Allopathic & Osteopathic Physicians|Obstetrics & Gynecology',
        'original_data_type': 'VARCHAR2(300)',
        'bigquery_type': 'STRING',
        'max_length': 300
    },
    {
        'field_name': 'Covered_Recipient_Specialty_3',
        'description': 'Physician\'s or non-physician practitioner\'s specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021.',
        'sample_data': 'Allopathic & Osteopathic Physicians|Obstetrics & Gynecology',
        'original_data_type': 'VARCHAR2(300)',
        'bigquery_type': 'STRING',
        'max_length': 300
    },
    {
        'field_name': 'Covered_Recipient_Specialty_4',
        'description': 'Physician\'s or non-physician practitioner\'s specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021.',
        'sample_data': 'Allopathic & Osteopathic Physicians|Obstetrics & Gynecology',
        'original_data_type': 'VARCHAR2(300)',
        'bigquery_type': 'STRING',
        'max_length': 300
    },
    {
        'field_name': 'Covered_Recipient_Specialty_5',
        'description': 'Physician\'s or non-physician practitioner\'s specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021.',
        'sample_data': 'Allopathic & Osteopathic Physicians|Obstetrics & Gynecology',
        'original_data_type': 'VARCHAR2(300)',
        'bigquery_type': 'STRING',
        'max_length': 300
    },
    {
        'field_name': 'Covered_Recipient_Specialty_6',
        'description': 'Physician\'s or non-physician practitioner\'s specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021.',
        'sample_data': 'Allopathic & Osteopathic Physicians|Obstetrics & Gynecology',
        'original_data_type': 'VARCHAR2(300)',
        'bigquery_type': 'STRING',
        'max_length': 300
    },
    {
        'field_name': 'Covered_Recipient_License_State_code1',
        'description': 'The state license number of the covered recipient physician or covered recipient non-physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 license states, if a physician is licensed in multiple states.',
        'sample_data': 'MA',
        'original_data_type': 'CHAR(2)',
        'bigquery_type': 'STRING',
        'max_length': 2
    },
    {
        'field_name': 'Covered_Recipient_License_State_code2',
        'description': 'The state license number of the covered recipient physician or covered recipient non-physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 license states, if a physician is licensed in multiple states.',
        'sample_data': 'PA',
        'original_data_type': 'CHAR(2)',
        'bigquery_type': 'STRING',
        'max_length': 2
    },
    {
        'field_name': 'Covered_Recipient_License_State_code3',
        'description': 'The state license number of the covered recipient physician or covered recipient non-physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 license states, if a physician is licensed in multiple states.',
        'sample_data': 'VA',
        'original_data_type': 'CHAR(2)',
        'bigquery_type': 'STRING',
        'max_length': 2
    },
    {
        'field_name': 'Covered_Recipient_License_State_code4',
        'description': 'The state license number of the covered recipient physician or covered recipient non-physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 license states, if a physician is licensed in multiple states.',
        'sample_data': 'MI',
        'original_data_type': 'CHAR(2)',
        'bigquery_type': 'STRING',
        'max_length': 2
    },
    {
        'field_name': 'Covered_Recipient_License_State_code5',
        'description': 'The state license number of the covered recipient physician or covered recipient non-physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 license states, if a physician is licensed in multiple states.',
        'sample_data': 'WI',
        'original_data_type': 'CHAR(2)',
        'bigquery_type': 'STRING',
        'max_length': 2
    },
    {
        'field_name': 'Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name',
        'description': 'The textual proper name of the submitting applicable manufacturer or submitting applicable GPO.',
        'sample_data': 'ABCDE Manufacturing',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID',
        'description': 'System generated unique identifier of the Applicable Manufacturer or Applicable Group Purchasing Organization (GPO) Making a payment or other transfer of value.',
        'sample_data': '1000000049',
        'original_data_type': 'VARCHAR2(12)',
        'bigquery_type': 'STRING',
        'max_length': 12
    },
    {
        'field_name': 'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
        'description': 'The textual proper name of the applicable manufacturer or applicable GPO making the payment or other transfer of value.',
        'sample_data': 'ABCDE Manufacturing',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State',
        'description': 'State name of the submitting applicable manufacturer or submitting applicable GPO as provided in Open Payments.',
        'sample_data': 'VA',
        'original_data_type': 'CHAR(2)',
        'bigquery_type': 'STRING',
        'max_length': 2
    },
    {
        'field_name': 'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country',
        'description': 'Country name of the Submitting Applicable Manufacturer or Submitting Applicable Group Purchasing Organization (GPO) as provided in Open Payments.',
        'sample_data': 'United States',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Total_Amount_of_Payment_USDollars',
        'description': 'U.S. dollar amount of payment or other transfer of value to the recipient (manufacturer must convert to dollar currency if necessary).',
        'sample_data': '1978.00',
        'original_data_type': 'NUMBER(12,2)',
        'bigquery_type': 'FLOAT64',
        'max_length': 12
    },
    {
        'field_name': 'Date_of_Payment',
        'description': 'If a singular payment, then this is the actual date the payment was issued; if a series of payments or an aggregated set of payments, this is the date of the first payment to the covered recipient in this PY.',
        'sample_data': '04/01/2015',
        'original_data_type': 'DATE',
        'bigquery_type': 'DATE',
        'max_length': 12
    },
    {
        'field_name': 'Number_of_Payments_Included_in_Total_Amount',
        'description': 'The number of discrete payments being reported in the "Total Amount of Payment".',
        'sample_data': '1',
        'original_data_type': 'NUMBER(3,0)',
        'bigquery_type': 'INT64',
        'max_length': 3
    },
    {
        'field_name': 'Form_of_Payment_or_Transfer_of_Value',
        'description': 'The method of payment used to pay the covered recipient or to make the transfer of value.',
        'sample_data': 'In-kind items and services',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Nature_of_Payment_or_Transfer_of_Value',
        'description': 'The nature of payment used to pay the covered recipient or to make the transfer of value.',
        'sample_data': 'Consulting Fee',
        'original_data_type': 'VARCHAR2(200)',
        'bigquery_type': 'STRING',
        'max_length': 200
    },
    {
        'field_name': 'City_of_Travel',
        'description': 'For "Travel and Lodging" payments, destination city where covered recipient traveled.',
        'sample_data': 'San Diego',
        'original_data_type': 'VARCHAR2(40)',
        'bigquery_type': 'STRING',
        'max_length': 40
    },
    {
        'field_name': 'State_of_Travel',
        'description': 'For "Travel and Lodging" payments, the destination state where the covered recipient traveled.',
        'sample_data': 'CA',
        'original_data_type': 'CHAR(2)',
        'bigquery_type': 'STRING',
        'max_length': 2
    },
    {
        'field_name': 'Country_of_Travel',
        'description': 'For "Travel and Lodging" payments, the destination country where the covered recipient traveled.',
        'sample_data': 'United States',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Physician_Ownership_Indicator',
        'description': 'Indicates whether the physician holds an ownership or investment interest in the applicable manufacturer; this indicator is limited to physician\'s ownership, not the physician\'s family members\' ownership.',
        'sample_data': 'No',
        'original_data_type': 'CHAR(3)',
        'bigquery_type': 'STRING',
        'max_length': 3
    },
    {
        'field_name': 'Third_Party_Payment_Recipient_Indicator',
        'description': 'Indicates if a payment or transfer of value was paid to a third party entity or individual at the request of or on behalf of a covered recipient (physician or teaching hospital).',
        'sample_data': 'Entity',
        'original_data_type': 'VARCHAR2(50)',
        'bigquery_type': 'STRING',
        'max_length': 50
    },
    {
        'field_name': 'Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value',
        'description': 'The name of the entity that received the payment or other transfer of value.',
        'sample_data': 'EDCBA Manufacturing',
        'original_data_type': 'VARCHAR2(50)',
        'bigquery_type': 'STRING',
        'max_length': 50
    },
    {
        'field_name': 'Charity_Indicator',
        'description': 'Indicates the third party entity that received the payment or other transfer of value is a charity.',
        'sample_data': 'No',
        'original_data_type': 'CHAR(3)',
        'bigquery_type': 'STRING',
        'max_length': 3
    },
    {
        'field_name': 'Third_Party_Equals_Covered_Recipient_Indicator',
        'description': 'An indicator showing the "Third Party" that received the payment or other transfer of value is a Covered Recipient.',
        'sample_data': 'No',
        'original_data_type': 'CHAR(3)',
        'bigquery_type': 'STRING',
        'max_length': 3
    },
    {
        'field_name': 'Contextual_Information',
        'description': 'Any free String, which the reporting entity deems helpful or appropriate regarding this payment or other transfer of value.',
        'sample_data': 'Transfer made to promote the use of the product',
        'original_data_type': 'VARCHAR2(500)',
        'bigquery_type': 'STRING',
        'max_length': 500
    },
    {
        'field_name': 'Delay_in_Publication_Indicator',
        'description': 'An indicator showing if an Applicable Manufacturer/GPO is requesting a delay in the publication of a payment or other transfer of value.',
        'sample_data': 'No',
        'original_data_type': 'CHAR(3)',
        'bigquery_type': 'STRING',
        'max_length': 3
    },
    {
        'field_name': 'Record_ID',
        'description': 'System-assigned identifier to the general transaction at the time of submission.',
        'sample_data': '100000000241',
        'original_data_type': 'NUMBER(38,0)',
        'bigquery_type': 'INT64',
        'max_length': 38
    },
    {
        'field_name': 'Dispute_Status_for_Publication',
        'description': 'Indicates whether the payment or other transfer of value is disputed by the covered recipient or not.',
        'sample_data': 'Yes',
        'original_data_type': 'CHAR(3)',
        'bigquery_type': 'STRING',
        'max_length': 3
    },
    {
        'field_name': 'Related_Product_Indicator',
        'description': 'The indicator allows the applicable manufacturer or applicable GPO to select whether the payment or other transfer of value is related to one or more product(s) (drugs, devices, biologicals, or medical supplies). If the payment was not made in relation to a product, select "No". If the payment was related to one or more products, select "Yes".',
        'sample_data': 'Y',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Covered_or_Noncovered_Indicator_1',
        'description': 'Each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non-covered product per the covered product definition in the Open Payments final rule.',
        'sample_data': 'Covered',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1',
        'description': 'Each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply.',
        'sample_data': 'Drug',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Product_Category_or_Therapeutic_Area_1',
        'description': 'Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value.',
        'sample_data': 'Endocrinology',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1',
        'description': 'The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value.',
        'sample_data': 'Sample Drug 1',
        'original_data_type': 'VARCHAR2(500)',
        'bigquery_type': 'STRING',
        'max_length': 500
    },
    {
        'field_name': 'Associated_Drug_or_Biological_NDC_1',
        'description': 'The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes.',
        'sample_data': '3698-7272-61',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 12
    },
    {
        'field_name': 'Associated_Device_or_Medical_Supply_PDI_1',
        'description': 'The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021.',
        'sample_data': '00848657000260',
        'original_data_type': 'VARCHAR(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    # Repeat for products 2-5
    {
        'field_name': 'Covered_or_Noncovered_Indicator_2',
        'description': 'For each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non-covered product per the covered product definition in the Open Payments final rule.',
        'sample_data': 'Covered',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2',
        'description': 'For each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply.',
        'sample_data': 'Drug',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Product_Category_or_Therapeutic_Area_2',
        'description': 'Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value.',
        'sample_data': 'Endocrinology',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2',
        'description': 'The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value.',
        'sample_data': 'Sample Drug 2',
        'original_data_type': 'VARCHAR2(500)',
        'bigquery_type': 'STRING',
        'max_length': 500
    },
    {
        'field_name': 'Associated_Drug_or_Biological_NDC_2',
        'description': 'The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes.',
        'sample_data': '3698-7272-62',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 12
    },
    {
        'field_name': 'Associated_Device_or_Medical_Supply_PDI_2',
        'description': 'The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021.',
        'sample_data': '00848657000260',
        'original_data_type': 'VARCHAR(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Covered_or_Noncovered_Indicator_3',
        'description': 'For each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non-covered product per the covered product definition in the Open Payments final rule.',
        'sample_data': 'Covered',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3',
        'description': 'For each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply.',
        'sample_data': 'Drug',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Product_Category_or_Therapeutic_Area_3',
        'description': 'Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value.',
        'sample_data': 'Endocrinology',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3',
        'description': 'The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value.',
        'sample_data': 'Sample Drug 3',
        'original_data_type': 'VARCHAR2(500)',
        'bigquery_type': 'STRING',
        'max_length': 500
    },
    {
        'field_name': 'Associated_Drug_or_Biological_NDC_3',
        'description': 'The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes.',
        'sample_data': '36987-272-63',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 12
    },
    {
        'field_name': 'Associated_Device_or_Medical_Supply_PDI_3',
        'description': 'The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021.',
        'sample_data': '00848657000260',
        'original_data_type': 'VARCHAR(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Covered_or_Noncovered_Indicator_4',
        'description': 'For each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non-covered product per the covered product definition in the Open Payments final rule.',
        'sample_data': 'Covered',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4',
        'description': 'For each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply.',
        'sample_data': 'Biological',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Product_Category_or_Therapeutic_Area_4',
        'description': 'Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value.',
        'sample_data': 'Endocrinology',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4',
        'description': 'The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value.',
        'sample_data': 'Sample Drug 4',
        'original_data_type': 'VARCHAR2(500)',
        'bigquery_type': 'STRING',
        'max_length': 500
    },
    {
        'field_name': 'Associated_Drug_or_Biological_NDC_4',
        'description': 'The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes.',
        'sample_data': '3698-7272-64',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 12
    },
    {
        'field_name': 'Associated_Device_or_Medical_Supply_PDI_4',
        'description': 'The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021.',
        'sample_data': '00848657000260',
        'original_data_type': 'VARCHAR(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Covered_or_Noncovered_Indicator_5',
        'description': 'For each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non-covered product per the covered product definition in the Open Payments final rule.',
        'sample_data': 'Covered',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5',
        'description': 'For each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply.',
        'sample_data': 'Device',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Product_Category_or_Therapeutic_Area_5',
        'description': 'Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value.',
        'sample_data': 'Endocrinology',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5',
        'description': 'The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value.',
        'sample_data': 'Sample Drug 5',
        'original_data_type': 'VARCHAR2(500)',
        'bigquery_type': 'STRING',
        'max_length': 500
    },
    {
        'field_name': 'Associated_Drug_or_Biological_NDC_5',
        'description': 'The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes.',
        'sample_data': '36987-272-65',
        'original_data_type': 'VARCHAR2(100)',
        'bigquery_type': 'STRING',
        'max_length': 12
    },
    {
        'field_name': 'Associated_Device_or_Medical_Supply_PDI_5',
        'description': 'The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021.',
        'sample_data': '00848657000260',
        'original_data_type': 'VARCHAR(100)',
        'bigquery_type': 'STRING',
        'max_length': 100
    },
    {
        'field_name': 'Program_Year',
        'description': 'The year in which the payment occurred, as reported by submitting entity.',
        'sample_data': '2016',
        'original_data_type': 'CHAR(4)',
        'bigquery_type': 'STRING',
        'max_length': 4
    },
    {
        'field_name': 'Payment_Publication_Date',
        'description': 'The predefined date when the payment or other transfer of value is scheduled to be published.',
        'sample_data': '06/30/2017',
        'original_data_type': 'DATE',
        'bigquery_type': 'DATE',
        'max_length': 12
    }
]

# Save the schema to JSON
with open('/home/incent/conflixis-analytics/projects/002-analytic-agent/general_payments_schema.json', 'w') as f:
    json.dump(general_payments_fields, f, indent=2)

# Create BigQuery schema definition
bigquery_schema = []
for field in general_payments_fields:
    bq_field = {
        'name': field['field_name'],
        'type': field['bigquery_type'],
        'mode': 'NULLABLE',
        'description': field['description']
    }
    bigquery_schema.append(bq_field)

# Save BigQuery schema
with open('/home/incent/conflixis-analytics/projects/002-analytic-agent/general_payments_bigquery_schema.json', 'w') as f:
    json.dump(bigquery_schema, f, indent=2)

# Create a summary report
print(f"Total fields extracted: {len(general_payments_fields)}")
print("\nField Summary by Type:")
type_counts = {}
for field in general_payments_fields:
    bq_type = field['bigquery_type']
    type_counts[bq_type] = type_counts.get(bq_type, 0) + 1

for bq_type, count in sorted(type_counts.items()):
    print(f"  {bq_type}: {count} fields")

print("\nSchema files created:")
print("  - general_payments_schema.json (detailed field information)")
print("  - general_payments_bigquery_schema.json (BigQuery schema definition)")

# Create a markdown summary
with open('/home/incent/conflixis-analytics/projects/002-analytic-agent/general_payments_field_summary.md', 'w') as f:
    f.write("# Open Payments General Payments Table Schema\n\n")
    f.write(f"Total fields: {len(general_payments_fields)}\n\n")
    f.write("## Field Summary\n\n")
    f.write("| Field Name | BigQuery Type | Description |\n")
    f.write("|------------|---------------|-------------|\n")
    
    for field in general_payments_fields:
        desc = field['description'].replace('\n', ' ')
        if len(desc) > 100:
            desc = desc[:97] + '...'
        f.write(f"| {field['field_name']} | {field['bigquery_type']} | {desc} |\n")

print("\nMarkdown summary created: general_payments_field_summary.md")