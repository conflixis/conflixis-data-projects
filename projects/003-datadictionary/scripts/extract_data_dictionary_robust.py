#!/usr/bin/env python3
"""
Robust PDF data dictionary extractor that properly handles the complex
multi-line table format in the Open Payments documentation.
"""

import pypdf
import re
from pathlib import Path
import json
from typing import List, Dict, Optional, Tuple
import unicodedata

def normalize_text(text: str) -> str:
    """Normalize unicode characters and clean up text."""
    # Replace common problematic unicode characters
    text = text.replace('\u2013', '-')  # en dash
    text = text.replace('\u2014', '-')  # em dash
    text = text.replace('\u201c', '"')  # left double quote
    text = text.replace('\u201d', '"')  # right double quote
    text = text.replace('\u2018', "'")  # left single quote
    text = text.replace('\u2019', "'")  # right single quote
    return text

def extract_pdf_text(pdf_path: Path) -> str:
    """Extract and normalize text from PDF file."""
    text = ""
    with open(pdf_path, 'rb') as file:
        reader = pypdf.PdfReader(file)
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            page_text = page.extract_text()
            text += normalize_text(page_text)
    return text

def find_table_sections(text: str) -> Dict[str, Dict]:
    """Find and extract sections for all three tables."""
    lines = text.split('\n')
    
    sections = {
        'general_payments': {'start': -1, 'end': -1, 'text': []},
        'research_payments': {'start': -1, 'end': -1, 'text': []},
        'ownership': {'start': -1, 'end': -1, 'text': []}
    }
    
    # Find table starts
    for i, line in enumerate(lines):
        if re.search(r'Table\s+B-1.*General\s+Payment.*File\s+Attributes', line, re.IGNORECASE):
            sections['general_payments']['start'] = i
            print(f"Found General Payments table at line {i}")
        elif re.search(r'Table\s+D-1.*Research\s+Payment.*File\s+Attributes', line, re.IGNORECASE):
            sections['research_payments']['start'] = i
            print(f"Found Research Payments table at line {i}")
        elif re.search(r'Table\s+F-1.*Physician\s+Ownership.*File\s+Attributes', line, re.IGNORECASE):
            sections['ownership']['start'] = i
            print(f"Found Ownership table at line {i}")
    
    # Find section ends
    for section_name, section_data in sections.items():
        if section_data['start'] != -1:
            start_idx = section_data['start']
            end_idx = len(lines)
            
            # Look for next section or appendix
            for i in range(start_idx + 10, len(lines)):
                # Check for next table or appendix
                if re.search(r'(Table\s+[A-Z]-\d|Appendix\s+[A-Z]:)', lines[i]):
                    # Make sure it's not the current table
                    current_table_refs = {
                        'general_payments': ['B-1', 'General Payment'],
                        'research_payments': ['D-1', 'Research Payment'],
                        'ownership': ['F-1', 'Physician Ownership']
                    }
                    
                    is_current = False
                    for ref in current_table_refs.get(section_name, []):
                        if ref in lines[i]:
                            is_current = True
                            break
                    
                    if not is_current:
                        end_idx = i
                        break
            
            section_data['end'] = end_idx
            section_data['text'] = lines[start_idx:end_idx]
    
    return sections

def is_valid_field_name(text: str) -> bool:
    """Check if text is likely a valid field name."""
    if not text or len(text) < 2:
        return False
    
    # List of known field name patterns
    field_patterns = [
        r'^Change_Type$',
        r'^Covered_Recipient_',
        r'^Teaching_Hospital_',
        r'^Physician_',
        r'^Recipient_',
        r'^Submitting_',
        r'^Applicable_',
        r'^Total_Amount',
        r'^Date_of_',
        r'^Number_of_',
        r'^Form_of_',
        r'^Nature_of_',
        r'^City_of_',
        r'^State_of_',
        r'^Country_of_',
        r'^Name_of_',
        r'^Third_Party_',
        r'^Contextual_',
        r'^Delay_in_',
        r'^Record_ID$',
        r'^Dispute_Status',
        r'^Related_Product',
        r'^Covered_or_',
        r'^Indicate_Drug',
        r'^Product_Category',
        r'^Associated_',
        r'^Program_Year$',
        r'^Payment_Publication_Date$',
        r'^Principal_Investigator_',
        r'^Research_',
        r'^ClinicalTrials_',
        r'^Preclinical_',
        r'^Interest_',
        r'^Value_of_',
        r'^Terms_of_'
    ]
    
    # Check against known patterns
    for pattern in field_patterns:
        if re.match(pattern, text, re.IGNORECASE):
            return True
    
    # General pattern for field names (CamelCase or snake_case)
    if re.match(r'^[A-Z][a-zA-Z0-9_]*$', text):
        # Avoid false positives
        false_positives = ['NEW', 'ADDED', 'CHANGED', 'UNCHANGED', 'Table', 'Field', 
                          'Name', 'Description', 'Sample', 'Data', 'Type', 'Format', 
                          'Max', 'Length', 'Attribute', 'Appendix', 'Open', 'Payments',
                          'Page', 'PY', 'US', 'MD', 'CA', 'VA', 'MI', 'PA', 'WI',
                          'CMS', 'NDC', 'Hospital', 'Manufacturing', 'United', 'States',
                          'Osteopathic', 'Physicians', 'Gynecology', 'Assistant']
        if text in false_positives:
            return False
        return True
    
    return False

def parse_data_type_line(line: str) -> Dict[str, str]:
    """Parse a line that contains data type information."""
    result = {
        'sample_data': '',
        'data_type': '',
        'format': '',
        'max_length': ''
    }
    
    # Common patterns for data types
    data_type_pattern = r'(VARCHAR2?\s*\(\s*\d+\s*\)|NUMBER\s*\(\s*\d+\s*,\s*\d+\s*\)|CHAR\s*\(\s*\d+\s*\)|DATE|TIMESTAMP|CLOB)'
    
    # Try to find data type in the line
    match = re.search(data_type_pattern, line, re.IGNORECASE)
    if match:
        data_type = match.group(1)
        # Clean up data type
        data_type = re.sub(r'\s+', '', data_type.upper())
        data_type = data_type.replace('VARCHAR2', 'VARCHAR2')
        result['data_type'] = data_type
        
        # Everything before data type is likely sample data
        before_type = line[:match.start()].strip()
        if before_type:
            result['sample_data'] = before_type
        
        # Everything after data type
        after_type = line[match.end():].strip()
        parts = after_type.split()
        if parts:
            result['format'] = parts[0] if len(parts) > 0 else ''
            result['max_length'] = parts[1] if len(parts) > 1 else ''
    
    return result

def parse_table_robust(section_lines: List[str]) -> List[Dict]:
    """Robust parser for table fields."""
    fields = []
    
    # Find where the actual data starts (after headers)
    data_start = 0
    for i, line in enumerate(section_lines):
        if 'Field Name' in line and 'Field Description' in line and 'Data Type' in line:
            data_start = i + 1
            # Skip any separator lines
            while data_start < len(section_lines) and (
                not section_lines[data_start].strip() or 
                section_lines[data_start].strip().startswith('-')
            ):
                data_start += 1
            break
    
    i = data_start
    current_field = None
    
    while i < len(section_lines):
        line = section_lines[i].strip()
        
        # Skip empty lines and headers
        if not line or any(skip in line for skip in ['Open Payments Methodology', 'Expiration Date:', 'OMB Control']):
            i += 1
            continue
        
        # Check for field name at start of line
        words = line.split()
        if words and is_valid_field_name(words[0]):
            # Save previous field
            if current_field and current_field.get('name'):
                fields.append(current_field)
            
            # Start new field
            field_name = words[0]
            rest_of_line = ' '.join(words[1:]) if len(words) > 1 else ''
            
            current_field = {
                'name': field_name,
                'description': rest_of_line,
                'sample_data': '',
                'data_type': '',
                'format': '',
                'max_length': ''
            }
            
            # Collect complete field information
            j = i + 1
            description_parts = [rest_of_line] if rest_of_line else []
            
            # Look for the rest of the field information
            while j < len(section_lines):
                next_line = section_lines[j].strip()
                
                if not next_line:
                    j += 1
                    continue
                
                # Stop if we hit another field name
                next_words = next_line.split()
                if next_words and is_valid_field_name(next_words[0]):
                    break
                
                # Check if this line contains data type info
                if re.search(r'VARCHAR|NUMBER|CHAR|DATE|TIMESTAMP|CLOB', next_line, re.IGNORECASE):
                    # Parse the data type line
                    type_info = parse_data_type_line(next_line)
                    if type_info['data_type']:
                        # If we found data type info, update field
                        current_field.update(type_info)
                        j += 1
                        break
                    else:
                        # Otherwise treat as description
                        description_parts.append(next_line)
                else:
                    # Regular description line
                    description_parts.append(next_line)
                
                j += 1
            
            # Combine description
            current_field['description'] = ' '.join(description_parts).strip()
            
            # Move to next potential field
            i = j
        else:
            i += 1
    
    # Don't forget last field
    if current_field and current_field.get('name'):
        fields.append(current_field)
    
    return fields

def create_clean_markdown(fields: List[Dict], table_name: str) -> str:
    """Create a clean markdown table from field definitions."""
    markdown = f"# {table_name} Data Dictionary\n\n"
    markdown += f"This data dictionary describes the fields in the {table_name} table.\n\n"
    
    if not fields:
        markdown += "*No fields were extracted. Please check the PDF structure.*\n"
        return markdown
    
    markdown += f"**Total Fields:** {len(fields)}\n\n"
    
    # Create table
    markdown += "| Field Name | Description | Sample Data | Data Type | Format | Max Length |\n"
    markdown += "|------------|-------------|-------------|-----------|---------|------------|\n"
    
    for field in fields:
        name = field['name']
        description = field['description'].replace('|', '\\|').strip()
        sample_data = field['sample_data'].replace('|', '\\|') if field['sample_data'] else '-'
        data_type = field['data_type'] if field['data_type'] else '-'
        format_info = field['format'] if field['format'] else '-'
        max_length = field['max_length'] if field['max_length'] else '-'
        
        # Clean up values
        if len(description) > 300:
            description = description[:297] + '...'
        
        # Extract numeric max length from data type if not already set
        if max_length == '-' and data_type != '-':
            length_match = re.search(r'\((\d+)\)', data_type)
            if length_match:
                max_length = length_match.group(1)
        
        markdown += f"| {name} | {description} | {sample_data} | {data_type} | {format_info} | {max_length} |\n"
    
    # Add summary
    markdown += "\n## Field Summary\n\n"
    
    # Count data types
    data_type_counts = {}
    for field in fields:
        if field['data_type']:
            dt_base = field['data_type'].split('(')[0]
            data_type_counts[dt_base] = data_type_counts.get(dt_base, 0) + 1
        else:
            data_type_counts['Not specified'] = data_type_counts.get('Not specified', 0) + 1
    
    markdown += "### Data Types Distribution\n\n"
    for dt, count in sorted(data_type_counts.items()):
        markdown += f"- {dt}: {count} fields\n"
    
    return markdown

def main():
    """Main function to orchestrate the extraction."""
    pdf_path = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/data dictionary/open_payments_data_dictionary_methodology-january_2025.pdf")
    output_dir = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/data_dictionaries")
    temp_dir = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/temp")
    
    output_dir.mkdir(exist_ok=True)
    temp_dir.mkdir(exist_ok=True)
    
    print("="*60)
    print("PDF Data Dictionary Extractor - Robust Version")
    print("="*60)
    print(f"\nExtracting text from: {pdf_path}")
    
    full_text = extract_pdf_text(pdf_path)
    print(f"Extracted {len(full_text)} characters")
    
    # Find table sections
    print("\nSearching for table sections...")
    sections = find_table_sections(full_text)
    
    # Process each table
    tables = {
        'general_payments': 'General Payments',
        'research_payments': 'Research Payments', 
        'ownership': 'Physician Ownership and Investment Interest'
    }
    
    results = {}
    
    for section_key, table_name in tables.items():
        print(f"\n{'='*60}")
        print(f"Processing: {table_name}")
        print(f"{'='*60}")
        
        if sections[section_key]['start'] != -1:
            section_lines = sections[section_key]['text']
            print(f"Section spans lines {sections[section_key]['start']} to {sections[section_key]['end']}")
            print(f"Total lines: {len(section_lines)}")
            
            # Parse fields
            fields = parse_table_robust(section_lines)
            print(f"Extracted {len(fields)} fields")
            
            # Save JSON for debugging
            json_path = temp_dir / f'{section_key}_fields_robust.json'
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(fields, f, indent=2, ensure_ascii=False)
            
            # Create markdown
            markdown = create_clean_markdown(fields, table_name)
            
            # Save markdown
            md_path = output_dir / f'{section_key}_data_dictionary.md'
            with open(md_path, 'w', encoding='utf-8') as f:
                f.write(markdown)
            
            print(f"✓ Created: {md_path}")
            
            # Show sample fields
            if fields and len(fields) >= 3:
                print("\nFirst 3 fields:")
                for i, field in enumerate(fields[:3]):
                    dt = field['data_type'] if field['data_type'] else 'Not specified'
                    print(f"  {i+1}. {field['name']} ({dt})")
            
            results[section_key] = len(fields)
        else:
            print("✗ Section not found in PDF")
            results[section_key] = 0
    
    # Final summary
    print(f"\n{'='*60}")
    print("EXTRACTION COMPLETE")
    print(f"{'='*60}")
    for section_key, count in results.items():
        status = "✓" if count > 0 else "✗"
        print(f"{status} {tables[section_key]}: {count} fields")
    print(f"\nOutput directory: {output_dir}")

if __name__ == "__main__":
    main()