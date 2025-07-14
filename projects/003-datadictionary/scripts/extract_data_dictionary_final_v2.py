#!/usr/bin/env python3
"""
Final version of PDF data dictionary extractor specifically designed for the
Open Payments PDF table structure. This version properly handles the complex
multi-line format where data appears in a specific pattern.
"""

import pypdf
import re
from pathlib import Path
import json
from typing import List, Dict, Optional, Tuple
import sys

def extract_pdf_text(pdf_path: Path) -> str:
    """Extract text from PDF file."""
    text = ""
    with open(pdf_path, 'rb') as file:
        reader = pypdf.PdfReader(file)
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            page_text = page.extract_text()
            # Clean up common unicode issues
            page_text = page_text.replace('\u2013', '-')
            page_text = page_text.replace('\u2014', '-')
            page_text = page_text.replace('\u201c', '"')
            page_text = page_text.replace('\u201d', '"')
            page_text = page_text.replace('\u2018', "'")
            page_text = page_text.replace('\u2019', "'")
            text += page_text
    return text

def find_table_sections(text: str) -> Dict[str, Dict]:
    """Find and extract the three main table sections."""
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
        elif re.search(r'Table\s+D-1.*Research\s+Payment.*File\s+Attributes', line, re.IGNORECASE):
            sections['research_payments']['start'] = i
        elif re.search(r'Table\s+F-1.*Physician\s+Ownership.*File\s+Attributes', line, re.IGNORECASE):
            sections['ownership']['start'] = i
    
    # Find section ends
    for section_name, section_data in sections.items():
        if section_data['start'] != -1:
            start_idx = section_data['start']
            end_idx = len(lines)
            
            # Look for the next major section
            for i in range(start_idx + 10, len(lines)):
                if re.search(r'(Table\s+[A-Z]-\d|Appendix\s+[A-Z]:)', lines[i]):
                    # Verify it's not the current table continued
                    if section_name == 'general_payments' and 'Table B-' in lines[i]:
                        continue
                    elif section_name == 'research_payments' and 'Table D-' in lines[i]:
                        continue
                    elif section_name == 'ownership' and 'Table F-' in lines[i]:
                        continue
                    end_idx = i
                    break
            
            section_data['end'] = end_idx
            section_data['text'] = lines[start_idx:end_idx]
    
    return sections

def is_valid_field_name(text: str) -> bool:
    """Determine if a text string is likely a field name."""
    if not text or len(text) < 2:
        return False
    
    # Skip common false positives
    false_positives = [
        'NEW', 'ADDED', 'CHANGED', 'UNCHANGED',
        'Field', 'Name', 'Description', 'Sample', 'Data', 'Type', 'Format', 'Max', 'Length',
        'Table', 'Appendix', 'Open', 'Payments', 'Methodology', 'Expiration', 'Date',
        'Page', 'Hospital', 'Manufacturing', 'United', 'States', 'Attribute',
        'Physician', 'Assistant', 'Osteopathic', 'Physicians', 'Gynecology',
        'VARCHAR', 'NUMBER', 'CHAR', 'DATE', 'TIMESTAMP', 'CLOB',
        'OMB', 'Control', 'No', 'US', 'MD', 'VA', 'CA', 'PA', 'MI', 'WI', 'MA',
        'Dentistry', 'YYYY', 'MM', 'DD', 'PY', 'CMS', 'NDC', 'PDI'
    ]
    
    first_word = text.split()[0] if text.split() else ''
    if first_word.upper() in false_positives:
        return False
    
    # Known field name patterns
    field_patterns = [
        r'^Change_Type$',
        r'^.*_Recipient_.*',
        r'^Teaching_Hospital_.*',
        r'^Physician_.*',
        r'^Recipient_.*',
        r'^Submitting_.*',
        r'^Applicable_.*',
        r'^Total_Amount.*',
        r'^Date_of_.*',
        r'^Number_of_.*',
        r'^Form_of_.*',
        r'^Nature_of_.*',
        r'^.*_of_Travel$',
        r'^Name_of_.*',
        r'^Third_Party_.*',
        r'^Contextual_.*',
        r'^Delay_in_.*',
        r'^Record_ID$',
        r'^Dispute_Status.*',
        r'^.*_Product_.*',
        r'^.*_or_Noncovered_.*',
        r'^Indicate_Drug.*',
        r'^Product_Category.*',
        r'^Associated_.*',
        r'^Program_Year$',
        r'^Payment_Publication_Date$',
        r'^Principal_Investigator_.*',
        r'^Research_.*',
        r'^ClinicalTrials_.*',
        r'^Preclinical_.*',
        r'^Interest_.*',
        r'^Value_of_.*',
        r'^Terms_of_.*',
        r'^.*_Indicator$',
        r'^.*_NDC_.*',
        r'^.*_PDI_.*',
        r'^.*_Supply.*'
    ]
    
    for pattern in field_patterns:
        if re.match(pattern, first_word, re.IGNORECASE):
            return True
    
    # General pattern: starts with uppercase, contains underscores
    if re.match(r'^[A-Z][a-zA-Z0-9_]+$', first_word) and '_' in first_word:
        return True
    
    return False

def extract_field_from_lines(lines: List[str], start_idx: int) -> Tuple[Dict, int]:
    """Extract a complete field starting from the given index."""
    field = {
        'name': '',
        'description': '',
        'sample_data': '',
        'data_type': '',
        'format': '',
        'max_length': ''
    }
    
    # Get the field name from the first line
    first_line = lines[start_idx].strip()
    words = first_line.split()
    if not words or not is_valid_field_name(words[0]):
        return None, start_idx + 1
    
    field['name'] = words[0]
    
    # Start collecting the description
    description_parts = [' '.join(words[1:])] if len(words) > 1 else []
    
    # Look for the complete field information
    i = start_idx + 1
    found_data_type = False
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Skip empty lines and headers
        if not line or any(skip in line for skip in ['Open Payments Methodology', 'Expiration Date:', 'OMB Control']):
            i += 1
            continue
        
        # Check if this is the start of a new field
        line_words = line.split()
        if line_words and is_valid_field_name(line_words[0]):
            break
        
        # Look for data type patterns in the current line
        # Match patterns like "Sample Data VARCHAR2(50) string 50"
        type_match = re.search(
            r'(.+?)\s+(VARCHAR2?\s*\(\s*\d+\s*\)|NUMBER\s*\(\s*\d+\s*,\s*\d+\s*\)|CHAR\s*\(\s*\d+\s*\)|DATE|TIMESTAMP|CLOB)\s+(\S+)\s+(\S+)',
            line,
            re.IGNORECASE
        )
        
        if type_match:
            # Found a line with sample data and type info
            field['sample_data'] = type_match.group(1).strip()
            field['data_type'] = re.sub(r'\s+', '', type_match.group(2).upper())
            field['format'] = type_match.group(3)
            field['max_length'] = type_match.group(4)
            found_data_type = True
            i += 1
            break
        
        # Check if line contains just data type info (sometimes split across lines)
        simple_type_match = re.match(
            r'^(VARCHAR2?\s*\(\s*\d+\s*\)|NUMBER\s*\(\s*\d+\s*,\s*\d+\s*\)|CHAR\s*\(\s*\d+\s*\)|DATE|TIMESTAMP|CLOB)\s*(.*)$',
            line,
            re.IGNORECASE
        )
        
        if simple_type_match:
            field['data_type'] = re.sub(r'\s+', '', simple_type_match.group(1).upper())
            rest = simple_type_match.group(2).strip()
            if rest:
                parts = rest.split()
                if parts:
                    field['format'] = parts[0] if len(parts) > 0 else ''
                    field['max_length'] = parts[1] if len(parts) > 1 else ''
            found_data_type = True
            i += 1
            break
        
        # Otherwise, it's part of the description
        description_parts.append(line)
        i += 1
    
    # Combine description
    field['description'] = ' '.join(description_parts).strip()
    
    # Clean up data type (handle VARCHAR 2 split)
    field['data_type'] = field['data_type'].replace('VARCHAR2', 'VARCHAR2')
    
    # Extract max length from data type if not already set
    if not field['max_length'] and field['data_type']:
        length_match = re.search(r'\((\d+)(?:,\d+)?\)', field['data_type'])
        if length_match:
            field['max_length'] = length_match.group(1)
    
    return field, i

def parse_table_fields_v2(section_lines: List[str]) -> List[Dict]:
    """Parse fields from the table section with improved logic."""
    fields = []
    
    # Find where the actual data starts
    data_start = 0
    for i, line in enumerate(section_lines):
        if 'Field Name' in line and 'Field Description' in line and 'Data Type' in line:
            data_start = i + 1
            # Skip separator lines
            while data_start < len(section_lines) and (
                not section_lines[data_start].strip() or 
                section_lines[data_start].strip().startswith('-')
            ):
                data_start += 1
            break
    
    # Process fields
    i = data_start
    while i < len(section_lines):
        field, next_idx = extract_field_from_lines(section_lines, i)
        if field:
            fields.append(field)
        i = next_idx
    
    return fields

def create_markdown_table(fields: List[Dict], table_name: str) -> str:
    """Create a clean markdown table from parsed fields."""
    markdown = f"# {table_name} Data Dictionary\n\n"
    markdown += f"This data dictionary describes the fields in the {table_name} table.\n\n"
    
    if not fields:
        markdown += "*No fields were extracted. Please check the PDF structure.*\n"
        return markdown
    
    markdown += f"**Total Fields:** {len(fields)}\n\n"
    
    # Create the table
    markdown += "| Field Name | Description | Sample Data | Data Type | Format | Max Length |\n"
    markdown += "|------------|-------------|-------------|-----------|---------|------------|\n"
    
    for field in fields:
        name = field['name']
        desc = field['description'].replace('|', '\\|').strip()
        sample = field['sample_data'].replace('|', '\\|') if field['sample_data'] else '-'
        dtype = field['data_type'] if field['data_type'] else '-'
        fmt = field['format'] if field['format'] else '-'
        maxlen = field['max_length'] if field['max_length'] else '-'
        
        # Truncate very long descriptions
        if len(desc) > 200:
            desc = desc[:197] + '...'
        
        # Clean sample data
        if len(sample) > 50:
            sample = sample[:47] + '...'
        
        markdown += f"| {name} | {desc} | {sample} | {dtype} | {fmt} | {maxlen} |\n"
    
    # Add summary
    markdown += "\n## Summary Statistics\n\n"
    
    # Count data types
    type_counts = {}
    for field in fields:
        if field['data_type']:
            base_type = field['data_type'].split('(')[0]
            type_counts[base_type] = type_counts.get(base_type, 0) + 1
        else:
            type_counts['Not specified'] = type_counts.get('Not specified', 0) + 1
    
    markdown += "### Data Types\n\n"
    for dtype, count in sorted(type_counts.items()):
        markdown += f"- **{dtype}**: {count} fields\n"
    
    # Add field name patterns
    markdown += "\n### Field Categories\n\n"
    categories = {
        'Covered Recipient': 0,
        'Teaching Hospital': 0,
        'Payment': 0,
        'Product/Drug': 0,
        'Travel': 0,
        'Third Party': 0,
        'Other': 0
    }
    
    for field in fields:
        name = field['name'].lower()
        if 'covered_recipient' in name:
            categories['Covered Recipient'] += 1
        elif 'teaching_hospital' in name:
            categories['Teaching Hospital'] += 1
        elif 'payment' in name or 'amount' in name:
            categories['Payment'] += 1
        elif 'drug' in name or 'product' in name or 'device' in name or 'ndc' in name or 'pdi' in name:
            categories['Product/Drug'] += 1
        elif 'travel' in name:
            categories['Travel'] += 1
        elif 'third_party' in name:
            categories['Third Party'] += 1
        else:
            categories['Other'] += 1
    
    for cat, count in categories.items():
        if count > 0:
            markdown += f"- **{cat}**: {count} fields\n"
    
    return markdown

def main():
    """Main function to extract and process the PDF."""
    pdf_path = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/data dictionary/open_payments_data_dictionary_methodology-january_2025.pdf")
    output_dir = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/data_dictionaries")
    temp_dir = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/temp")
    
    output_dir.mkdir(exist_ok=True)
    temp_dir.mkdir(exist_ok=True)
    
    print("="*70)
    print("Open Payments Data Dictionary Extractor - Final Version")
    print("="*70)
    
    print(f"\nExtracting text from PDF...")
    full_text = extract_pdf_text(pdf_path)
    print(f"✓ Extracted {len(full_text):,} characters")
    
    # Find table sections
    print("\nLocating table sections...")
    sections = find_table_sections(full_text)
    
    # Table names mapping
    tables = {
        'general_payments': 'General Payments',
        'research_payments': 'Research Payments',
        'ownership': 'Physician Ownership and Investment Interest'
    }
    
    # Process each table
    results = {}
    
    for section_key, table_name in tables.items():
        print(f"\n{'='*70}")
        print(f"Processing: {table_name}")
        print(f"{'='*70}")
        
        if sections[section_key]['start'] != -1:
            section_lines = sections[section_key]['text']
            print(f"✓ Section found: lines {sections[section_key]['start']} to {sections[section_key]['end']}")
            
            # Parse fields
            fields = parse_table_fields_v2(section_lines)
            print(f"✓ Extracted {len(fields)} fields")
            
            # Save JSON for debugging
            json_file = temp_dir / f'{section_key}_fields_final_v2.json'
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(fields, f, indent=2, ensure_ascii=False)
            
            # Create markdown
            markdown = create_markdown_table(fields, table_name)
            
            # Save markdown
            md_file = output_dir / f'{section_key}_data_dictionary.md'
            with open(md_file, 'w', encoding='utf-8') as f:
                f.write(markdown)
            print(f"✓ Created: {md_file}")
            
            # Show preview
            if fields and len(fields) >= 3:
                print("\nPreview (first 3 fields):")
                for i, field in enumerate(fields[:3]):
                    print(f"  {i+1}. {field['name']}")
                    print(f"     Type: {field['data_type'] if field['data_type'] else 'Not specified'}")
                    print(f"     Sample: {field['sample_data'][:50] + '...' if len(field['sample_data']) > 50 else field['sample_data'] if field['sample_data'] else 'N/A'}")
            
            results[section_key] = len(fields)
        else:
            print("✗ Section not found in PDF")
            results[section_key] = 0
    
    # Final summary
    print(f"\n{'='*70}")
    print("EXTRACTION COMPLETE")
    print(f"{'='*70}")
    
    total_fields = sum(results.values())
    print(f"\nTotal fields extracted: {total_fields}")
    print("\nBreakdown by table:")
    for section_key, count in results.items():
        status = "✓" if count > 0 else "✗"
        print(f"  {status} {tables[section_key]}: {count} fields")
    
    print(f"\nOutput files saved to: {output_dir}")
    
    # Show any tables with low field counts as a warning
    low_count_tables = [name for name, count in results.items() if 0 < count < 20]
    if low_count_tables:
        print("\n⚠️  Warning: The following tables have fewer than 20 fields:")
        for table in low_count_tables:
            print(f"   - {tables[table]}: {results[table]} fields")
        print("   This may indicate parsing issues. Please review the output.")

if __name__ == "__main__":
    main()