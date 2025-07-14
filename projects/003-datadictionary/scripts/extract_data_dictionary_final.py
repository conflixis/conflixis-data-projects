#!/usr/bin/env python3
"""
Final improved script to extract data dictionary information from Open Payments PDF.
This version properly handles the complex multi-line table format where field information
spans multiple lines and data type information may be split across lines.
"""

import pypdf
import re
from pathlib import Path
import json
from typing import List, Dict, Optional, Tuple

def extract_pdf_text(pdf_path: Path) -> str:
    """Extract text from PDF file."""
    text = ""
    with open(pdf_path, 'rb') as file:
        reader = pypdf.PdfReader(file)
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            text += page.extract_text()
    return text

def find_table_sections(text: str) -> Dict[str, Dict]:
    """Find and extract sections for all three tables."""
    lines = text.split('\n')
    
    sections = {
        'general_payments': {'start': -1, 'end': -1, 'text': []},
        'research_payments': {'start': -1, 'end': -1, 'text': []},
        'ownership': {'start': -1, 'end': -1, 'text': []}
    }
    
    # Find section starts
    for i, line in enumerate(lines):
        if re.search(r'Table\s+B-1.*General\s+Payment.*File\s+Attributes', line, re.IGNORECASE):
            sections['general_payments']['start'] = i
            print(f"Found General Payments table start at line {i}: {line}")
        elif re.search(r'Table\s+D-1.*Research\s+Payment.*File\s+Attributes', line, re.IGNORECASE):
            sections['research_payments']['start'] = i
            print(f"Found Research Payments table start at line {i}: {line}")
        elif re.search(r'Table\s+F-1.*Physician\s+Ownership.*File\s+Attributes', line, re.IGNORECASE):
            sections['ownership']['start'] = i
            print(f"Found Physician Ownership table start at line {i}: {line}")
    
    # Find section ends
    for section_name, section_data in sections.items():
        if section_data['start'] != -1:
            start_idx = section_data['start']
            # Look for the end of this section
            for i in range(start_idx + 1, len(lines)):
                # Check for next table or appendix
                if re.search(r'Table\s+[A-Z]-\d|Appendix\s+[A-Z]:', lines[i], re.IGNORECASE):
                    # Make sure it's not part of the current table
                    if not any(keyword in lines[i].lower() for keyword in section_name.split('_')):
                        section_data['end'] = i
                        break
            
            # If no end found, use the full remaining text
            if section_data['end'] == -1:
                section_data['end'] = min(start_idx + 2000, len(lines))  # Limit to prevent huge sections
            
            # Extract text for this section
            section_data['text'] = lines[start_idx:section_data['end']]
    
    return sections

def clean_text(text: str) -> str:
    """Clean text by removing extra spaces and newlines."""
    return ' '.join(text.split())

def is_field_name(text: str) -> bool:
    """Check if a line likely contains a field name."""
    # Field names are typically PascalCase or snake_case
    # They don't contain common words found in descriptions
    if not text or len(text) < 2:
        return False
    
    # Skip header rows and common description words
    skip_patterns = [
        r'^Field Name',
        r'^Field Description',
        r'^Data Type',
        r'^Sample Data',
        r'^Max Length',
        r'^\d+$',
        r'^Page \d+',
        r'Open Payments Methodology',
        r'Expiration Date:',
        r'OMB Control'
    ]
    
    for pattern in skip_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return False
    
    # Field names typically start with uppercase letter or are all caps with underscores
    field_patterns = [
        r'^[A-Z][a-zA-Z0-9_]*(?:_[A-Za-z0-9]+)*$',  # PascalCase or snake_case
        r'^[A-Z]+(?:_[A-Z]+)*$',  # ALL_CAPS_SNAKE_CASE
    ]
    
    for pattern in field_patterns:
        if re.match(pattern, text.split()[0] if text.split() else ''):
            return True
    
    return False

def parse_table_fields_improved(section_lines: List[str]) -> List[Dict]:
    """Improved parser that handles the complex multi-line table format."""
    fields = []
    
    # Skip header lines and find where actual field data starts
    data_start = 0
    for i, line in enumerate(section_lines):
        if re.search(r'Field\s+Name.*Field\s+Description.*Data\s+Type', line, re.IGNORECASE):
            # Skip the header line and any separator lines
            data_start = i + 1
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
        
        # Skip empty lines and page headers/footers
        if not line or any(skip in line for skip in ['Open Payments Methodology', 'Expiration Date:', 'OMB Control']):
            i += 1
            continue
        
        # Check if this line starts with a field name
        if is_field_name(line):
            # Save previous field if exists
            if current_field and current_field.get('name'):
                fields.append(current_field)
            
            # Extract field name and start of description
            parts = line.split(None, 1)
            field_name = parts[0]
            
            current_field = {
                'name': field_name,
                'description': parts[1] if len(parts) > 1 else '',
                'sample_data': '',
                'data_type': '',
                'format': '',
                'max_length': ''
            }
            
            # Collect the complete field information
            j = i + 1
            description_lines = [current_field['description']] if current_field['description'] else []
            found_data_type = False
            
            while j < len(section_lines) and not is_field_name(section_lines[j].strip()):
                next_line = section_lines[j].strip()
                
                if not next_line or any(skip in next_line for skip in ['Open Payments Methodology', 'Expiration Date:', 'OMB Control']):
                    j += 1
                    continue
                
                # Look for lines containing data type information
                if re.search(r'VARCHAR|NUMBER|CHAR|DATE|TIMESTAMP|CLOB', next_line, re.IGNORECASE):
                    # This line contains sample data and data type info
                    # Split by common data type patterns
                    data_type_match = re.search(
                        r'(.*?)\s*(VARCHAR2?\s*\(\s*\d+\s*\)|NUMBER\s*\(\s*\d+\s*,\s*\d+\s*\)|CHAR\s*\(\s*\d+\s*\)|DATE|TIMESTAMP|CLOB)\s*(.*)',
                        next_line,
                        re.IGNORECASE
                    )
                    
                    if data_type_match:
                        sample_part = data_type_match.group(1).strip()
                        data_type_part = data_type_match.group(2).strip()
                        rest_part = data_type_match.group(3).strip()
                        
                        # Clean up VARCHAR 2 split across lines
                        data_type_part = re.sub(r'VARCHAR\s+2', 'VARCHAR2', data_type_part, flags=re.IGNORECASE)
                        
                        current_field['sample_data'] = sample_part
                        current_field['data_type'] = data_type_part
                        
                        # Parse format and max length from rest
                        rest_parts = rest_part.split()
                        if rest_parts:
                            current_field['format'] = rest_parts[0]
                            if len(rest_parts) > 1:
                                current_field['max_length'] = rest_parts[1]
                        
                        found_data_type = True
                    else:
                        # Sometimes the data type is on its own line
                        if re.match(r'^(VARCHAR2?\s*\(\s*\d+\s*\)|NUMBER\s*\(\s*\d+\s*,\s*\d+\s*\)|CHAR\s*\(\s*\d+\s*\)|DATE|TIMESTAMP|CLOB)', next_line, re.IGNORECASE):
                            # Previous line might have sample data
                            if j > i + 1 and description_lines:
                                last_desc_line = description_lines[-1]
                                # Check if last line might be sample data
                                if len(last_desc_line) < 50 and not last_desc_line.endswith('.'):
                                    current_field['sample_data'] = last_desc_line
                                    description_lines = description_lines[:-1]
                            
                            parts = next_line.split()
                            current_field['data_type'] = parts[0]
                            if len(parts) > 1:
                                current_field['format'] = parts[1]
                            if len(parts) > 2:
                                current_field['max_length'] = parts[2]
                            
                            found_data_type = True
                        else:
                            description_lines.append(next_line)
                else:
                    # Regular description line
                    if not found_data_type:
                        description_lines.append(next_line)
                
                j += 1
            
            # Combine description lines
            current_field['description'] = ' '.join(description_lines).strip()
            
            # Clean up common issues
            current_field['data_type'] = re.sub(r'VARCHAR\s*2', 'VARCHAR2', current_field['data_type'])
            
            # Move to next field
            i = j
        else:
            i += 1
    
    # Don't forget the last field
    if current_field and current_field.get('name'):
        fields.append(current_field)
    
    return fields

def create_markdown_table(fields: List[Dict], table_name: str) -> str:
    """Create a clean markdown table from field definitions."""
    markdown = f"# {table_name} Data Dictionary\n\n"
    markdown += f"This data dictionary describes the fields in the {table_name} table.\n\n"
    
    if not fields:
        markdown += "*No fields were extracted. Please check the PDF structure.*\n"
        return markdown
    
    markdown += f"**Total Fields:** {len(fields)}\n\n"
    
    markdown += "| Field Name | Description | Sample Data | Data Type | Format | Max Length |\n"
    markdown += "|------------|-------------|-------------|-----------|---------|------------|\n"
    
    for field in fields:
        name = field['name']
        description = field['description'].replace('|', '\\|').strip()
        sample_data = field['sample_data'].replace('|', '\\|') if field['sample_data'] else '-'
        data_type = field['data_type'] if field['data_type'] else '-'
        format_info = field['format'] if field['format'] else '-'
        max_length = field['max_length'] if field['max_length'] else '-'
        
        # Clean up and format values
        if sample_data == '-' or not sample_data:
            sample_data = '-'
        
        # Truncate very long descriptions
        if len(description) > 300:
            description = description[:297] + '...'
        
        # Ensure data type is properly formatted
        data_type = data_type.upper() if data_type != '-' else '-'
        
        markdown += f"| {name} | {description} | {sample_data} | {data_type} | {format_info} | {max_length} |\n"
    
    # Add summary statistics
    markdown += f"\n## Field Summary\n\n"
    
    # Count data types
    data_type_counts = {}
    for field in fields:
        dt = field['data_type'].split('(')[0].strip() if field['data_type'] else 'Not specified'
        data_type_counts[dt] = data_type_counts.get(dt, 0) + 1
    
    markdown += "### Data Types Distribution\n\n"
    for dt, count in sorted(data_type_counts.items()):
        markdown += f"- {dt}: {count} fields\n"
    
    return markdown

def save_section_text(section_name: str, section_lines: List[str], output_dir: Path):
    """Save the raw section text for debugging."""
    with open(output_dir / f'{section_name}_section.txt', 'w', encoding='utf-8') as f:
        f.write('\n'.join(section_lines))

def main():
    """Main function to orchestrate the extraction process."""
    pdf_path = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/data dictionary/open_payments_data_dictionary_methodology-january_2025.pdf")
    output_dir = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/data_dictionaries")
    temp_dir = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/temp")
    
    output_dir.mkdir(exist_ok=True)
    temp_dir.mkdir(exist_ok=True)
    
    print(f"Extracting text from: {pdf_path}")
    full_text = extract_pdf_text(pdf_path)
    
    print(f"Total text length: {len(full_text)} characters")
    
    # Save full text for debugging
    with open(temp_dir / 'pdf_full_text.txt', 'w', encoding='utf-8') as f:
        f.write(full_text)
    
    # Find sections for all three tables
    print("\nSearching for table sections...")
    sections = find_table_sections(full_text)
    
    # Process each table
    tables = {
        'general_payments': 'General Payments',
        'research_payments': 'Research Payments',
        'ownership': 'Physician Ownership and Investment Interest'
    }
    
    all_results = {}
    
    for section_key, table_name in tables.items():
        print(f"\n{'='*60}")
        print(f"Processing {table_name}")
        print(f"{'='*60}")
        
        if sections[section_key]['start'] != -1:
            section_lines = sections[section_key]['text']
            print(f"Section found from line {sections[section_key]['start']} to {sections[section_key]['end']}")
            print(f"Section contains {len(section_lines)} lines")
            
            # Save raw section for debugging
            save_section_text(section_key, section_lines, temp_dir)
            
            # Parse fields with improved parser
            fields = parse_table_fields_improved(section_lines)
            print(f"Extracted {len(fields)} fields")
            
            # Save field data as JSON for debugging
            json_file = temp_dir / f'{section_key}_fields_final.json'
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(fields, f, indent=2, ensure_ascii=False)
            print(f"Saved field data to: {json_file}")
            
            # Create markdown
            markdown_content = create_markdown_table(fields, table_name)
            
            # Save markdown file
            output_file = output_dir / f'{section_key}_data_dictionary.md'
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            print(f"Created markdown file: {output_file}")
            
            # Show first few fields as preview
            if fields:
                print("\nFirst 3 fields extracted:")
                for i, field in enumerate(fields[:3]):
                    print(f"  {i+1}. {field['name']} - {field['data_type']}")
            
            all_results[section_key] = len(fields)
            
        else:
            print(f"ERROR: Table section not found for {table_name}")
            all_results[section_key] = 0
    
    # Final summary
    print(f"\n{'='*60}")
    print("FINAL SUMMARY")
    print(f"{'='*60}")
    for section_key, count in all_results.items():
        status = "✓" if count > 0 else "✗"
        print(f"{status} {tables[section_key]}: {count} fields")
    
    print(f"\nAll markdown files saved to: {output_dir}")
    print(f"Debug files saved to: {temp_dir}")

if __name__ == "__main__":
    main()