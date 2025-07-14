#!/usr/bin/env python3
"""Extract data dictionary information from Open Payments PDF and create markdown files - Improved version."""

import pypdf
import re
from pathlib import Path
import json

def extract_pdf_text(pdf_path):
    """Extract text from PDF file."""
    text = ""
    with open(pdf_path, 'rb') as file:
        reader = pypdf.PdfReader(file)
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            text += page.extract_text()
    return text

def find_table_sections(text):
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
    
    # Find section ends (next table or appendix)
    for section_name, section_data in sections.items():
        if section_data['start'] != -1:
            start_idx = section_data['start']
            # Look for the end of this section
            for i in range(start_idx + 1, len(lines)):
                if re.search(r'Table\s+[A-Z]-\d|Appendix\s+[A-Z]:|Open\s+Payments\s+Methodology', lines[i], re.IGNORECASE):
                    # Check if this is a different table
                    if not re.search(rf'{section_name.replace("_", " ")}', lines[i], re.IGNORECASE):
                        section_data['end'] = i
                        break
            
            # If no end found, use the full remaining text
            if section_data['end'] == -1:
                section_data['end'] = len(lines)
            
            # Extract text for this section
            section_data['text'] = lines[start_idx:section_data['end']]
    
    return sections

def parse_table_fields(section_lines):
    """Parse field definitions from table format."""
    fields = []
    
    # Skip header lines and find where actual field data starts
    data_start = 0
    for i, line in enumerate(section_lines):
        if re.search(r'Field\s+Name.*Field\s+Description.*Data\s+Type', line, re.IGNORECASE):
            data_start = i + 1
            break
    
    current_field = None
    i = data_start
    
    while i < len(section_lines):
        line = section_lines[i].strip()
        
        # Skip empty lines and page headers/footers
        if not line or 'Open Payments Methodology' in line or 'Expiration Date:' in line:
            i += 1
            continue
        
        # Check if this is a field name line (starts with a field name pattern)
        # Field names are typically CamelCase or snake_case at the start of a line
        field_name_match = re.match(r'^([A-Z][a-zA-Z0-9_]+(?:_[A-Za-z0-9]+)*)\s+(.*)$', line)
        
        if field_name_match and not re.search(r'^\d+$|^Field Name|^Length', line):
            # Save previous field if exists
            if current_field:
                fields.append(current_field)
            
            # Start new field
            field_name = field_name_match.group(1)
            rest_of_line = field_name_match.group(2)
            
            current_field = {
                'name': field_name,
                'description': rest_of_line,
                'sample_data': '',
                'data_type': '',
                'format': '',
                'max_length': ''
            }
            
            # Look ahead for continuation of this field's data
            j = i + 1
            description_lines = [rest_of_line]
            
            while j < len(section_lines):
                next_line = section_lines[j].strip()
                
                # Stop if we hit another field name or empty line
                if not next_line or re.match(r'^[A-Z][a-zA-Z0-9_]+(?:_[A-Za-z0-9]+)*\s+', next_line):
                    break
                
                # Check for data type patterns
                if re.search(r'VARCHAR|NUMBER|CHAR|DATE|TIMESTAMP', next_line, re.IGNORECASE):
                    # Extract sample data and data type info
                    parts = next_line.split()
                    
                    # Find VARCHAR, NUMBER, etc.
                    for k, part in enumerate(parts):
                        if re.search(r'VARCHAR|NUMBER|CHAR|DATE|TIMESTAMP', part, re.IGNORECASE):
                            # Sample data is typically before the data type
                            if k > 0:
                                current_field['sample_data'] = ' '.join(parts[:k])
                            
                            # Data type
                            current_field['data_type'] = part
                            
                            # Format and length typically follow
                            if k + 1 < len(parts):
                                current_field['format'] = parts[k + 1]
                            if k + 2 < len(parts):
                                current_field['max_length'] = parts[k + 2]
                            break
                    break
                else:
                    # This is a continuation of the description
                    description_lines.append(next_line)
                
                j += 1
            
            # Combine description lines
            current_field['description'] = ' '.join(description_lines)
            
            # Move index to where we stopped
            i = j
        else:
            i += 1
    
    # Don't forget the last field
    if current_field:
        fields.append(current_field)
    
    return fields

def create_markdown_table(fields, table_name):
    """Create a markdown table from field definitions."""
    markdown = f"# {table_name} Data Dictionary\n\n"
    markdown += f"This data dictionary describes the fields in the {table_name} table.\n\n"
    
    if not fields:
        markdown += "*No fields were extracted. Please check the PDF structure.*\n"
        return markdown
    
    markdown += "| Field Name | Description | Sample Data | Data Type | Format | Max Length |\n"
    markdown += "|------------|-------------|-------------|-----------|---------|------------|\n"
    
    for field in fields:
        name = field['name']
        description = field['description'].replace('|', '\\|').strip()
        sample_data = field['sample_data'].replace('|', '\\|') if field['sample_data'] else 'N/A'
        data_type = field['data_type'] if field['data_type'] else 'Not specified'
        format_info = field['format'] if field['format'] else 'N/A'
        max_length = field['max_length'] if field['max_length'] else 'N/A'
        
        # Clean up data type formatting
        data_type = re.sub(r'VARCHAR\s*2', 'VARCHAR2', data_type)
        
        # Truncate long descriptions for readability
        if len(description) > 200:
            description = description[:197] + '...'
        
        markdown += f"| {name} | {description} | {sample_data} | {data_type} | {format_info} | {max_length} |\n"
    
    markdown += f"\n\n**Total Fields:** {len(fields)}\n"
    
    return markdown

def main():
    pdf_path = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/data dictionary/open_payments_data_dictionary_methodology-january_2025.pdf")
    output_dir = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/data_dictionaries")
    output_dir.mkdir(exist_ok=True)
    
    print(f"Extracting text from: {pdf_path}")
    full_text = extract_pdf_text(pdf_path)
    
    print(f"Total text length: {len(full_text)} characters")
    
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
        print(f"\n\nProcessing {table_name}...")
        
        if sections[section_key]['start'] != -1:
            section_lines = sections[section_key]['text']
            print(f"Section found from line {sections[section_key]['start']} to {sections[section_key]['end']}")
            print(f"Section contains {len(section_lines)} lines")
            
            # Parse fields
            fields = parse_table_fields(section_lines)
            print(f"Found {len(fields)} fields")
            
            # Save field data as JSON for debugging
            debug_dir = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/temp")
            debug_dir.mkdir(exist_ok=True)
            
            with open(debug_dir / f'{section_key}_fields_improved.json', 'w', encoding='utf-8') as f:
                json.dump(fields, f, indent=2)
            
            # Create markdown
            markdown_content = create_markdown_table(fields, table_name)
            
            # Save markdown file
            output_file = output_dir / f'{section_key}_data_dictionary.md'
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            print(f"Created: {output_file}")
            all_results[section_key] = len(fields)
            
        else:
            print(f"Table not found for {table_name}")
            all_results[section_key] = 0
    
    # Summary
    print("\n\nSummary:")
    print("-" * 50)
    for section_key, count in all_results.items():
        print(f"{tables[section_key]}: {count} fields")

if __name__ == "__main__":
    main()