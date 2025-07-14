#!/usr/bin/env python3
"""Extract data dictionary information from Open Payments PDF and create markdown files."""

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
        'general_payments': {'text': [], 'found': False},
        'research_payments': {'text': [], 'found': False},
        'ownership': {'text': [], 'found': False}
    }
    
    current_section = None
    
    for i, line in enumerate(lines):
        # Check for General Payments section
        if re.search(r'general\s+payment.*data.*file|appendix\s+b.*general\s+payment', line, re.IGNORECASE):
            current_section = 'general_payments'
            sections[current_section]['found'] = True
            print(f"Found General Payments section at line {i}: {line}")
        
        # Check for Research Payments section
        elif re.search(r'research\s+payment.*data.*file|appendix\s+d.*research\s+payment|appendix\s+d:\s*research', line, re.IGNORECASE):
            current_section = 'research_payments'
            sections[current_section]['found'] = True
            print(f"Found Research Payments section at line {i}: {line}")
        
        # Check for Physician Ownership section
        elif re.search(r'physician\s+ownership.*data.*file|ownership.*investment.*interest|appendix\s+d.*ownership', line, re.IGNORECASE):
            current_section = 'ownership'
            sections[current_section]['found'] = True
            print(f"Found Physician Ownership section at line {i}: {line}")
        
        # Check if we've moved to a different major section
        elif current_section and re.search(r'appendix\s+[a-z]|methodology\s+document', line, re.IGNORECASE):
            # Don't switch if it's the current section's appendix
            if not re.search(rf'appendix.*{current_section.replace("_", " ")}', line, re.IGNORECASE):
                current_section = None
        
        if current_section:
            sections[current_section]['text'].append(line)
    
    # Convert text lists to strings
    for section in sections:
        sections[section]['text'] = '\n'.join(sections[section]['text'])
    
    return sections

def parse_field_definitions(text, table_name):
    """Parse field definitions from the text."""
    fields = []
    lines = text.split('\n')
    
    current_field = None
    in_field_list = False
    
    for i, line in enumerate(lines):
        line_stripped = line.strip()
        
        # Skip empty lines
        if not line_stripped:
            continue
        
        # Look for field definition patterns
        # Pattern 1: Field_Name (with underscores)
        # Pattern 2: FIELD_NAME (all caps)
        # Pattern 3: Field Name followed by description or type
        
        # Check if we're in a field definition section
        if re.search(r'field\s+name|data\s+element|column\s+name', line_stripped, re.IGNORECASE):
            in_field_list = True
            continue
        
        # Match field patterns
        field_patterns = [
            r'^([A-Z][a-zA-Z0-9_]+(?:_[A-Za-z0-9]+)*)\s*[-–:]?\s*(.*)$',  # CamelCase or snake_case
            r'^([A-Z][A-Z0-9_]+)\s*[-–:]?\s*(.*)$',  # ALL_CAPS
            r'^(\w+_\w+(?:_\w+)*)\s*[-–:]?\s*(.*)$',  # any_snake_case
        ]
        
        field_match = None
        for pattern in field_patterns:
            field_match = re.match(pattern, line_stripped)
            if field_match:
                break
        
        if field_match and len(field_match.group(1)) > 2:  # Avoid single letters
            if current_field:
                fields.append(current_field)
            
            field_name = field_match.group(1)
            description = field_match.group(2).strip() if field_match.group(2) else ''
            
            current_field = {
                'name': field_name,
                'description': description,
                'type': None,
                'format': None,
                'required': None,
                'notes': []
            }
        
        elif current_field and line_stripped:
            # Parse additional field information
            if re.search(r'(?:data\s+)?type:\s*(.+)', line_stripped, re.IGNORECASE):
                type_match = re.search(r'(?:data\s+)?type:\s*(.+?)(?:\s*[-–]|$)', line_stripped, re.IGNORECASE)
                if type_match:
                    current_field['type'] = type_match.group(1).strip()
            
            elif re.search(r'format:\s*(.+)', line_stripped, re.IGNORECASE):
                format_match = re.search(r'format:\s*(.+?)(?:\s*[-–]|$)', line_stripped, re.IGNORECASE)
                if format_match:
                    current_field['format'] = format_match.group(1).strip()
            
            elif re.search(r'required:|mandatory:', line_stripped, re.IGNORECASE):
                current_field['required'] = 'Yes' if 'yes' in line_stripped.lower() else 'No'
            
            elif any(keyword in line_stripped.lower() for keyword in ['note:', 'important:', 'example:', 'values:']):
                current_field['notes'].append(line_stripped)
            
            else:
                # Continuation of description
                if len(line_stripped) > 10 and not re.match(r'^\d+$', line_stripped):  # Not just a page number
                    current_field['description'] += ' ' + line_stripped
    
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
    
    markdown += "| Field Name | Description | Data Type | Format | Required | Notes |\n"
    markdown += "|------------|-------------|-----------|---------|----------|-------|\n"
    
    for field in fields:
        name = field['name']
        description = field['description'].replace('|', '\\|') if field['description'] else ''
        data_type = field['type'] if field['type'] else 'Not specified'
        format_info = field['format'] if field['format'] else 'N/A'
        required = field['required'] if field['required'] else 'Not specified'
        notes = ' '.join(field['notes']).replace('|', '\\|') if field['notes'] else 'N/A'
        
        # Truncate long descriptions for readability
        if len(description) > 200:
            description = description[:197] + '...'
        if len(notes) > 100:
            notes = notes[:97] + '...'
        
        markdown += f"| {name} | {description} | {data_type} | {format_info} | {required} | {notes} |\n"
    
    markdown += f"\n\n**Total Fields:** {len(fields)}\n"
    
    return markdown

def main():
    pdf_path = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/data dictionary/open_payments_data_dictionary_methodology-january_2025.pdf")
    output_dir = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/data_dictionaries")
    output_dir.mkdir(exist_ok=True)
    
    print(f"Extracting text from: {pdf_path}")
    full_text = extract_pdf_text(pdf_path)
    
    # Save full text for debugging
    debug_dir = Path("/home/incent/conflixis-analytics/projects/003-sql-agent-v2/temp")
    debug_dir.mkdir(exist_ok=True)
    
    with open(debug_dir / 'pdf_full_text.txt', 'w', encoding='utf-8') as f:
        f.write(full_text)
    
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
        
        if sections[section_key]['found']:
            section_text = sections[section_key]['text']
            print(f"Section found with {len(section_text)} characters")
            
            # Save section text for debugging
            with open(debug_dir / f'{section_key}_section.txt', 'w', encoding='utf-8') as f:
                f.write(section_text)
            
            # Parse fields
            fields = parse_field_definitions(section_text, table_name)
            print(f"Found {len(fields)} fields")
            
            # Save field data as JSON for debugging
            with open(debug_dir / f'{section_key}_fields.json', 'w', encoding='utf-8') as f:
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
            print(f"Section not found for {table_name}")
            all_results[section_key] = 0
    
    # Summary
    print("\n\nSummary:")
    print("-" * 50)
    for section_key, count in all_results.items():
        print(f"{tables[section_key]}: {count} fields")

if __name__ == "__main__":
    main()