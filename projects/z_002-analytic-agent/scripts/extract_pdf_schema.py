#!/usr/bin/env python3
"""Extract schema information from Open Payments data dictionary PDF."""

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

def find_general_payments_section(text):
    """Find and extract the General Payments section."""
    # Look for section headers related to General Payments
    lines = text.split('\n')
    
    in_general_section = False
    general_section_text = []
    
    for i, line in enumerate(lines):
        # Check for General Payments section start
        if re.search(r'general\s+payment.*data\s+file|general\s+payment.*fields|general\s+payment.*schema', line, re.IGNORECASE):
            in_general_section = True
            print(f"Found General Payments section at line {i}: {line}")
        
        # Check for next major section (to know when to stop)
        elif in_general_section and re.search(r'(research\s+payment|ownership\s+and\s+investment|physician\s+owned)', line, re.IGNORECASE):
            break
            
        if in_general_section:
            general_section_text.append(line)
    
    return '\n'.join(general_section_text)

def parse_field_definitions(text):
    """Parse field definitions from the text."""
    fields = []
    lines = text.split('\n')
    
    current_field = None
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Look for patterns that indicate field names
        # Common patterns: FIELD_NAME, Field_Name, field_name followed by description
        field_match = re.match(r'^([A-Z][A-Z0-9_]+)\s*[-–:]?\s*(.*)$', line)
        if field_match:
            if current_field:
                fields.append(current_field)
            current_field = {
                'name': field_match.group(1),
                'description': field_match.group(2).strip() if field_match.group(2) else '',
                'type': None,
                'notes': []
            }
        elif current_field and line:
            # Continuation of description
            if 'Type:' in line or 'Data Type:' in line:
                type_match = re.search(r'(?:Data )?Type:\s*(.+?)(?:\s*[-–]|$)', line, re.IGNORECASE)
                if type_match:
                    current_field['type'] = type_match.group(1).strip()
            elif any(keyword in line.lower() for keyword in ['note:', 'important:', 'required:', 'optional:']):
                current_field['notes'].append(line)
            else:
                current_field['description'] += ' ' + line
    
    if current_field:
        fields.append(current_field)
    
    return fields

def main():
    pdf_path = Path("/home/incent/conflixis-analytics/projects/002-analytic-agent/data dictionary/open_payments_data_dictionary_methodology-january_2025.pdf")
    
    print(f"Extracting text from: {pdf_path}")
    full_text = extract_pdf_text(pdf_path)
    
    # Save full text for inspection
    with open('/home/incent/conflixis-analytics/projects/002-analytic-agent/pdf_full_text.txt', 'w', encoding='utf-8') as f:
        f.write(full_text)
    
    print(f"Total text length: {len(full_text)} characters")
    
    # Find General Payments section
    print("\nSearching for General Payments section...")
    general_section = find_general_payments_section(full_text)
    
    if general_section:
        with open('/home/incent/conflixis-analytics/projects/002-analytic-agent/general_payments_section.txt', 'w', encoding='utf-8') as f:
            f.write(general_section)
        print(f"General Payments section found: {len(general_section)} characters")
    else:
        print("General Payments section not found, searching entire document...")
        general_section = full_text
    
    # Parse field definitions
    fields = parse_field_definitions(general_section)
    
    # Save results
    with open('/home/incent/conflixis-analytics/projects/002-analytic-agent/general_payments_fields.json', 'w', encoding='utf-8') as f:
        json.dump(fields, f, indent=2)
    
    print(f"\nFound {len(fields)} field definitions")
    
    # Print summary
    for field in fields[:10]:  # Show first 10 fields
        print(f"\n{field['name']}")
        print(f"  Description: {field['description'][:100]}...")
        if field['type']:
            print(f"  Type: {field['type']}")
        if field['notes']:
            print(f"  Notes: {field['notes'][0]}")

if __name__ == "__main__":
    main()