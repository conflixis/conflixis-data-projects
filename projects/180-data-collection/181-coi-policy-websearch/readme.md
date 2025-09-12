# COI Policy WebSearch

**Jira Issue**: [DA-181](https://conflixis.atlassian.net/browse/DA-181)  
**Parent Epic**: [DA-180](https://conflixis.atlassian.net/browse/DA-180)

## Overview
Successfully collected **59 Conflict of Interest (COI) policies** from major US healthcare organizations through automated web scraping and search methods.

## Collection Results

### Statistics
- **Total Policies**: 59 PDF/HTML documents
- **Organizations**: 59 unique US healthcare systems
- **Success Rate**: ~45% (many organizations restrict public access)
- **Collection Date**: August 28, 2025

### Breakdown by Category
| Category | Count | Coverage |
|----------|-------|----------|
| Major Health Systems | 30 | 50.8% |
| Academic Medical Centers | 13 | 22% |
| Research & Professional | 9 | 15.3% |
| Children's Hospitals | 6 | 10.2% |
| Other Healthcare | 1 | 1.7% |

## Project Structure
```
coi-policy-websearch/
├── scripts/
│   └── main-coi-collector.py    # Main collection script
├── data/
│   ├── raw/
│   │   ├── policies/            # All downloaded policies
│   │   └── policies_categorized/# Organized by type
│   └── output/                  # Collection reports (JSON)
├── final-collection-report.md   # Comprehensive results
├── requirements.txt             # Python dependencies
└── archive/                     # Test scripts (not in repo)
```

## Usage

### Basic Collection
```bash
# Install dependencies
pip install -r requirements.txt

# Run main collector (downloads all policies)
python scripts/main-coi-collector.py

# Download specific category
python scripts/main-coi-collector.py --collection major
python scripts/main-coi-collector.py --collection children
python scripts/main-coi-collector.py --collection academic
```

### Script Options
- `--collection`: Collection type (all, major, children, academic)
- `--max-workers`: Concurrent downloads (default: 5)
- `--output-dir`: Report directory (default: data/output)
- `--download-dir`: Policy directory (default: data/raw/policies)

## Collection Methods
1. **Direct URL Collection**: Known policy URLs from major systems
2. **Google Search Scraping**: Playwright browser automation for discovery
3. **Targeted System Search**: Focused searches on health networks
4. **Manual Verification**: Confirmed US-only organizations

## Data Organization
Policies are categorized into:
- `academic_medical_centers/` - University-affiliated hospitals
- `childrens_hospitals/` - Pediatric healthcare institutions
- `major_health_systems/` - Large healthcare networks
- `research_professional_orgs/` - Research institutes and societies
- `other_healthcare/` - Specialized healthcare organizations

## Key Organizations Collected

### Major Health Systems (Sample)
- Mayo Clinic
- Memorial Sloan Kettering
- Intermountain Healthcare
- Geisinger
- SSM Health
- RWJBarnabas Health
- Allina Health

### Academic Medical Centers
- Stanford Health Care
- University of Miami Health
- Penn Medicine
- Northwestern Medicine
- UCSF Medical

### Children's Hospitals
- Boston Children's Hospital
- Children's Hospital of Philadelphia
- Children's Mercy
- Nemours Children's Health

## Output Format
Collection reports are in JSON format:
```json
{
  "timestamp": "2025-08-28T09:38:00",
  "collection_name": "all",
  "total_attempted": 59,
  "successful": 59,
  "results": [...]
}
```

## Next Steps
- Content extraction from PDFs
- Policy provision comparison
- Database creation for searchable access
- Regular updates for new policies
- Analysis of policy requirements and trends