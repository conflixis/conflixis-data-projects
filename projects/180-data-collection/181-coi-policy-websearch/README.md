# COI Policy WebSearch Collection

## Project Overview
**Project**: DA-181 - COI Policy WebSearch  
**Epic**: DA-180 - Data Collection  
**Objective**: Collect and organize Conflict of Interest (COI) policies from US healthcare organizations for compliance analysis

## Collection Summary
Successfully collected and verified **46 actual COI policies** from major US healthcare systems and government agencies (cleaned from initial 88 documents).

### Statistics
- **Total Verified COI Policies**: 46 documents (PDF/HTML format)
- **Organizations**: 46 unique healthcare entities
- **Categories**: 6 organization types
- **Geographic Coverage**: All US regions + federal agencies
- **Cleanup**: Removed 42 non-COI/invalid files (HTML without COI content, JavaScript, images, empty files)

### Breakdown by Category (After Cleanup)
| Category | Count | Examples |
|----------|-------|----------|
| Major Health Systems | 16 | Mayo Clinic, Kaiser Permanente, Atrium Health |
| Federal & State Agencies | 8 | FDA, NIH, Texas HHS, Mississippi Health |
| Academic Medical Centers | 7 | Stanford, Brown, University of Colorado |
| Research & Professional | 9 | American Cancer Society, GE HealthCare |
| Children's Hospitals | 5 | Boston Children's, CHOP |
| Other Healthcare | 1 | Osivax |

## Repository Structure
```
coi-policy-websearch/
├── scripts/
│   ├── coi-policy-collector.py    # Main consolidated collection script
│   └── README.md                  # Script documentation
├── data/
│   ├── raw/
│   │   └── policies_categorized/  # Organized policy documents
│   │       ├── major_health_systems/       (16 policies)
│   │       ├── federal-state/              (8 policies)
│   │       ├── academic_medical_centers/   (7 policies)
│   │       ├── research_professional_orgs/ (9 policies)
│   │       ├── childrens_hospitals/        (5 policies)
│   │       └── other_healthcare/           (1 policy)
│   └── output/                    # Collection reports (JSON)
├── archive/                       # Old scripts and test files
├── collection-report.md           # Detailed collection report
└── README.md                      # This file
```

## Usage

### Running the Collection Script
```bash
# Install dependencies
pip install requests urllib3

# Collect all policies
python scripts/coi-policy-collector.py

# Collect specific categories
python scripts/coi-policy-collector.py --collection healthcare
python scripts/coi-policy-collector.py --collection federal-state

# Custom configuration
python scripts/coi-policy-collector.py \
    --max-workers 10 \
    --output-dir custom/output \
    --download-dir custom/downloads
```

### Script Options
- `--collection`: Choose collection type (all, healthcare, federal-state)
- `--max-workers`: Concurrent download threads (default: 5)
- `--output-dir`: Report output directory
- `--download-dir`: Policy download directory

## Collection Methods
1. **Direct URL Downloads**: Known policy URLs from major organizations
2. **Google Search Scraping**: Playwright MCP browser automation
3. **VPN-Enhanced Access**: US VPN for government/restricted sites
4. **Alternative URL Discovery**: Finding backup URLs for blocked sites

## Technical Details
- **Languages**: Python 3.x
- **Libraries**: requests, urllib3, concurrent.futures
- **Techniques**: 
  - Concurrent downloads with ThreadPoolExecutor
  - SSL certificate bypass for government sites
  - User-agent rotation for access
  - Content hash-based deduplication

## Key Achievements
✅ 46 verified COI policies after content analysis and cleanup  
✅ Comprehensive coverage of US healthcare organizations  
✅ Clean, categorized data structure  
✅ Both federal and state government agencies included  
✅ Major academic medical centers represented  

## Data Quality
- **Initial Collection**: 88 documents collected
- **Content Analysis**: Verified actual COI content using keyword analysis
- **Cleanup Results**: 
  - Removed 25 HTML files without COI content
  - Removed 3 JavaScript files misnamed as PDFs
  - Removed 1 image file and 2 empty files
  - Renamed 1 HTML file with COI content from .pdf to .html
  - Moved 11 non-COI documents (codes of conduct, training materials)
- **Final Dataset**: 46 verified COI policy documents
- **Format**: 43 PDFs, 3 HTML files with confirmed COI content

## Next Steps
1. **Content Analysis**: Extract key policy provisions
2. **Comparison Matrix**: Create compliance comparison grid
3. **Database Creation**: Structure for searchability
4. **Regular Updates**: Schedule periodic re-collection

## Notes
- US VPN access significantly improved success rate
- Many organizations restrict public access to policies
- Government sites often have SSL certificate issues
- Playwright MCP tools essential for Google search scraping

## Resources
- [Collection Report](collection-report.md) - Detailed statistics and findings
- [Script Documentation](scripts/README.md) - Technical script details

---
*Last Updated: August 28, 2025*