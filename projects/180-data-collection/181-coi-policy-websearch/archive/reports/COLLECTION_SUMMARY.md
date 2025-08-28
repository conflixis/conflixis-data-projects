# COI Policy Collection Summary

## Project: DA-181 - COI Policy WebSearch
**Epic**: DA-180 - Data Collection  
**Date**: August 28, 2025

## Collection Results

### Overall Statistics
- **Total Policies Attempted**: 63
- **Successfully Downloaded**: 32 (51% success rate)
- **Failed Downloads**: 31
- **Total Files**: 32 PDF/HTML documents

### Successfully Downloaded Organizations

#### Academic Medical Centers
1. University of South Carolina
2. Brown University Health
3. University of Colorado Anschutz
4. Penn Medicine
5. Northwestern Medicine
6. UCSF Medical

#### Children's Hospitals
1. Boston Children's Hospital
2. Children's Mercy
3. Stanley Manne Children's Research Institute
4. Nemours Children's Health
5. Children's National Hospital

#### Major Health Systems
1. Mayo Clinic (2 policies - main and research)
2. Stony Brook Medicine
3. Albany Med Health System
4. Atrium Health (2 policies - main and Wake Forest Baptist)
5. Unity Health Toronto
6. Memorial Sloan Kettering
7. Hospital for Special Surgery
8. Virtua Health
9. Intermountain Healthcare

#### Research & Professional Organizations
1. American College of Surgeons
2. American Society of Gene & Cell Therapy
3. Public Health Institute
4. National Bureau of Economic Research
5. American Cancer Society
6. Vitalant Research Institute
7. American Society of Nephrology
8. La Jolla Institute for Immunology
9. GE HealthCare

#### Other Healthcare Organizations
1. Osivax

### Failed Downloads (Access Denied/404)
Major systems that blocked access or had broken links:
- Johns Hopkins Medicine
- Stanford Medicine
- Cleveland Clinic
- Harvard Medical School
- Mount Sinai Health
- Mass General Brigham
- NYU Langone
- Michigan Medicine
- UCLA Medical
- Duke Health
- Vanderbilt Medical
- UPMC
- Kaiser Permanente
- CommonSpirit Health
- Ascension Health
- HCA Healthcare
- Yale New Haven Health
- Columbia University Medical
- Washington University Medical
- UNC Health

### Data Storage Structure
```
data/
├── raw/
│   └── policies/
│       ├── *.pdf (30 PDF documents)
│       └── *.html (2 HTML documents)
└── output/
    ├── browser_download_report_*.json
    ├── page2_download_report_*.json
    └── comprehensive_download_*.json
```

### Key Findings
1. **Success Rate**: Achieved 51% success rate in downloading policies
2. **Document Types**: Primarily PDF format (94%), some HTML (6%)
3. **Geographic Coverage**: Nationwide US healthcare systems represented
4. **Organization Types**: Mix of academic medical centers, children's hospitals, research institutes, and major health systems

### Technical Implementation
- Used Python requests library for HTTP downloads
- Implemented concurrent downloads with ThreadPoolExecutor
- Hash-based filename generation to avoid conflicts
- Comprehensive error handling and reporting
- JSON-formatted reports for tracking and analysis

### Next Steps
1. Extract and analyze content from downloaded policies
2. Identify common COI policy themes and requirements
3. Compare policies across different organization types
4. Create structured database of policy provisions
5. Attempt alternative methods for failed downloads

## Files Generated
- 32 policy documents (PDF/HTML)
- 3 download reports (JSON format)
- Scripts for automated collection