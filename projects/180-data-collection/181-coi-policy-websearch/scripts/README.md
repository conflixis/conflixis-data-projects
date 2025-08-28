# COI Policy Collection Scripts

## Overview
This folder contains the production script for collecting Conflict of Interest (COI) policies from US healthcare systems and government agencies.

## Main Script

### `coi-policy-collector.py`
**Purpose**: Comprehensive COI policy collector for US healthcare organizations  
**Project**: DA-181 - COI Policy WebSearch

#### Features
- Downloads COI policies from multiple sources
- Supports concurrent downloads for efficiency
- Robust error handling with retry logic
- Generates detailed collection reports
- Handles SSL certificate issues for government sites

#### Usage
```bash
# Download all policies (healthcare + federal/state)
python coi-policy-collector.py

# Download only healthcare system policies
python coi-policy-collector.py --collection healthcare

# Download only federal and state government policies
python coi-policy-collector.py --collection federal-state

# Custom configuration
python coi-policy-collector.py \
    --collection all \
    --max-workers 10 \
    --output-dir custom/output \
    --download-dir custom/downloads
```

#### Command Line Arguments
- `--collection`: Which collection to download (choices: all, healthcare, federal-state)
- `--max-workers`: Maximum concurrent downloads (default: 5)
- `--output-dir`: Output directory for reports (default: data/output)
- `--download-dir`: Download directory for policies (default: data/raw/policies)

#### Output
- **Downloaded policies**: Saved to `data/raw/policies/` with format `{OrgName}_{hash}.{ext}`
- **Collection report**: JSON report saved to `data/output/{collection}_report_{timestamp}.json`
- **Console summary**: Displays collection statistics and newly downloaded organizations

#### Collections

**Healthcare Systems** (examples):
- Mayo Clinic
- Cleveland Clinic
- Johns Hopkins Medicine
- Stanford Medicine
- Kaiser Permanente Research
- Mount Sinai Health
- Boston Children's Hospital
- Children's Hospital of Philadelphia

**Federal Agencies** (examples):
- FDA Ethics Office
- NIH Ethics Office
- CDC Ethics Office
- CMS Ethics Division
- Veterans Affairs
- National Science Foundation

**State Health Departments** (examples):
- California Department of Public Health
- New York State Department of Health
- Texas Health and Human Services
- Florida Department of Health
- And all other US states...

## Archived Scripts
Older versions and test scripts have been moved to `../archive/scripts/`:
- `download-federal-state-policies.py` - Initial federal/state downloader
- `enhanced-federal-state-collector.py` - Enhanced version with alternative URLs
- `main-coi-collector.py` - Original healthcare system collector

## Requirements
```python
requests>=2.31.0
urllib3>=2.0.0
```

## Notes
- The script disables SSL warnings for government sites that have certificate issues
- Uses concurrent downloads (ThreadPoolExecutor) for better performance
- Automatically skips already downloaded files to avoid duplicates
- Handles various URL formats (PDF, HTML, DOC)
- Implements retry logic with different headers for stubborn sites

## Development
To add new policies, edit the functions in `coi-policy-collector.py`:
- `get_healthcare_policies()` - Add healthcare system URLs
- `get_federal_state_policies()` - Add government agency URLs

---
*Last updated: August 28, 2025*