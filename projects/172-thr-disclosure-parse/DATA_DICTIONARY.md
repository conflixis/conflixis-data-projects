# THR Disclosure Data Dictionary

## Overview
This document describes all fields extracted from the THR disclosure data, organized by disclosure category and type.

## Data Sources
- **Primary**: `conflixis-engine.firestore_export.disclosures_raw_latest`
- **Member Data**: `conflixis-engine.firestore_export.member_shards_raw_latest_parsed`
- **Form Structure**: `conflixis-engine.firestore_export.disclosure_forms_raw_latest`
- **Group ID**: gcO9AHYlNSzFeGTRSFRa
- **Campaign ID**: qyH2ggzVV0WLkuRfem7S

## Disclosure Categories Summary
1. **External Roles & Relationships** (458 disclosures)
2. **Financial & Investment Interests** (295 disclosures)
3. **Political, Community, and Advocacy Activities** (304 disclosures)
4. **Legal, Regulatory, Ethical, and Compliance Matters** (101 disclosures)
5. **Open Payments (CMS Imports)** (142 disclosures)

---

## COMMON FIELDS (All Disclosures)

### Core Identity Fields
| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `document_id` | string | Unique identifier for each disclosure | "FtI8SQcb7KXSQgEc6CQa" |
| `provider_name` | string | Name of the person making the disclosure | "Dr. Jane Smith" |
| `provider_email` | string | Email address of the reporter | "jane.smith@texashealth.org" |
| `provider_npi` | string | National Provider Identifier from member data | "1234567890" |
| `job_title` | string | Job title from member data | "Physician" |
| `department` | string | Department/entity from member data | "THPG" |
| `manager_name` | string | Manager name | "Sarah Johnson" |

### Disclosure Metadata
| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `category_label` | string | Disclosure category | "Financial & Investment Interests" |
| `relationship_type` | string | Type of disclosure/question | "Receipt of Compensation" |
| `question_id` | string | Unique question identifier | "d9d4e964-3527-42dd-80b0" |
| `category_id` | string | Numeric category identifier | "2" |
| `status` | string | Disclosure status | "complete" |
| `source` | string | How disclosure was created | "form" |
| `campaign_title` | string | Campaign name | "2025 Texas Health COI Survey" |

### Date Fields
| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `disclosure_date` | date | When disclosure was created | "2025-01-15" |
| `disclosure_timeframe_start` | date | Reporting period start | "2024-01-01" |
| `disclosure_timeframe_end` | date | Reporting period end | "2024-12-31" |
| `signature_date` | datetime | When disclosure was signed | "2025-01-15 10:30:00" |
| `created_at` | datetime | Record creation timestamp | "2025-01-15 10:30:00" |
| `updated_at` | datetime | Last update timestamp | "2025-01-16 14:20:00" |

### Signature & Attestation
| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `signature_name` | string | Full name on signature | "Jane Smith" |
| `signature_initials` | string | Initials on signature | "JS" |
| `disputed` | boolean | Whether disclosure is disputed | false |
| `notes` | string | Additional notes | "Serves as medical director" |

---

## 1. EXTERNAL ROLES & RELATIONSHIPS
*458 disclosures covering board memberships, advisory roles, and family relationships*

### Question Types in this Category:
- Board Membership (52 disclosures)
- Related Parties (303 disclosures)
- Advisory Position with Entity (103 disclosures)

### Category-Specific Fields

#### For All External Roles:
| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `person_with_interest` | string | Person the disclosure is about | "John Doe" |
| `entity_name` | string | Company or entity involved | "Abbott Laboratories" |
| `interest_type` | string | Specific type of interest | "Board Member" |
| `relationship_start_date` | date | Start date of relationship | "2024-01-01" |
| `relationship_end_date` | date | End date of relationship | "2024-12-31" |
| `relationship_ongoing` | boolean | Whether relationship is ongoing | true |
| `service_provided` | string | Description of services | "Advisory services" |

#### For Related Parties Only:
| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `related_party_first_name` | string | First name of related party | "John" |
| `related_party_last_name` | string | Last name of related party | "Doe" |
| `related_party_entity_location` | string | Entity where related party works | "THFW (Texas Health Fort Worth)" |
| `related_party_job_title` | string | Job title of related party | "Nurse" |

---

## 2. FINANCIAL & INVESTMENT INTERESTS
*295 disclosures covering compensation, investments, and financial relationships*

### Question Types in this Category:
- Receipt of Compensation (112 disclosures)
- Investment Interest (72 disclosures) 
- Employment Income (66 disclosures)
- Ownership Interest (45 disclosures)

### Category-Specific Fields
| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `person_with_interest` | string | Person receiving compensation | "Dr. Jane Smith" |
| `entity_name` | string | Company paying compensation | "Pfizer Inc." |
| `interest_type` | string | Type of financial interest | "Consulting Fees" |
| `financial_amount` | float | Dollar amount of the disclosure | 25000.00 |
| `compensation_type` | string | Type of compensation | "Cash" |
| `compensation_received_by` | string | Who received the compensation | "Self" |
| `compensation_received_by_self` | boolean | Whether reporter received compensation | true |
| `service_provided` | string | Services provided for compensation | "Medical consulting" |
| `is_research` | boolean | Research-related payment | false |

### Risk Assessment Fields
| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `risk_tier` | string | Risk level based on amount | "moderate" |
| `risk_score` | integer | Risk score (0-100) | 45 |

**Risk Tier Thresholds:**
- None: $0
- Low: $1 - $999
- Moderate: $1,000 - $9,999
- High: $10,000 - $99,999
- Critical: $100,000+

---

## 3. POLITICAL, COMMUNITY, AND ADVOCACY ACTIVITIES
*304 disclosures covering political positions, community service, and advocacy*

### Question Types in this Category:
- Political Office or Candidacy (97 disclosures)
- Community Service (124 disclosures)
- Advocacy Activities (83 disclosures)

### Category-Specific Fields
| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `person_with_interest` | string | Person holding position | "Jane Smith" |
| `entity_name` | string | Organization or office | "City Council" |
| `interest_type` | string | Type of activity | "Elected Official" |
| `jurisdiction_location` | string | Office location/jurisdiction | "City of Frisco" |
| `service_provided` | string | Description of role | "Board member" |
| `relationship_start_date` | date | Start date of position | "2024-01-01" |
| `relationship_end_date` | date | End date of position | "2024-12-31" |
| `relationship_ongoing` | boolean | Whether position is current | true |

---

## 4. LEGAL, REGULATORY, ETHICAL, AND COMPLIANCE MATTERS
*101 disclosures covering legal issues, sanctions, and compliance matters*

### Question Types in this Category:
- Government Sanctions/Exclusions (41 disclosures)
- Legal Proceedings (32 disclosures)
- Professional License Issues (28 disclosures)

### Category-Specific Fields
| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `person_with_interest` | string | Person involved in matter | "John Doe" |
| `entity_name` | string | Entity involved | "State Medical Board" |
| `interest_type` | string | Type of matter | "License Suspension" |
| `entity_where_occurred` | string | Where issue occurred | "THSL" |
| `resolution_date` | date | Date matter was resolved | "2025-12-30" |
| `disputed` | boolean | Whether matter is disputed | false |
| `notes` | string | Additional details | "Matter resolved satisfactorily" |

---

## 5. OPEN PAYMENTS (CMS IMPORTS)
*142 disclosures with 2,536 individual transactions from CMS Open Payments database*

### Aggregated Fields (Main Disclosure File)
| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `entity_name` | string | Company making payments | "Abbott Laboratories" |
| `financial_amount` | float | Total aggregated amount | 5000.00 |
| `compensation_type` | string | Type of payments | "Multiple" |
| `interests` | json | Array of payment types | ["Consulting", "Speaking"] |

### Transaction-Level Fields (thr_op_transactions_*.csv)
| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| `document_id` | string | Links to main disclosure | "j5HuSNP2XjRpPCGFy8bv" |
| `reporter_name` | string | Provider name | "Dr. Jane Smith" |
| `provider_npi` | string | Provider NPI | "1234567890" |
| `company_name` | string | Paying company | "Abbott Laboratories" |
| `submitting_entity_id` | string | CMS entity ID | "100000010774" |
| `record_id` | string | CMS Open Payments record ID | "1140257425" |
| `payment_date` | date | Date of payment | "2024-05-01" |
| `payment_amount` | float | Individual payment amount | 400.00 |
| `payment_nature` | string | Type of payment | "Consulting Fee" |
| `payment_form` | string | Form of payment | "Cash or cash equivalent" |
| `payment_count` | integer | Number of payments | 1 |
| `program_year` | string | CMS program year | "2024" |
| `payment_publication_date` | date | CMS publication date | "2025-06-30" |
| `name_of_study` | string | For research payments | null |
| `transaction_source` | string | Source system | "op-general" |

---

## DATA QUALITY NOTES

### Field Population Rates
- **Person With Interest**: 572 records (44% of disclosures)
- **Interest Type**: 259 records (20% of disclosures) 
- **Financial Amount**: 267 records (primarily Financial & Open Payments categories)
- **NPIs**: ~58% match rate from member data
- **Related Party Fields**: 303 records (Related Parties disclosures only)

### Key Business Rules
1. **Person With Interest** may differ from reporter - critical for tracking actual conflict holders
2. **Open Payments** are aggregated by reporter + company pair in main file
3. **Dynamic fields** vary by question type and are extracted from nested JSON structures
4. **Category normalization** applied for consistency (e.g., "External Roles and Relationships" → "External Roles & Relationships")

### Data Aggregation
- **Main disclosure file**: One row per disclosure (1,300 records total)
- **Transaction file**: One row per Open Payments transaction (2,536 records)
- **Deduplication**: Only 1 duplicate reporter-company pair found (Courtney Cartier - INTUITIVE SURGICAL)

---

## EXPORT FORMATS

### File Naming Convention
- Main file: `thr_disclosures_YYYYMMDD_HHMMSS.csv`
- Transactions: `thr_op_transactions_YYYYMMDD_HHMMSS.csv`

### Available Formats
- **CSV**: Primary format for analysis (UTF-8 encoded)
- **Parquet**: Compressed format for performance (Snappy compression)
- **JSON**: Includes metadata and summary statistics

### Column Ordering in CSV
Columns are organized for readability:
1. Identity fields (document_id, provider info)
2. Category/classification fields
3. Specific disclosure fields (varies by category)
4. Financial/risk fields (where applicable)
5. Date fields
6. Status/metadata fields
7. Signature/attestation fields