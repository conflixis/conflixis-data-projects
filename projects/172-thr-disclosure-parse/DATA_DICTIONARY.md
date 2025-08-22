# THR Disclosure Data Dictionary

## Overview
This document describes all fields extracted from the THR disclosure data, including their sources, data types, and business rules.

## Data Sources
- **Primary**: `conflixis-engine.firestore_export.disclosures_raw_latest`
- **Member Data**: `conflixis-engine.firestore_export.member_shards_raw_latest_parsed`
- **Group ID**: gcO9AHYlNSzFeGTRSFRa
- **Campaign ID**: qyH2ggzVV0WLkuRfem7S

## Main Disclosure Fields (thr_disclosures_*.csv)

### Identity & Document Fields

| Field Name | Data Type | Source Path | Description | Example |
|------------|-----------|-------------|-------------|---------|
| `document_id` | string | `document_id` | Unique identifier for each disclosure | "FtI8SQcb7KXSQgEc6CQa" |
| `id` | string | `document_id` | Duplicate of document_id for backwards compatibility | "FtI8SQcb7KXSQgEc6CQa" |
| `provider_name` | string | `$.reporter.name` | Name of the person making the disclosure | "Dr. Jane Smith" |
| `provider_npi` | string | Member join | National Provider Identifier from member data | "1234567890" |
| `provider_email` | string | `$.reporter.email` | Email address of the reporter | "jane.smith@texashealth.org" |
| `person_with_interest` | string | Dynamic field extraction | Person the disclosure is about (may differ from reporter) | "John Doe" |

### Organizational Fields

| Field Name | Data Type | Source Path | Description | Example |
|------------|-----------|-------------|-------------|---------|
| `job_title` | string | Member join | Job title from member data | "Physician" |
| `department` | string | Member join | Department/entity from member data | "THPG" |
| `manager_name` | string | `$.manager` or Member join | Manager name | "Sarah Johnson" |
| `entity_name` | string | Computed | Company or entity involved in the disclosure | "Abbott Laboratories" |

### Disclosure Classification

| Field Name | Data Type | Source Path | Description | Example |
|------------|-----------|-------------|-------------|---------|
| `relationship_type` | string | `$.question.title` | Type of disclosure/question | "Receipt of Compensation" |
| `category_label` | string | `$.question.category_label` | Disclosure category | "Financial & Investment Interests" |
| `interest_type` | string | Dynamic field extraction | Specific type of interest | "Consulting Fees" |
| `question_id` | string | `$.question.question_id` | Unique question identifier | "d9d4e964-3527-42dd-80b0" |
| `category_id` | string | `$.question.category_id` | Numeric category identifier | "2" |

### Financial Information

| Field Name | Data Type | Source Path | Description | Example |
|------------|-----------|-------------|-------------|---------|
| `financial_amount` | float | `$.compensation_value` | Dollar amount of the disclosure | 25000.00 |
| `compensation_type` | string | `$.compensation_type` | Type of compensation | "Cash" |
| `compensation_received_by` | string | `$.compensation_received_by` | Who received the compensation | "Self" |
| `compensation_received_by_self` | boolean | `$.compensation_received_by_self` | Whether reporter received compensation | true |

### Risk Assessment

| Field Name | Data Type | Source Path | Description | Example |
|------------|-----------|-------------|-------------|---------|
| `risk_tier` | string | Calculated | Risk level based on amount | "moderate" |
| `risk_score` | integer | Calculated | Risk score (0-100) | 45 |

### Dates

| Field Name | Data Type | Source Path | Description | Example |
|------------|-----------|-------------|-------------|---------|
| `disclosure_date` | date | `timestamp` | When disclosure was created | "2025-01-15" |
| `relationship_start_date` | date | `$.service_start_date` | Start date of relationship | "2024-01-01" |
| `relationship_end_date` | date | `$.service_end_date` | End date of relationship | "2024-12-31" |
| `relationship_ongoing` | boolean | Computed | Whether relationship is ongoing | true |
| `disclosure_timeframe_start` | date | `$.disclosure_timeframe_start_date` | Reporting period start | "2024-01-01" |
| `disclosure_timeframe_end` | date | `$.disclosure_timeframe_end_date` | Reporting period end | "2024-12-31" |
| `signature_date` | datetime | `$.signature_date._seconds` | When disclosure was signed | "2025-01-15 10:30:00" |
| `created_at` | datetime | `timestamp` | Record creation timestamp | "2025-01-15 10:30:00" |
| `updated_at` | datetime | `$.updated_at` | Last update timestamp | "2025-01-16 14:20:00" |

### Status & Review

| Field Name | Data Type | Source Path | Description | Example |
|------------|-----------|-------------|-------------|---------|
| `status` | string | `$.status` | Disclosure status | "complete" |
| `review_status` | string | `$.review_status` | Review status | "pending" |
| `reviewer` | string | `$.reviewer` | Name of reviewer | "Admin User" |
| `last_review_date` | date | Computed | Date of last review | "2025-01-15" |
| `next_review_date` | date | Computed | Scheduled next review | "2025-12-31" |

### Additional Metadata

| Field Name | Data Type | Source Path | Description | Example |
|------------|-----------|-------------|-------------|---------|
| `is_research` | boolean | `$.is_research` | Research-related disclosure | false |
| `disputed` | boolean | `$.disputed` | Whether disclosure is disputed | false |
| `notes` | string | `$.notes` | Additional notes | "Serves as medical director" |
| `signature_name` | string | `$.signature.full_name` | Full name on signature | "Jane Smith" |
| `signature_initials` | string | `$.signature.initials` | Initials on signature | "JS" |
| `source` | string | `$.source` | How disclosure was created | "form" |
| `campaign_title` | string | `$.campaign_title` | Campaign name | "2025 Texas Health COI Survey" |
| `service_provided` | string | `$.service_provided` | Description of services | "Consulting services" |
| `interests` | string | `$.interests` | Array of interests | "[]" |
| `person_id` | string | `$.person_id` | Person identifier | "6QqiZgevQGi24j4UOufO" |

### Related Party Fields (for "Related Parties" disclosures)

| Field Name | Data Type | Source Path | Description | Example |
|------------|-----------|-------------|-------------|---------|
| `related_party_first_name` | string | Field values [0] | First name of related party | "John" |
| `related_party_last_name` | string | Field values [1] | Last name of related party | "Doe" |
| `related_party_entity_location` | string | Field values [2] | Entity where related party works | "THFW (Texas Health Fort Worth)" |
| `related_party_job_title` | string | Field values [3] | Job title of related party | "Nurse" |

### Category-Specific Fields

| Field Name | Data Type | Source Path | Description | Example |
|------------|-----------|-------------|-------------|---------|
| `jurisdiction_location` | string | Dynamic extraction | For political disclosures - office location | "City of Frisco" |
| `resolution_date` | date | Dynamic extraction | For legal disclosures - resolution date | "2025-12-30" |
| `entity_where_occurred` | string | Dynamic extraction | For legal/compliance - where issue occurred | "THSL" |

## Open Payments Transaction Fields (thr_op_transactions_*.csv)

| Field Name | Data Type | Source Path | Description | Example |
|------------|-----------|-------------|-------------|---------|
| `document_id` | string | `document_id` | Links to main disclosure | "j5HuSNP2XjRpPCGFy8bv" |
| `reporter_name` | string | `$.reporter.name` | Provider name | "Dr. Jane Smith" |
| `reporter_email` | string | `$.reporter.email` | Provider email | "jane.smith@texashealth.org" |
| `provider_npi` | string | `$.recipient_npi` or Member join | Provider NPI | "1234567890" |
| `provider_job_title` | string | Member join | Job title | "Physician" |
| `provider_entity` | string | Member join | Entity/department | "THPG" |
| `company_name` | string | `$.company_name` | Paying company | "Abbott Laboratories" |
| `submitting_entity_id` | string | `$.company_id` | CMS entity ID | "100000010774" |
| `record_id` | string | Transaction `.record_id` | CMS Open Payments record ID | "1140257425" |
| `payment_date` | date | Transaction `.payment_date` | Date of payment | "2024-05-01" |
| `payment_amount` | float | Transaction `.payment_total_usd` | Payment amount | 400.00 |
| `payment_nature` | string | Transaction `.payment_nature` | Type of payment | "Consulting Fee" |
| `payment_form` | string | Transaction `.payment_form` | Form of payment | "Cash or cash equivalent" |
| `payment_count` | integer | Transaction `.payment_count` | Number of payments | 1 |
| `program_year` | string | Transaction `.program_year` | CMS program year | "2024" |
| `payment_publication_date` | date | Transaction `.payment_publication_date` | CMS publication date | "2025-06-30" |
| `name_of_study` | string | Transaction `.name_of_study` | For research payments | null |
| `transaction_source` | string | Transaction `.source` | Source system | "op-general" |
| `disclosure_start` | date | `$.disclosure_timeframe_start_date` | Reporting period start | "2024-01-01" |
| `disclosure_end` | date | `$.disclosure_timeframe_end_date` | Reporting period end | "2024-12-31" |
| `signature_name` | string | `$.signature.full_name` | Signature name | "Jane Smith" |
| `signature_date` | datetime | `$.signature_date._seconds` | Signature timestamp | "2025-01-15 10:30:00" |
| `created_at` | datetime | `$.created_at._seconds` | Creation timestamp | "2025-01-15 10:30:00" |

## Business Rules

### Risk Tier Calculation
- **None**: $0
- **Low**: $1 - $999
- **Moderate**: $1,000 - $9,999
- **High**: $10,000 - $99,999
- **Critical**: $100,000+

### Data Aggregation
- Main disclosure file: One row per disclosure (1,300 records)
- Transaction file: One row per Open Payments transaction (~2,536 records)
- Open Payments are aggregated by reporter + company in main file

### Category Mapping
- Null category_label → "Open Payments (CMS Imports)"
- "External Roles and Relationships" → "External Roles & Relationships"
- "Financial and Investment Interests" → "Financial & Investment Interests"

### Dynamic Field Extraction
Fields are extracted from `question.field_values` based on `question.fields` structure:
1. Map field IDs to field definitions
2. Extract values based on field titles
3. Handle different field types (person, company, currency, etc.)

## Data Quality Notes

- **Person With Interest**: May differ from reporter; critical for accurate disclosure tracking
- **NPIs**: Matched from member data; ~58% match rate
- **Duplicate Check**: Only 1 duplicate reporter-company pair found (Courtney Cartier - INTUITIVE SURGICAL)
- **Missing Values**: Filled with appropriate defaults (empty strings, zeros, "Not Specified")

## Export Formats

- **CSV**: Primary format for analysis
- **Parquet**: Compressed format for performance
- **JSON**: Includes metadata and summary statistics