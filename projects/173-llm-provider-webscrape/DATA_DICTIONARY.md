# Provider Profile Web Enrichment - Data Dictionary

**Version**: 1.0  
**Last Updated**: August 2024  
**Format**: JSON  

## Overview

This document defines all data fields captured in the provider profile enrichment process. Each profile consists of three main sections: `profile` (structured data), `citations` (source references), and `metadata` (processing information).

## Root Structure

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `profile` | Object | Yes | Structured provider profile data |
| `citations` | Array | Yes | List of web sources with URLs and extracted data |
| `metadata` | Object | Yes | Processing metadata and quality metrics |

## Profile Object

### profile.basic_info

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `name` | String | Yes | Provider's full professional name | "John A. Smith, MD" |
| `npi` | String | No | 10-digit National Provider Identifier | "1234567890" |
| `specialty` | String | No | Primary medical specialty | "Orthopedic Surgery" |
| `subspecialties` | Array[String] | No | List of subspecialties | ["Sports Medicine", "Joint Replacement"] |

### profile.professional

#### profile.professional.current_positions

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `title` | String | Yes | Position title | "Chief of Orthopedics" |
| `organization` | String | Yes | Organization name | "Penn State Health" |
| `department` | String | No | Department name | "Department of Surgery" |
| `start_date` | String | No | Position start date (YYYY-MM) | "2020-01" |
| `location` | String | No | Geographic location | "Hershey, PA" |
| `is_primary` | Boolean | No | Primary affiliation flag | true |

#### profile.professional.previous_positions

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `title` | String | Yes | Position title | "Associate Professor" |
| `organization` | String | Yes | Organization name | "Johns Hopkins Medicine" |
| `department` | String | No | Department name | "Orthopedic Surgery" |
| `start_date` | String | No | Start date (YYYY-MM) | "2015-07" |
| `end_date` | String | No | End date (YYYY-MM) | "2019-12" |
| `location` | String | No | Geographic location | "Baltimore, MD" |

#### profile.professional.hospital_affiliations

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `hospital_name` | String | Yes | Hospital/facility name | "Penn State Milton S. Hershey Medical Center" |
| `affiliation_type` | String | No | Type of affiliation | "Admitting Privileges" |
| `department` | String | No | Hospital department | "Orthopedics" |
| `status` | String | No | Current status | "Active" |

#### profile.professional.practice_locations

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `practice_name` | String | Yes | Practice/clinic name | "Penn State Bone and Joint Institute" |
| `address` | String | No | Full street address | "30 Hope Drive, Suite 2400" |
| `city` | String | No | City | "Hershey" |
| `state` | String | No | State code | "PA" |
| `zip` | String | No | ZIP code | "17033" |
| `phone` | String | No | Phone number | "717-531-5638" |
| `accepts_new_patients` | Boolean | No | New patient flag | true |

### profile.education

#### profile.education.medical_school

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `institution` | String | Yes | Medical school name | "Harvard Medical School" |
| `degree` | String | No | Degree earned | "MD" |
| `graduation_year` | Integer | No | Year graduated | 1995 |
| `location` | String | No | School location | "Boston, MA" |

#### profile.education.residency

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `institution` | String | Yes | Residency institution | "Mayo Clinic" |
| `specialty` | String | No | Residency specialty | "Orthopedic Surgery" |
| `start_year` | Integer | No | Start year | 1995 |
| `end_year` | Integer | No | Completion year | 2000 |
| `chief_resident` | Boolean | No | Chief resident flag | true |

#### profile.education.fellowship

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `institution` | String | Yes | Fellowship institution | "Hospital for Special Surgery" |
| `specialty` | String | No | Fellowship focus | "Sports Medicine" |
| `start_year` | Integer | No | Start year | 2000 |
| `end_year` | Integer | No | Completion year | 2001 |

#### profile.education.certifications

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `board_name` | String | Yes | Certifying board | "American Board of Orthopedic Surgery" |
| `certification` | String | Yes | Certification name | "Board Certified in Orthopedic Surgery" |
| `initial_certification_year` | Integer | No | Year first certified | 2001 |
| `recertification_year` | Integer | No | Most recent recertification | 2021 |
| `expires` | String | No | Expiration date | "2031-12-31" |

### profile.industry_relationships

#### profile.industry_relationships.pharmaceutical

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `company_name` | String | Yes | Pharmaceutical company | "Pfizer Inc." |
| `relationship_type` | String | Yes | Type of relationship | "Consultant" |
| `description` | String | No | Relationship description | "Advisory board member for pain management" |
| `start_date` | String | No | Start date (YYYY-MM) | "2022-01" |
| `end_date` | String | No | End date if concluded | null |
| `compensation_range` | String | No | Compensation level | "$10,000-$25,000" |

#### profile.industry_relationships.medical_device

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `company_name` | String | Yes | Medical device company | "Stryker Corporation" |
| `relationship_type` | String | Yes | Type of relationship | "Product Development" |
| `products` | Array[String] | No | Related products | ["Knee Replacement System"] |
| `description` | String | No | Relationship details | "Design consultant for next-gen knee implant" |
| `patents` | Array[String] | No | Related patents | ["US10123456B2"] |

#### profile.industry_relationships.healthcare_startups

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `company_name` | String | Yes | Startup company name | "OrthoAI Inc." |
| `role` | String | Yes | Role/relationship | "Medical Advisor" |
| `equity_stake` | Boolean | No | Has equity flag | true |
| `description` | String | No | Company/role description | "AI-powered surgical planning platform" |

### profile.research

#### profile.research.publications

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `title` | String | Yes | Publication title | "Outcomes of Minimally Invasive Knee Replacement" |
| `journal` | String | No | Journal name | "Journal of Bone and Joint Surgery" |
| `year` | Integer | No | Publication year | 2023 |
| `authors` | Array[String] | No | Author list | ["Smith JA", "Jones BC"] |
| `pmid` | String | No | PubMed ID | "36543210" |
| `doi` | String | No | Digital Object Identifier | "10.1234/jbjs.2023.001" |
| `citation_count` | Integer | No | Number of citations | 45 |

#### profile.research.clinical_trials

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `trial_id` | String | Yes | ClinicalTrials.gov ID | "NCT04567890" |
| `title` | String | Yes | Trial title | "Robotic vs Traditional Knee Surgery" |
| `role` | String | No | Provider's role | "Principal Investigator" |
| `sponsor` | String | No | Trial sponsor | "NIH" |
| `status` | String | No | Current status | "Recruiting" |
| `start_date` | String | No | Trial start date | "2023-01" |
| `estimated_completion` | String | No | Expected completion | "2025-12" |

#### profile.research.grants

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `grant_title` | String | Yes | Grant title | "Novel Approaches to Joint Preservation" |
| `funding_source` | String | Yes | Funding organization | "National Institutes of Health" |
| `grant_number` | String | No | Grant identifier | "R01-AR-123456" |
| `amount` | Number | No | Grant amount | 2500000 |
| `period` | String | No | Grant period | "2022-2027" |
| `role` | String | No | Provider's role | "Principal Investigator" |

### profile.leadership

#### profile.leadership.board_memberships

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `organization` | String | Yes | Organization name | "American Academy of Orthopedic Surgeons" |
| `position` | String | Yes | Board position | "Board of Directors" |
| `committee` | String | No | Committee membership | "Quality Committee" |
| `term_start` | String | No | Term start date | "2022-01" |
| `term_end` | String | No | Term end date | "2025-12" |
| `organization_type` | String | No | Type of organization | "Professional Society" |

#### profile.leadership.advisory_roles

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `organization` | String | Yes | Organization name | "CMS Quality Payment Program" |
| `role` | String | Yes | Advisory role | "Technical Expert Panel Member" |
| `focus_area` | String | No | Area of expertise | "Orthopedic Quality Measures" |
| `start_date` | String | No | Role start date | "2023-06" |
| `compensation` | Boolean | No | Compensated position | false |

### profile.professional_activities

#### profile.professional_activities.speaking_engagements

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `event_name` | String | Yes | Event/conference name | "AAOS Annual Meeting 2024" |
| `presentation_title` | String | No | Presentation title | "Innovations in Joint Replacement" |
| `event_date` | String | No | Event date | "2024-03" |
| `location` | String | No | Event location | "San Francisco, CA" |
| `role` | String | No | Speaker role | "Keynote Speaker" |
| `sponsor` | String | No | Event sponsor | null |

#### profile.professional_activities.professional_societies

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `society_name` | String | Yes | Society name | "American Orthopedic Association" |
| `membership_type` | String | No | Type of membership | "Fellow" |
| `join_year` | Integer | No | Year joined | 2005 |
| `leadership_roles` | Array[String] | No | Leadership positions held | ["Committee Chair 2020-2022"] |

#### profile.professional_activities.awards_recognitions

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `award_name` | String | Yes | Award/recognition name | "Top Doctor - Orthopedic Surgery" |
| `awarding_organization` | String | Yes | Awarding body | "Castle Connolly" |
| `year` | Integer | No | Year received | 2023 |
| `description` | String | No | Award description | "Peer-nominated excellence in patient care" |

## Citations Array

Each citation object contains:

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `source_url` | String | Yes | Full URL of source | "https://www.pennstatehealth.org/doctors/john-smith" |
| `source_title` | String | Yes | Title/name of source | "Penn State Health - Provider Profile" |
| `source_type` | String | Yes | Type of source | "Hospital Website" |
| `accessed_at` | String | Yes | ISO 8601 timestamp | "2024-08-21T10:30:00Z" |
| `relevant_text` | String | No | Extracted text snippet | "Dr. Smith serves as Chief of Orthopedics..." |
| `confidence` | Number | Yes | Confidence score (0-1) | 0.95 |
| `data_points_extracted` | Array[String] | Yes | Fields extracted from this source | ["current_position", "hospital_affiliation"] |
| `source_credibility` | String | Yes | Credibility rating | "High" |

### Source Types
- `Hospital Website` - Official hospital/health system sites
- `University Website` - Academic institution sites
- `Professional Directory` - Medical directories (Doximity, Healthgrades)
- `Government Database` - NPPES, CMS, state boards
- `Research Database` - PubMed, ClinicalTrials.gov
- `News Article` - Healthcare news outlets
- `Professional Society` - Medical society websites
- `Company Website` - Pharmaceutical/device company sites

### Source Credibility Ratings
- `High` - Official institutional sources, government databases
- `Medium` - Professional directories, verified news sources
- `Low` - Unverified sources, user-generated content

## Metadata Object

| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `request_id` | String | Yes | Unique request identifier | "550e8400-e29b-41d4-a716-446655440000" |
| `scraped_at` | String | Yes | ISO 8601 timestamp | "2024-08-21T10:30:00Z" |
| `scrape_version` | String | Yes | Scraper version | "1.0.0" |
| `search_queries_used` | Array[String] | Yes | Actual search queries | ["John Smith MD Penn State orthopedic"] |
| `total_sources_found` | Integer | Yes | Number of sources found | 15 |
| `sources_used` | Integer | Yes | Sources included in profile | 8 |
| `sources_excluded` | Integer | Yes | Sources excluded (low quality) | 7 |
| `overall_confidence` | Number | Yes | Overall confidence (0-1) | 0.87 |
| `field_completeness` | Number | Yes | Percentage fields populated | 0.78 |
| `processing_time_seconds` | Number | Yes | Total processing time | 45.2 |
| `api_calls_made` | Integer | Yes | Number of API calls | 3 |
| `api_tokens_used` | Integer | Yes | Total tokens consumed | 2850 |
| `errors` | Array[Object] | No | Any errors encountered | [] |
| `warnings` | Array[String] | No | Any warnings generated | ["Low confidence for research_grants"] |
| `manual_review_required` | Boolean | Yes | Flag for manual review | false |
| `review_reasons` | Array[String] | No | Reasons for manual review | [] |

### Error Object Structure

| Field | Type | Description |
|-------|------|-------------|
| `error_type` | String | Type of error |
| `error_message` | String | Error description |
| `field_affected` | String | Which field was affected |
| `timestamp` | String | When error occurred |

## Quality Metrics

### Confidence Score Calculation
Confidence scores are calculated based on:
- Source credibility (40%)
- Number of corroborating sources (30%)
- Recency of information (20%)
- Specificity of match (10%)

### Field Completeness Calculation
- Required fields populated: 100%
- Optional high-value fields: 80%
- Optional low-value fields: 60%
- Overall score = weighted average

### Manual Review Triggers
Profiles flagged for manual review when:
- Overall confidence < 0.70
- Field completeness < 0.60
- Conflicting information detected
- High-value provider (C-suite, department chair)
- Unusual patterns detected

## Data Validation Rules

### NPI Validation
- Must be exactly 10 digits
- Must pass Luhn algorithm check
- Must exist in NPPES database

### Date Validation
- Dates in YYYY-MM-DD or YYYY-MM format
- Future dates allowed only for scheduled events
- Historical dates must be reasonable (e.g., graduation year > 1950)

### URL Validation
- Must be valid URL format
- Must be accessible (HTTP 200 response)
- Must not be blacklisted domain

### Text Field Validation
- No PII beyond professional information
- No patient information
- HTML stripped from all text fields
- Maximum field lengths enforced

## Update Frequency

### Real-time Fields
Updated with each scrape:
- Current positions
- Practice locations
- Contact information

### Periodic Update Fields
Updated quarterly or as detected:
- Board certifications
- Hospital affiliations
- Industry relationships

### Static Fields
Rarely change:
- Education history
- Historical positions
- Publications (append-only)

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2024-08-21 | Initial version |

---

*This data dictionary is maintained by the Data Analytics team. For questions or updates, contact DA team via Jira.*