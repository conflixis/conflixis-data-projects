# General Payments Data Dictionary (Select Fields)

This data dictionary describes selected fields from the General Payments table.

**BigQuery Table:** `data-analytics-389803.conflixis_agent.op_general_all_aggregate_static`

**Total Fields:** 15

| Field Name | Description | Sample Data | BigQuery Data Type |
|------------|-------------|-------------|-------------------|
| record_id | System-assigned identifier to the general transaction at the time of submission | 100000000241 | INTEGER |
| covered_recipient_profile_id | System generated unique identifier for covered recipient physician or covered recipient non-physician practitioner profile receiving the payment or other transfer of value. | 1000000378 | INTEGER |
| covered_recipient_npi | National Provider Identifier is a unique identification number for covered recipient physician or covered recipient non-physician practitioner (and not the NPI of a group the physician/non-physician practitioner belongs to). | 2495351826 | INTEGER |
| covered_recipient_first_name | First name of the covered recipient physician or covered recipient non-physician practitioner receiving the payment or transfer of value, as reported by the submitting entity. | John | STRING |
| covered_recipient_middle_name | Middle name of the covered recipient physician or covered recipient non-physician practitioner receiving the payment or transfer of value, as reported by the submitting entity. | A | STRING |
| covered_recipient_last_name | Last name of the covered recipient physician or covered recipient non-physician practitioner receiving the payment or transfer of value, as reported by the submitting entity. | Smith | STRING |
| covered_recipient_specialty_1 | Physician's or non-physician practitioner's specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021. | Allopathic & Osteopathic Physicians\|Obst etrics & Gynecology | STRING |
| applicable_manufacturer_or_applicable_gpo_making_payment_id | System generated unique identifier of the Applicable Manufacturer or Applicable Group Purchasing Organization (GPO) Making a payment or other transfer of value | 1000000049 | STRING |
| applicable_manufacturer_or_applicable_gpo_making_payment_name | The textual proper name of the applicable manufacturer or applicable GPO making the payment or other transfer of value | ABCDE Manufacturing | STRING |
| Company_Type | Whether the Manufacturer is a drug or device company | Drug | STRING |
| program_year | The year in which the payment occurred, as reported by submitting entity. | 2016 | INTEGER |
| date_of_payment | If a singular payment, then this is the actual date the payment was issued; if a series of payments or an aggregated set of payments, this is the date of the first payment. | 05/15/2016 | DATE |
| nature_of_payment_or_transfer_of_value | The nature of payment used to pay the covered recipient or to make the transfer of value. | Consulting Fee | STRING |
| name_of_drug_or_biological_or_device_or_medical_supply_1 | The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value. | Sample Drug 1 | STRING |
| total_amount_of_payment_usdollars | U.S. dollar amount of payment or other transfer of value to the recipient (manufacturer must convert to dollar currency if necessary) | 1978.00 | INTEGER |
| physician | Nested structure containing physician details and their facility affiliations | See nested fields below | STRUCT |
| physician.NPI | National Provider Identifier matching the covered_recipient_npi | 2495351826 | INTEGER |
| physician.physician_first_name | First name from physician affiliations data - DO NOT USE | John | STRING |
| physician.physician_last_name | Last name from physician affiliations data - DO NOT USE | Smith | STRING |
| physician.affiliations | Array of physician's facility affiliations | See nested array structure | ARRAY<STRUCT> |
| physician.affiliations.AFFILIATED_ID | Unique identifier for the affiliated facility | 123456 | STRING |
| physician.affiliations.AFFILIATED_NAME | Name of the affiliated facility | Memorial Hospital | STRING |
| physician.affiliations.AFFILIATED_FIRM_TYPE | Type of affiliated organization | Hospital | STRING |
| physician.affiliations.AFFILIATED_HQ_CITY | Headquarters city of affiliated facility | New York | STRING |
| physician.affiliations.AFFILIATED_HQ_STATE | Headquarters state of affiliated facility | NY | STRING |
| physician.affiliations.NETWORK_ID | Network identifier if facility is part of a network | 789012 | STRING |
| physician.affiliations.NETWORK_NAME | Name of the network | HealthCare Network | STRING |
| physician.affiliations.PRIMARY_AFFILIATED_FACILITY_FLAG | Indicates if this is the physician's primary affiliation | Y | STRING |
| physician.affiliations.AFFILIATION_STRENGTH | Strength or type of affiliation | Strong | STRING |