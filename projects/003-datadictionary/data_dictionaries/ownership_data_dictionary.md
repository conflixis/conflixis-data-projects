# Physician Ownership and Investment Interest Data Dictionary

This data dictionary describes the fields in the Physician Ownership and Investment Interest table.

**Total Fields:** 26

| Field Name | Description | Sample Data | Data Type | Format | Max Length |
|------------|-------------|-------------|-----------|---------|------------|
| Physician_NPI | National Provider Identifier is a unique identification number for covered recipient physician (and not the NPI of | a group the physician  belongs to).  2495351826 | NUMBER(10,0) | number | 10 |
| Physician_Middle_Name | Middle name of the physician (covered recipient) with the ownership or investment interest being reported A VARCH AR2(20) string 20 | - | - | - | - |
| Physician_Last_Name | Last name of the physician (covered recipient) with | the ownership or investment interest being repo... | VARCHAR2(35) | string | 35 |
| Physician_Name_Su | ffix Name suffix of the physician (covered recipient) with | the ownership or investment interest being repo... | VARCHAR2(5) | string | 5 |
| Recipient_Primary_Business_S | treet_Address_Line1 The first line of the primary practice street address of the physician with the ownership or investment interest being reported 7500  Security | Blvd. | VARCHAR2(55) | string | 55 |
| Recipient_Primary_Business_S | treet_Address_Line2 The second line of the primary practice street address of the physician with the ownership or investment | interest being reported Suite 100 | VARCHAR2(55) | string | 55 |
| Recipient_City | The primary practice city of the physician with the | ownership or investment interest being reported... | VARCHAR2(40) | string | 40 |
| Recipient_State | The primary practice/business state or territory abbreviation of the physician with the ownership or investment interest being reported, if the primary | practice/business address is in the United S ta... | CHAR(2) | string | 2 |
| Recipient_Zip_Code | The 9-digit zip code for the primary practice location of the physician with the ownership or investment interest being reported, if the primary practice address is in the United States 21244 -3712... | - | - | - | - |
| Recipient_Country | The primary practice/business address country name of the physician with the ownership or investment interest being reported US VARCH AR2(100) string 100 | - | - | - | - |
| Recipient_Province | The primary practice/business province name of the physician with the ownership or investment interest being reported, if the primary practice/business address is outside the United S tates, and if... | - | - | - | - |
| Recipient_Postal_Code | The international postal code for the primary practice/business location of the physician with the ownership or investment interest being reported, if the primary practice/business address is outsi... | - | - | - | - |
| Physician_Primary_Type | The p rimary type of medicine practiced by the physician covered recipient with the ownership or investment interest being reported Doctor of Dentistry (DDS) VARCH AR2(50) string 50 | - | - | - | - |
| Physician_Specialty | Physician's single-specialty chosen from the standardized "provider taxonomy" code list Allopathic & Osteopathic Physicians \|Obstetrics & Gynecology VARCH AR2(300) string 300 | - | - | - | - |
| Record_ID | Open Payments system-generated unique identifier for the | ownership payment record 10000000052 | NUMBER(38,0) | num | ber |
| Program_Year | The year in which the ownership/investment interest 107 Name  Description Sample  Data Data Type Form at Max Length | - | - | - | - |
| Total_Amount_Invested_USDollar | s The dollar amount the physician or immediate family member has invested in the applicable manufacturer or | applicable GPO during the PY, in US dollars 600... | NUMBER(12,2) | decimal | 12 |
| Value_of | _Interest The cumulative value of ownership or investment interest held by the physician or immediate family member in the | applicable manufacturer or applicable GPO, in U... | NUMBER(12,2) | decimal | 12 |
| Terms_of_Interest | Description of any applicable terms of the ownership or investment interest Terms of interest are standard VARCH AR2(500) string 500 | - | - | - | - |
| Submitting_Applicable_M | anuf acturer_or_Applicable_GPO_ Name The t extual prop er name of either the submitting applicable manufacturer or applicable GPO ABCDE Manufacturing VARCH AR2(100) string 100 | - | - | - | - |
| Applicable_M | anufacturer_or_App licable_GPO_Making_Payment_ID Open Payments ID of either the submitting applicable | manufacturer or applicable GPO 1000000049 | NUMBER(38,0) | num | ber |
| Applicable_M | anufacturer_or_App licable_GPO_Making_Payment_N ame The t extual prop er name of either the submitting applicable manufacturer or applicable GPO EDCBA VARCH AR2(100) string 100 | - | - | - | - |
| Applicable_M | anufacturer_or_App licable_GPO_Making_Payment_St ate State name of either the submitting applicable | manufacturer or applicable GPO VA | CHAR(2) | string | 2 |
| Applicable_M | anufacturer_or_App licable_GPO_Making_Payment_C ountry Coun try name of the submitting applicable manufacturer or applicable GPO US VARCH AR2(100) string 100 | - | - | - | - |
| Dispute_Status_for_Publication | Indicates whe ther the own ership or investment interest is 108 Name  Description Sample  Data Data Type Form at Max Length Interes t_Held_by_Physician_or_a n_Immediate_Family_Me mber An indicator ... | - | - | - | - |
| Payment_Publication_Date | The predefined date when the ownership or investment interest is scheduled to be published 06/30/2016  DATE Date MM /DD/ 109 | - | - | - | - |

## Summary Statistics

### Data Types

- **CHAR**: 2 fields
- **NUMBER**: 5 fields
- **Not specified**: 14 fields
- **VARCHAR2**: 5 fields

### Field Categories

- **Payment**: 2 fields
- **Other**: 24 fields
