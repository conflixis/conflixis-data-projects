# General Payments Data Dictionary

This data dictionary describes the fields in the General Payments table.

**Total Fields:** 103

| Field Name | Description | Sample Data | Data Type | Format | Max Length |
|------------|-------------|-------------|-----------|---------|------------|
| Change_Type | An indicator showing if the payment record is New, Added, Changed, or Unchanged in the current publication compared to the previous publication. NEW - To identify "new" records added from the end of the previous submission deadline until the current submission period deadline date ADDED - To identify records that were not eligible at the time of previous publication, which is eligible for current publication. CHANGED - To identify previously published records modified after the last publication. UNCHANGED - To identify previously published records that remain "unchanged" in current publication. | NEW | VARCHAR | - | 20 |
| Covered_Recipient_Type | An indicator showing if the recipient of the payment or transfer of value is a physician -covered recipient or non - physician practitioner or a teaching hospital. | Physician | VARCHAR | - | 50 |
| Teaching_Hospital_CCN | A unique identifying number (CMS Certification Number) of the Teaching Hospital receiving the payment or other transfer of value. | 330024 | VARCHAR | - | - |
| Teaching_Hospital_ID | The system generated a unique identifier of the Teaching Hospital receiving the payment or other transfer of value. | 1000000999 | NUMBER( | number | - |
| Teaching_Hospital_Name | The name of the Teaching Hospital receiving the payment or other transfer of value. The name displayed is as listed in CMS teaching hospital list under Hospital name. | Healthy Heart Hospital | - | - | 22 |
| Covered_Recipient_Profile_ID | System generated unique identifier for covered recipient physician or covered recipient non- physician practitioner profile receiving the payment or other transfer of value. | 1000000378 | NUMBER | number | - |
| Covered_Recipient_NPI | National Provider Identifier is a unique identification number for covered recipient physician or covered recipient non-physician practitioner (and not the NPI of a group the physician/non -physician practitioner belongs to). | 2495351826 | NUMBER | number | - |
| Covered_Recipient_First_Name | First name of the covered recipient physician or covered recipient non- physician practitioner receiving the payment or transfer of value, as reported by the submitting entity. | John | VARCHAR | - | 20 |
| Covered_Recipient_Middle_Name | Middle name of the covered recipient physician or covered recipient non- physician practitioner receiving the payment or transfer of value, as reported by the submitting entity. | 20 | A | - | 20 |
| Covered_Recipient_Last_Name | Last name of the covered recipient physician or covered recipient non- physician practitioner receiving the payment or transfer of value, as reported by the submitting entity. | Smith | VARCHAR | - | 35 |
| Covered_Recipient_Name_Suffix | Name suffix of the covered recipient physician or covered recipient non- physician practitioner receiving the payment or transfer of value, as reported by the submitting entity. | III | VARCHAR2(5) | string | 5 |
| Recipient_Primary_Business_Street_Address_Line1 | The first line of the primary practice/business street address of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value. | 7500 Main Street | VARCHAR2(55) | string | 55 |
| Recipient_Primary_Business_Street_Address_Line2 | The second line of the primary practice/business street address of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value. | Suite 100 | VARCHAR2(55) | string | 55 |
| Recipient_City | The primary practice/business city of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value. | Baltimore | VARCHAR | - | 40 |
| Recipient_State | The primary practice/business state or territory abbreviation of the physician or teaching hospital (covered recipient) receiving the payment or transfer of value. | MD | CHAR(2) | string | 2 |
| Recipient_Zip_Code | The 9 -digit zip code for the primary practice/business location of the physician or teaching hospital (covered recipient) receiving the payment or transfer of value. | 21244-3712 | VARCHAR2(10) | number | 10 |
| Recipient_Country | The primary practice/business address country name of the physician or teaching hospital (covered recipient) receiving the payment or transfer of value. | US | VARCHAR2(100) | string | 100 |
| Recipient_Province | The primary practice/business province name of the physician (covered recipient) receiving the payment or other transfer of value, if the primary practice/business address is outside the United States, and if applicable. | Manitoba | VARCHAR | - | 20 |
| Recipient_Postal_Code | The international postal code for the primary practice/business location of the physician (covered recipient) receiving the payment or other transfer of value, if the primary practice/business address is outside the United States | 5600098 | VARCHAR2(20) | string | 20 |
| Covered_Recipient_Primary_Type_1 | Primary type of medicine practiced by the physician or Non- Physician Practitioner (covered recipient). | Medical Doctor (MD)/ Physician Assistant (PA) | VARCHAR | string | 100 |
| Covered_Recipient_Primary_Type_2 | Primary type of medicine practiced by the physician or Non - Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021. | Medical Doctor (MD)/ Physician Assistant (PA) | VARCHAR | string | 100 |
| Covered_Recipient_Primary_Type_3 | Primary type of medicine practiced by the physician or Non- Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021. | Medical Doctor (MD)/ Physician Assistant (PA) | VARCHAR | - | 100 |
| Covered_Recipient_Primary_Type_4 | Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021. | Medical Doctor (MD)/ Physician Assistant (PA) | VARCHAR | - | 100 |
| Covered_Recipient_Primary_Type_5 | Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021. | Medical Doctor (MD)/Physician Assistant (PA) | VARCHAR2(100) | string | 100 |
| Covered_Recipient_Primary_Type_6 | Primary type of medicine practiced by the physician or Non- Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021. | Medical Doctor (MD)/ | VARCHAR | - | 100 |
| Covered_Recipient_Specialty_1 | Physician's or non -physician practitioner's specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021. | Allopathic & Osteopathic Physicians|Obst etrics & Gynecology | VARCHAR | - | 300 |
| Covered_Recipient_Specialty_2 | Physician's or non -physician practitioner's specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021. | Allopathic & Osteopathic Physicians|Obst etrics & Gynecology | VARCHAR | - | 300 |
| Covered_Recipient_Specialty_3 | Physician's or non -physician practitioner's specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021. | Allopathic & Osteopathic Physicians|Obst etrics & Gynecology | VARCHAR | - | 300 |
| Covered_Recipient_Specialty_4 | Physician's or non -physician practitioner's specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021. | Allopathic & Osteopathic Physicians|Obst etrics & Gynecology | VARCHAR | - | 300 |
| Covered_Recipient_Specialty_5 | Physician's or non -physician practitioner's specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021. | Allopathic & Osteopathic Physicians|Obstetrics & Gynecology | VARCHAR2(300) | string | 300 |
| Covered_Recipient_Specialty_6 | Physician's or non -physician practitioner's specialty chosen from the standardized "provider taxonomy" code list. Note: OP began accepting this field in PY2021. | Allopathic & Osteopathic Physicians|Obst etrics & Gynecology | VARCHAR | - | 300 |
| Covered_Recipient_License_State_code1 | The state license number of the covered recipient physician or covered recipient non-physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 license states. | MD | CHAR(2) | string | 2 |
| Covered_Recipient_License_State_code2 | The state license number of the covered recipient physician or covered recipient non -physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 | VA | CHAR(2) | string | 2 |
| Covered_Recipient_License_State_code3 | The state license number of the covered recipient physician or covered recipient non -physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 | CA | CHAR(2) | string | 2 |
| Covered_Recipient_License_State_code4 | The state license number of the covered recipient physician or covered recipient non -physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 | NY | CHAR(2) | string | 2 |
| Covered_Recipient_License_State_code5 | e5 The state license number of the covered recipient physician or covered recipient non -physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 | FL | CHAR(2) | string | 2 |
| Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name | The textual proper name of the submitting applicable manufacturer or submitting applicable GPO. | ABCDE Manufacturing | VARCHAR2(100) | string | 100 |
| Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID | System generated unique identifier of the Applicable Manufacturer or Applicable Group Purchasing Organization (GPO) Making a payment or other transfer of value | 1000000049 | VARCHAR2(26) | string | 26 |
| Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name | The textual proper name of the applicable manufacturer or applicable GPO making the payment or other transfer of value | ABCDE Manufacturing | VARCHAR2(100) | string | 100 |
| Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State | State name of the submitting applicable manufacturer or submitting applicable GPO as provided in Open Payments | CA | CHAR(2) | string | 2 |
| Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country | Country name of the Submitting Applicable Manufacturer or Submitting Applicable Group Purchasing Organization (GPO) as provided in Open Payments | United States | VARCHAR2(100) | string | 100 |
| Total_Amount_of_Payment_US | Dollars U.S. dollar amount of payment or other transfer of value to the recipient (manufacturer must convert to dollar currency if necessary) | 1978.00 | NUMBER | decimal | 12 |
| Date_of_Payment | If a singular payment, then this is the actual date the payment was issued; if a series of payments or an aggregated set of payments, this is the date of the first payment. | 05/15/2016 | DATE | DATE | 12 |
| Number_of_Payments_Included_in_Total_Amount | The number of discrete payments being reported in the "Total Amount of Payment". | 1 | NUMBER(3,0) | number | 3 |
| Form_of_Payment_or_Transfer_of_Value | The method of payment used to pay the covered recipient or to make the transfer of value. | In-kind items and services | VARCHAR2(100) | string | 100 |
| Nature_of_Payment_or_Transfer_of_Value | The nature of payment used to pay the covered recipient or to make the transfer of value. | Consulting Fee | VARCHAR2(200) | string | 200 |
| City_of_Travel | For "Travel and Lodging" payments, destination city where covered recipient traveled. | San Diego | VARCHAR | - | 40 |
| State_of_Travel | For "Travel and Lodging" payments, the destination state where the covered recipient traveled. | CA | CHAR(2) | string | 2 |
| Country_of_Travel | For "Travel and Lodging" payments, the destination country where the covered recipient traveled. | United States | VARCHAR2(100) | string | 100 |
| Physician_Ownership_Indicator | Indicates whether the physician holds an ownership or investment interest in the applicable manufacturer. | No | CHAR(3) | string | 3 |
| Third_Party_Payment_Recipient_Indicator | Indicates if a payment or transfer of value was paid to a third party entity or individual at the request of or on behalf of a covered recipient (physician or teaching hospital). | Entity | VARCHAR | - | 50 |
| Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value | The name of the entity that received the payment or other transfer of value. | EDCBA Manufacturing | VARCHAR2(50) | string | 50 |
| Charity_Indicator | Indicates the third party entity that received the payment or other transfer of value is a charity. | No | CHAR(3) | string | 3 |
| Third_Party_Equals_Covered_Recipient_Indicator | An indicator showing the "Third Party" that received the payment or other transfer of value is a Covered Recipient. | No | CHAR(3) | string | 3 |
| t_Indicator | An indicator showing the "Third Party" that received the | - | - | - | - |
| Contextual_Information | Any free String, which the reporting entity deems helpful or appropriate regarding this payment or other transfer of value. | Transfer made to promote the use of the product | VARCHAR | string | 500 |
| Delay_in_Publication_Indicator | An indicator showing if an Applicable Manufacturer/GPO is requesting a delay in the publication of a payment or other transfer of value | No | CHAR(3) | string | 3 |
| Record_ID | System-assigned identifier to the general transaction at the time of submission | 100000000241 | NUMBER(38,0) | number | 38 |
| Dispute_Status_for_Publication | Indicates whether the payment or other transfer of value is disputed by the covered recipient or not | Yes | CHAR(3) | string | 3 |
| Related_Product_Indicator | The indicator allows the applicable manufacturer or applicable GPO to select whether the payment or other transfer of value is related to one or more product(s). | Yes | CHAR(3) | string | 3 |
| Covered_or_Noncovered_Indicator_1 | ator_1 Each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non- covered product per the covered product definition in the Open Payments final rule. | Covered | VARCHAR | - | 100 |
| Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1 | Each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply. | Drug | VARCHAR | string | 100 |
| Product_Category_or_Therapeutic_Area_1 | Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value. | Endocrinology | VARCHAR2(100) | string | 100 |
| Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 | The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value. | Sample Drug 1 | VARCHAR | - | 500 |
| Associated_Drug_or_Biological_NDC_1 | The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value. | 3698-7272-61 | VARCHAR2(100) | string | 12 |
| Associated_Device_or_Medical_Supply |  | - | - | - | - |
| _PDI_1 | The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021. | 100 | 0084865700026 | - | 100 |
| Covered_or_Noncovered_Indicator_2 | ator_2 For each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non- covered product per the covered product definition in the Open Payments final rule. | Covered | VARCHAR | - | 100 |
| Indicate_Drug_or_Biological_or | _Devic | - | - | - | - |
| e_or_Medical_Supply_2 | For each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply. | Drug | - | - | 29 |
| Product_Category_or_Therapeutic_Area_2 | eutic_Ar ea_2 | Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value. | VARCHAR | string | 100 |
| Name_of_Drug_or_Biological_or_Devi |  | - | - | - | - |
| ce_or_Medical_Supply_2 | The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value. | Sample Drug 2 | VARCHAR | - | 500 |
| Associated_Drug_or_Biological_NDC_2 | _NDC_2 The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes | string | 3698- | string | 12 |
| Associated_Device_or_Medical_Supply_PDI_2 | The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021. | 100 | 0084865700026 | - | 100 |
| Covered_or_Noncovered_Indicator_3 | For each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non- covered product per the covered product definition in the Open Payments final rule. | Covered | VARCHAR | - | 100 |
| Indicate_Drug_or_Biological_or | _Devic | - | - | - | - |
| e_or_Medical_Supply_3 | For each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply. | Drug | VARCHAR | - | 100 |
| Product_Category_or_Therapeutic_Area_3 | eutic_Ar ea_3 | Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value. | - | - | 30 |
| Name_of_Drug_or_Biological_or_Devi |  | - | - | - | - |
| ce_or_Medical_Supply_3 | The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value. | Sample Drug 3 | VARCHAR | - | 500 |
| Associated_Drug_or_Biological_NDC_3 | _NDC_3 The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes | 12 | 36987 | - | 12 |
| Associated_Device_or_Medical_Supply_PDI_3 | The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021. | 100 | 0084865700026 | - | 100 |
| Covered_or_Noncovered_Indicator_4 | For each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non- covered product per the covered product definition in the Open Payments final rule. | Covered | VARCHAR | - | 100 |
| Indicate_Drug_or_Biological_or | _Devic | - | - | - | - |
| e_or_Medical_Supply_4 | For each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply. | Biological | VARCHAR | - | 100 |
| Product_Category_or_Therapeutic_Area_4 | eutic_Ar ea_4 | Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value. | VARCHAR | - | 100 |
| Name_of_Drug_or_Biological_or_Devi |  | - | - | - | - |
| ce_or_Medical_Supply_4 | The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value. | Sample Drug 4 | - | - | 31 |
| Associated_Drug_or_Biological_NDC_4 | The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes | 12 | 3698- | - | 12 |
| Associated_Device_or_Medical_Supply |  | - | - | - | - |
| _PDI_4 | The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021. | 100 | 0084865700026 | - | 100 |
| Covered_or_Noncovered_Indicator_5 | ator_5 For each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non- covered product per the covered product definition in the Open Payments final rule. | Covered | VARCHAR | - | 100 |
| Indicate_Drug_or_Biological_or | _Devic | - | - | - | - |
| e_or_Medical_Supply_5 | For each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply. | Device | VARCHAR | - | 100 |
| Product_Category_or_Therapeutic_Area_5 | eutic_Ar ea_5 | Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value. | VARCHAR | - | 100 |
| Name_of_Drug_or_Biological_or_Devi |  | - | - | - | - |
| ce_or_Medical_Supply_5 | The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value. | Sample Drug 5 | VARCHAR | - | 500 |
| Associated_Drug_or_Biological_NDC_5 | _NDC_5 The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes | Sample Data | - | - | 32 |
| Associated_Device_or_Medical_Supply |  | - | - | - | - |
| _PDI_5 | The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021. | 100 | 0084865700026 | - | 100 |
| Program_Year | The year in which the payment occurred, as reported by | - | - | - | - |
| Payment_Publication_Date | The predefined date when the payment or other transfer of value is scheduled to be published | 06/30/2017 | DATE | DATE MM/DD/YYYY | 12 |

## Summary Statistics

### Data Types

- **CHAR**: 13 fields
- **DATE**: 2 fields
- **NUMBER**: 15 fields
- **VARCHAR2**: 73 fields

### Field Categories

- **Covered Recipient**: 24 fields
- **Teaching Hospital**: 5 fields
- **Payment**: 7 fields
- **Product/Drug**: 40 fields
- **Travel**: 3 fields
- **Third Party**: 4 fields
- **Other**: 20 fields