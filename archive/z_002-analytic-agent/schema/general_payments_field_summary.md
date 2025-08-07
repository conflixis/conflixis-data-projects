# Open Payments General Payments Table Schema

Total fields: 91

## Field Summary

| Field Name | BigQuery Type | Description |
|------------|---------------|-------------|
| Change_Type | STRING | An indicator showing if the payment record is New, Added, Changed, or Unchanged in the current pu... |
| Covered_Recipient_Type | STRING | An indicator showing if the recipient of the payment or transfer of value is a physician-covered ... |
| Teaching_Hospital_CCN | STRING | A unique identifying number (CMS Certification Number) of the Teaching Hospital receiving the pay... |
| Teaching_Hospital_ID | INT64 | The system generated a unique identifier of the Teaching Hospital receiving the payment or other ... |
| Teaching_Hospital_Name | STRING | The name of the Teaching Hospital receiving the payment or other transfer of value. The name disp... |
| Covered_Recipient_Profile_ID | INT64 | System generated unique identifier for covered recipient physician or covered recipient non-physi... |
| Covered_Recipient_NPI | INT64 | National Provider Identifier is a unique identification number for covered recipient physician or... |
| Covered_Recipient_First_Name | STRING | First name of the covered recipient physician or covered recipient non-physician practitioner rec... |
| Covered_Recipient_Middle_Name | STRING | Middle name of the covered recipient physician or covered recipient non-physician practitioner re... |
| Covered_Recipient_Last_Name | STRING | Last name of the covered recipient physician or covered recipient non-physician practitioner rece... |
| Covered_Recipient_Name_Suffix | STRING | Name suffix of the covered recipient physician or covered recipient non-physician practitioner re... |
| Recipient_Primary_Business_Street_Address_Line1 | STRING | The first line of the primary practice/business street address of the physician or teaching hospi... |
| Recipient_Primary_Business_Street_Address_Line2 | STRING | The second line of the primary practice/business street address of the physician or teaching hosp... |
| Recipient_City | STRING | The primary practice/business city of the physician or teaching hospital (covered recipient) rece... |
| Recipient_State | STRING | The primary practice/business state or territory abbreviation of the physician or teaching hospit... |
| Recipient_Zip_Code | STRING | The 9-digit zip code for the primary practice/business location of the physician or teaching hosp... |
| Recipient_Country | STRING | The primary practice/business address country name of the physician or teaching hospital (covered... |
| Recipient_Province | STRING | The primary practice/business province name of the physician (covered recipient) receiving the pa... |
| Recipient_Postal_Code | STRING | The international postal code for the primary practice/business location of the physician (covere... |
| Covered_Recipient_Primary_Type_1 | STRING | Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipi... |
| Covered_Recipient_Primary_Type_2 | STRING | Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipi... |
| Covered_Recipient_Primary_Type_3 | STRING | Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipi... |
| Covered_Recipient_Primary_Type_4 | STRING | Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipi... |
| Covered_Recipient_Primary_Type_5 | STRING | Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipi... |
| Covered_Recipient_Primary_Type_6 | STRING | Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipi... |
| Covered_Recipient_Specialty_1 | STRING | Physician's or non-physician practitioner's specialty chosen from the standardized "provider taxo... |
| Covered_Recipient_Specialty_2 | STRING | Physician's or non-physician practitioner's specialty chosen from the standardized "provider taxo... |
| Covered_Recipient_Specialty_3 | STRING | Physician's or non-physician practitioner's specialty chosen from the standardized "provider taxo... |
| Covered_Recipient_Specialty_4 | STRING | Physician's or non-physician practitioner's specialty chosen from the standardized "provider taxo... |
| Covered_Recipient_Specialty_5 | STRING | Physician's or non-physician practitioner's specialty chosen from the standardized "provider taxo... |
| Covered_Recipient_Specialty_6 | STRING | Physician's or non-physician practitioner's specialty chosen from the standardized "provider taxo... |
| Covered_Recipient_License_State_code1 | STRING | The state license number of the covered recipient physician or covered recipient non-physician pr... |
| Covered_Recipient_License_State_code2 | STRING | The state license number of the covered recipient physician or covered recipient non-physician pr... |
| Covered_Recipient_License_State_code3 | STRING | The state license number of the covered recipient physician or covered recipient non-physician pr... |
| Covered_Recipient_License_State_code4 | STRING | The state license number of the covered recipient physician or covered recipient non-physician pr... |
| Covered_Recipient_License_State_code5 | STRING | The state license number of the covered recipient physician or covered recipient non-physician pr... |
| Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name | STRING | The textual proper name of the submitting applicable manufacturer or submitting applicable GPO. |
| Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID | STRING | System generated unique identifier of the Applicable Manufacturer or Applicable Group Purchasing ... |
| Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name | STRING | The textual proper name of the applicable manufacturer or applicable GPO making the payment or ot... |
| Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State | STRING | State name of the submitting applicable manufacturer or submitting applicable GPO as provided in ... |
| Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country | STRING | Country name of the Submitting Applicable Manufacturer or Submitting Applicable Group Purchasing ... |
| Total_Amount_of_Payment_USDollars | FLOAT64 | U.S. dollar amount of payment or other transfer of value to the recipient (manufacturer must conv... |
| Date_of_Payment | DATE | If a singular payment, then this is the actual date the payment was issued; if a series of paymen... |
| Number_of_Payments_Included_in_Total_Amount | INT64 | The number of discrete payments being reported in the "Total Amount of Payment". |
| Form_of_Payment_or_Transfer_of_Value | STRING | The method of payment used to pay the covered recipient or to make the transfer of value. |
| Nature_of_Payment_or_Transfer_of_Value | STRING | The nature of payment used to pay the covered recipient or to make the transfer of value. |
| City_of_Travel | STRING | For "Travel and Lodging" payments, destination city where covered recipient traveled. |
| State_of_Travel | STRING | For "Travel and Lodging" payments, the destination state where the covered recipient traveled. |
| Country_of_Travel | STRING | For "Travel and Lodging" payments, the destination country where the covered recipient traveled. |
| Physician_Ownership_Indicator | STRING | Indicates whether the physician holds an ownership or investment interest in the applicable manuf... |
| Third_Party_Payment_Recipient_Indicator | STRING | Indicates if a payment or transfer of value was paid to a third party entity or individual at the... |
| Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value | STRING | The name of the entity that received the payment or other transfer of value. |
| Charity_Indicator | STRING | Indicates the third party entity that received the payment or other transfer of value is a charity. |
| Third_Party_Equals_Covered_Recipient_Indicator | STRING | An indicator showing the "Third Party" that received the payment or other transfer of value is a ... |
| Contextual_Information | STRING | Any free String, which the reporting entity deems helpful or appropriate regarding this payment o... |
| Delay_in_Publication_Indicator | STRING | An indicator showing if an Applicable Manufacturer/GPO is requesting a delay in the publication o... |
| Record_ID | INT64 | System-assigned identifier to the general transaction at the time of submission. |
| Dispute_Status_for_Publication | STRING | Indicates whether the payment or other transfer of value is disputed by the covered recipient or ... |
| Related_Product_Indicator | STRING | The indicator allows the applicable manufacturer or applicable GPO to select whether the payment ... |
| Covered_or_Noncovered_Indicator_1 | STRING | Each product listed in relation to the payment or other transfer of value, indicates if the produ... |
| Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1 | STRING | Each product listed in relation to the payment or other transfer of value, indicates if the produ... |
| Product_Category_or_Therapeutic_Area_1 | STRING | Provide the product category or therapeutic area for the covered drug, device, biological, or med... |
| Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 | STRING | The marketed name of the drug, device, biological, or medical supply. May report the marketed nam... |
| Associated_Drug_or_Biological_NDC_1 | STRING | The National Drug Code, if any, of the drug or biological associated with the payment or other tr... |
| Associated_Device_or_Medical_Supply_PDI_1 | STRING | The Primary Device Identifier, if any, of the covered device or covered medical supply associated... |
| Covered_or_Noncovered_Indicator_2 | STRING | For each product listed in relation to the payment or other transfer of value, indicates if the p... |
| Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2 | STRING | For each product listed in relation to the payment or other transfer of value, indicates if the p... |
| Product_Category_or_Therapeutic_Area_2 | STRING | Provide the product category or therapeutic area for the covered drug, device, biological, or med... |
| Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 | STRING | The marketed name of the drug, device, biological, or medical supply. May report the marketed nam... |
| Associated_Drug_or_Biological_NDC_2 | STRING | The National Drug Code, if any, of the drug or biological associated with the payment or other tr... |
| Associated_Device_or_Medical_Supply_PDI_2 | STRING | The Primary Device Identifier, if any, of the covered device or covered medical supply associated... |
| Covered_or_Noncovered_Indicator_3 | STRING | For each product listed in relation to the payment or other transfer of value, indicates if the p... |
| Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3 | STRING | For each product listed in relation to the payment or other transfer of value, indicates if the p... |
| Product_Category_or_Therapeutic_Area_3 | STRING | Provide the product category or therapeutic area for the covered drug, device, biological, or med... |
| Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 | STRING | The marketed name of the drug, device, biological, or medical supply. May report the marketed nam... |
| Associated_Drug_or_Biological_NDC_3 | STRING | The National Drug Code, if any, of the drug or biological associated with the payment or other tr... |
| Associated_Device_or_Medical_Supply_PDI_3 | STRING | The Primary Device Identifier, if any, of the covered device or covered medical supply associated... |
| Covered_or_Noncovered_Indicator_4 | STRING | For each product listed in relation to the payment or other transfer of value, indicates if the p... |
| Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4 | STRING | For each product listed in relation to the payment or other transfer of value, indicates if the p... |
| Product_Category_or_Therapeutic_Area_4 | STRING | Provide the product category or therapeutic area for the covered drug, device, biological, or med... |
| Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 | STRING | The marketed name of the drug, device, biological, or medical supply. May report the marketed nam... |
| Associated_Drug_or_Biological_NDC_4 | STRING | The National Drug Code, if any, of the drug or biological associated with the payment or other tr... |
| Associated_Device_or_Medical_Supply_PDI_4 | STRING | The Primary Device Identifier, if any, of the covered device or covered medical supply associated... |
| Covered_or_Noncovered_Indicator_5 | STRING | For each product listed in relation to the payment or other transfer of value, indicates if the p... |
| Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5 | STRING | For each product listed in relation to the payment or other transfer of value, indicates if the p... |
| Product_Category_or_Therapeutic_Area_5 | STRING | Provide the product category or therapeutic area for the covered drug, device, biological, or med... |
| Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 | STRING | The marketed name of the drug, device, biological, or medical supply. May report the marketed nam... |
| Associated_Drug_or_Biological_NDC_5 | STRING | The National Drug Code, if any, of the drug or biological associated with the payment or other tr... |
| Associated_Device_or_Medical_Supply_PDI_5 | STRING | The Primary Device Identifier, if any, of the covered device or covered medical supply associated... |
| Program_Year | STRING | The year in which the payment occurred, as reported by submitting entity. |
| Payment_Publication_Date | DATE | The predefined date when the payment or other transfer of value is scheduled to be published. |
