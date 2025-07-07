-- BigQuery SQL to create the Open Payments General Payments table
-- Table: data-analytics-389803.conflixis_datasources.op_general_all

-- CREATE OR REPLACE TABLE `data-analytics-389803.conflixis_datasources.op_general_all`
(
  -- Change tracking fields
  Change_Type STRING OPTIONS(description="An indicator showing if the payment record is New, Added, Changed, or Unchanged in the current publication compared to the previous publication."),
  
  -- Recipient identification fields
  Covered_Recipient_Type STRING OPTIONS(description="An indicator showing if the recipient of the payment or transfer of value is a physician-covered recipient or non-physician practitioner or a teaching hospital."),
  Teaching_Hospital_CCN STRING OPTIONS(description="A unique identifying number (CMS Certification Number) of the Teaching Hospital receiving the payment or other transfer of value."),
  Teaching_Hospital_ID INT64 OPTIONS(description="The system generated a unique identifier of the Teaching Hospital receiving the payment or other transfer of value."),
  Teaching_Hospital_Name STRING OPTIONS(description="The name of the Teaching Hospital receiving the payment or other transfer of value. The name displayed is as listed in CMS teaching hospital list under Hospital name."),
  Covered_Recipient_Profile_ID INT64 OPTIONS(description="System generated unique identifier for covered recipient physician or covered recipient non-physician practitioner profile receiving the payment or other transfer of value."),
  Covered_Recipient_NPI INT64 OPTIONS(description="National Provider Identifier is a unique identification number for covered recipient physician or covered recipient non-physician practitioner (and not the NPI of a group the physician/non-physician practitioner belongs to)."),
  
  -- Recipient name fields
  Covered_Recipient_First_Name STRING OPTIONS(description="First name of the covered recipient physician or covered recipient non-physician practitioner receiving the payment or transfer of value, as reported by the submitting entity."),
  Covered_Recipient_Middle_Name STRING OPTIONS(description="Middle name of the covered recipient physician or covered recipient non-physician practitioner receiving the payment or transfer of value, as reported by the submitting entity."),
  Covered_Recipient_Last_Name STRING OPTIONS(description="Last name of the covered recipient physician or covered recipient non-physician practitioner receiving the payment or transfer of value, as reported by the submitting entity."),
  Covered_Recipient_Name_Suffix STRING OPTIONS(description="Name suffix of the covered recipient physician or covered recipient non-physician practitioner receiving the payment or transfer of value, as reported by the submitting entity."),
  
  -- Recipient address fields
  Recipient_Primary_Business_Street_Address_Line1 STRING OPTIONS(description="The first line of the primary practice/business street address of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value."),
  Recipient_Primary_Business_Street_Address_Line2 STRING OPTIONS(description="The second line of the primary practice/business street address of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value."),
  Recipient_City STRING OPTIONS(description="The primary practice/business city of the physician or teaching hospital (covered recipient) receiving the payment or other transfer of value."),
  Recipient_State STRING OPTIONS(description="The primary practice/business state or territory abbreviation of the physician or teaching hospital (covered recipient) receiving the payment or transfer of value, if the primary practice/business address is in United States."),
  Recipient_Zip_Code STRING OPTIONS(description="The 9-digit zip code for the primary practice/business location of the physician or teaching hospital (covered recipient) receiving the payment or transfer of value."),
  Recipient_Country STRING OPTIONS(description="The primary practice/business address country name of the physician or teaching hospital (covered recipient) receiving the payment or transfer of value."),
  Recipient_Province STRING OPTIONS(description="The primary practice/business province name of the physician (covered recipient) receiving the payment or other transfer of value, if the primary practice/business address is outside the United States, and if applicable."),
  Recipient_Postal_Code STRING OPTIONS(description="The international postal code for the primary practice/business location of the physician (covered recipient) receiving the payment or other transfer of value, if the primary practice/business address is outside the United States."),
  
  -- Recipient type and specialty fields
  Covered_Recipient_Primary_Type_1 STRING OPTIONS(description="Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient)."),
  Covered_Recipient_Primary_Type_2 STRING OPTIONS(description="Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021."),
  Covered_Recipient_Primary_Type_3 STRING OPTIONS(description="Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021."),
  Covered_Recipient_Primary_Type_4 STRING OPTIONS(description="Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021."),
  Covered_Recipient_Primary_Type_5 STRING OPTIONS(description="Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021."),
  Covered_Recipient_Primary_Type_6 STRING OPTIONS(description="Primary type of medicine practiced by the physician or Non-Physician Practitioner (covered recipient). Note: OP began accepting this field in PY2021."),
  
  -- Recipient specialty fields
  Covered_Recipient_Specialty_1 STRING OPTIONS(description="Physician's or non-physician practitioner's specialty chosen from the standardized 'provider taxonomy' code list. Note: OP began accepting this field in PY2021."),
  Covered_Recipient_Specialty_2 STRING OPTIONS(description="Physician's or non-physician practitioner's specialty chosen from the standardized 'provider taxonomy' code list. Note: OP began accepting this field in PY2021."),
  Covered_Recipient_Specialty_3 STRING OPTIONS(description="Physician's or non-physician practitioner's specialty chosen from the standardized 'provider taxonomy' code list. Note: OP began accepting this field in PY2021."),
  Covered_Recipient_Specialty_4 STRING OPTIONS(description="Physician's or non-physician practitioner's specialty chosen from the standardized 'provider taxonomy' code list. Note: OP began accepting this field in PY2021."),
  Covered_Recipient_Specialty_5 STRING OPTIONS(description="Physician's or non-physician practitioner's specialty chosen from the standardized 'provider taxonomy' code list. Note: OP began accepting this field in PY2021."),
  Covered_Recipient_Specialty_6 STRING OPTIONS(description="Physician's or non-physician practitioner's specialty chosen from the standardized 'provider taxonomy' code list. Note: OP began accepting this field in PY2021."),
  
  -- Recipient license state fields
  Covered_Recipient_License_State_code1 STRING OPTIONS(description="The state license number of the covered recipient physician or covered recipient non-physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 license states, if a physician is licensed in multiple states."),
  Covered_Recipient_License_State_code2 STRING OPTIONS(description="The state license number of the covered recipient physician or covered recipient non-physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 license states, if a physician is licensed in multiple states."),
  Covered_Recipient_License_State_code3 STRING OPTIONS(description="The state license number of the covered recipient physician or covered recipient non-physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 license states, if a physician is licensed in multiple states."),
  Covered_Recipient_License_State_code4 STRING OPTIONS(description="The state license number of the covered recipient physician or covered recipient non-physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 license states, if a physician is licensed in multiple states."),
  Covered_Recipient_License_State_code5 STRING OPTIONS(description="The state license number of the covered recipient physician or covered recipient non-physician practitioner, which is a 2-letter state abbreviation; the record may include up to 5 license states, if a physician is licensed in multiple states."),
  
  -- Manufacturer/GPO fields
  Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name STRING OPTIONS(description="The textual proper name of the submitting applicable manufacturer or submitting applicable GPO."),
  Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID STRING OPTIONS(description="System generated unique identifier of the Applicable Manufacturer or Applicable Group Purchasing Organization (GPO) Making a payment or other transfer of value."),
  Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name STRING OPTIONS(description="The textual proper name of the applicable manufacturer or applicable GPO making the payment or other transfer of value."),
  Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State STRING OPTIONS(description="State name of the submitting applicable manufacturer or submitting applicable GPO as provided in Open Payments."),
  Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country STRING OPTIONS(description="Country name of the Submitting Applicable Manufacturer or Submitting Applicable Group Purchasing Organization (GPO) as provided in Open Payments."),
  
  -- Payment details fields
  Total_Amount_of_Payment_USDollars FLOAT64 OPTIONS(description="U.S. dollar amount of payment or other transfer of value to the recipient (manufacturer must convert to dollar currency if necessary)."),
  Date_of_Payment DATE OPTIONS(description="If a singular payment, then this is the actual date the payment was issued; if a series of payments or an aggregated set of payments, this is the date of the first payment to the covered recipient in this PY."),
  Number_of_Payments_Included_in_Total_Amount INT64 OPTIONS(description="The number of discrete payments being reported in the 'Total Amount of Payment'."),
  Form_of_Payment_or_Transfer_of_Value STRING OPTIONS(description="The method of payment used to pay the covered recipient or to make the transfer of value."),
  Nature_of_Payment_or_Transfer_of_Value STRING OPTIONS(description="The nature of payment used to pay the covered recipient or to make the transfer of value."),
  
  -- Travel fields
  City_of_Travel STRING OPTIONS(description="For 'Travel and Lodging' payments, destination city where covered recipient traveled."),
  State_of_Travel STRING OPTIONS(description="For 'Travel and Lodging' payments, the destination state where the covered recipient traveled."),
  Country_of_Travel STRING OPTIONS(description="For 'Travel and Lodging' payments, the destination country where the covered recipient traveled."),
  
  -- Indicator fields
  Physician_Ownership_Indicator STRING OPTIONS(description="Indicates whether the physician holds an ownership or investment interest in the applicable manufacturer; this indicator is limited to physician's ownership, not the physician's family members' ownership."),
  Third_Party_Payment_Recipient_Indicator STRING OPTIONS(description="Indicates if a payment or transfer of value was paid to a third party entity or individual at the request of or on behalf of a covered recipient (physician or teaching hospital)."),
  Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value STRING OPTIONS(description="The name of the entity that received the payment or other transfer of value."),
  Charity_Indicator STRING OPTIONS(description="Indicates the third party entity that received the payment or other transfer of value is a charity."),
  Third_Party_Equals_Covered_Recipient_Indicator STRING OPTIONS(description="An indicator showing the 'Third Party' that received the payment or other transfer of value is a Covered Recipient."),
  
  -- Additional information fields
  Contextual_Information STRING OPTIONS(description="Any free String, which the reporting entity deems helpful or appropriate regarding this payment or other transfer of value."),
  Delay_in_Publication_Indicator STRING OPTIONS(description="An indicator showing if an Applicable Manufacturer/GPO is requesting a delay in the publication of a payment or other transfer of value."),
  Record_ID INT64 OPTIONS(description="System-assigned identifier to the general transaction at the time of submission."),
  Dispute_Status_for_Publication STRING OPTIONS(description="Indicates whether the payment or other transfer of value is disputed by the covered recipient or not."),
  Related_Product_Indicator STRING OPTIONS(description="The indicator allows the applicable manufacturer or applicable GPO to select whether the payment or other transfer of value is related to one or more product(s) (drugs, devices, biologicals, or medical supplies). If the payment was not made in relation to a product, select 'No'. If the payment was related to one or more products, select 'Yes'."),
  
  -- Product 1 fields
  Covered_or_Noncovered_Indicator_1 STRING OPTIONS(description="Each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non-covered product per the covered product definition in the Open Payments final rule."),
  Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1 STRING OPTIONS(description="Each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply."),
  Product_Category_or_Therapeutic_Area_1 STRING OPTIONS(description="Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value."),
  Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1 STRING OPTIONS(description="The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value."),
  Associated_Drug_or_Biological_NDC_1 STRING OPTIONS(description="The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes."),
  Associated_Device_or_Medical_Supply_PDI_1 STRING OPTIONS(description="The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021."),
  
  -- Product 2 fields
  Covered_or_Noncovered_Indicator_2 STRING OPTIONS(description="For each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non-covered product per the covered product definition in the Open Payments final rule."),
  Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2 STRING OPTIONS(description="For each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply."),
  Product_Category_or_Therapeutic_Area_2 STRING OPTIONS(description="Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value."),
  Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2 STRING OPTIONS(description="The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value."),
  Associated_Drug_or_Biological_NDC_2 STRING OPTIONS(description="The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes."),
  Associated_Device_or_Medical_Supply_PDI_2 STRING OPTIONS(description="The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021."),
  
  -- Product 3 fields
  Covered_or_Noncovered_Indicator_3 STRING OPTIONS(description="For each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non-covered product per the covered product definition in the Open Payments final rule."),
  Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3 STRING OPTIONS(description="For each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply."),
  Product_Category_or_Therapeutic_Area_3 STRING OPTIONS(description="Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value."),
  Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3 STRING OPTIONS(description="The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value."),
  Associated_Drug_or_Biological_NDC_3 STRING OPTIONS(description="The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes."),
  Associated_Device_or_Medical_Supply_PDI_3 STRING OPTIONS(description="The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021."),
  
  -- Product 4 fields
  Covered_or_Noncovered_Indicator_4 STRING OPTIONS(description="For each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non-covered product per the covered product definition in the Open Payments final rule."),
  Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4 STRING OPTIONS(description="For each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply."),
  Product_Category_or_Therapeutic_Area_4 STRING OPTIONS(description="Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value."),
  Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4 STRING OPTIONS(description="The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value."),
  Associated_Drug_or_Biological_NDC_4 STRING OPTIONS(description="The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes."),
  Associated_Device_or_Medical_Supply_PDI_4 STRING OPTIONS(description="The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021."),
  
  -- Product 5 fields
  Covered_or_Noncovered_Indicator_5 STRING OPTIONS(description="For each product listed in relation to the payment or other transfer of value, indicates if the product is a covered or non-covered product per the covered product definition in the Open Payments final rule."),
  Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5 STRING OPTIONS(description="For each product listed in relation to the payment or other transfer of value, indicates if the product is a drug, device, biological, or medical supply."),
  Product_Category_or_Therapeutic_Area_5 STRING OPTIONS(description="Provide the product category or therapeutic area for the covered drug, device, biological, or medical supply listed in relation to the payment or other transfer of value."),
  Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5 STRING OPTIONS(description="The marketed name of the drug, device, biological, or medical supply. May report the marketed name of up to five products (drugs, devices, biologicals, or medical supplies) associated with the payment or other transfer of value."),
  Associated_Drug_or_Biological_NDC_5 STRING OPTIONS(description="The National Drug Code, if any, of the drug or biological associated with the payment or other transfer of value (if applicable); the record may report up to 5 codes."),
  Associated_Device_or_Medical_Supply_PDI_5 STRING OPTIONS(description="The Primary Device Identifier, if any, of the covered device or covered medical supply associated with the payment or other transfer of values (if applicable); the record may report up to 5 codes. Note: OP Program began collecting PDI information during PY 2021."),
  
  -- Program year and publication date
  Program_Year STRING OPTIONS(description="The year in which the payment occurred, as reported by submitting entity."),
  Payment_Publication_Date DATE OPTIONS(description="The predefined date when the payment or other transfer of value is scheduled to be published.")
)
OPTIONS(
  description="Open Payments General Payments data containing payments or other transfers of value made by applicable manufacturers and GPOs to covered recipients (physicians and teaching hospitals). This table includes all payment years from 2016 onwards."
);

-- Add table partitioning by Program_Year if needed
-- PARTITION BY Program_Year

-- Create indexes on commonly queried fields
-- CREATE INDEX idx_covered_recipient_npi ON `data-analytics-389803.conflixis_datasources.op_general_all` (Covered_Recipient_NPI);
-- CREATE INDEX idx_manufacturer_name ON `data-analytics-389803.conflixis_datasources.op_general_all` (Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name);
-- CREATE INDEX idx_payment_date ON `data-analytics-389803.conflixis_datasources.op_general_all` (Date_of_Payment);
-- CREATE INDEX idx_program_year ON `data-analytics-389803.conflixis_datasources.op_general_all` (Program_Year);