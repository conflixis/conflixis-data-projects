
    SELECT *
    FROM `data-analytics-389803.conflixis_data_projects.op_general_all_aggregate_static_optimized`
    WHERE LOWER(applicable_manufacturer_or_applicable_gpo_making_payment_name) LIKE '%alcon%'
    AND (LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%systane%' OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%eysuvis%' OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%rocklatan%' OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%azopt%' OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%rocklatan%' OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%pataday%' OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%simbrinza%' OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%rhopressa%' OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%rhopressa%' OR LOWER(name_of_drug_or_biological_or_device_or_medical_supply_1) LIKE '%systane complete%' OR name_of_drug_or_biological_or_device_or_medical_supply_1 IS NULL OR TRIM(name_of_drug_or_biological_or_device_or_medical_supply_1) = '')
    AND covered_recipient_npi IS NOT NULL
    ORDER BY covered_recipient_npi
    