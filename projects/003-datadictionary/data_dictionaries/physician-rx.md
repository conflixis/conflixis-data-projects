# Physician Prescription Data Dictionary

This data dictionary describes the fields within the consolidated and optimized physician prescription claims table.

**BigQuery Table:** `data-analytics-389803.conflixis_agent.PHYSICIAN_RX_2020_2024`

**Total Fields:** 18

| Field Name | Description | Sample Data | BigQuery Data Type |
| :--- | :--- | :--- | :--- |
| **`NPI`** | A unique 10-digit National Provider Identifier for the prescribing physician. | `1609071448` | `INTEGER` |
| **`CLAIM_YEAR`** | The calendar year in which the prescription claim was processed. | `2023` | `NUMERIC` |
| **`CLAIM_MONTH`** | The full name of the month in which the prescription claim was processed. | `January` | `STRING` |
| **`PAYOR_ID`** | A system-assigned unique identifier for the insurance company or entity paying for the claim. | `98765` | `NUMERIC` |
| **`PAYOR_TYPE`** | The category or type of the payor (e.g., Commercial, Medicaid, Medicare). | `Commercial` | `STRING` |
| **`PAYOR_NAME`** | The proper name of the insurance company or entity paying for the claim. | `United Health` | `STRING` |
| **`BRAND_NAME`** | The marketed trade name of the prescribed drug as developed by the manufacturer. | `Krystexxa` | `STRING` |
| **`GENERIC_NAME`** | The official, non-proprietary name of the drug's active ingredient. | `pegloticase` | `STRING` |
| **`LABEL_NAME`** | The name appearing on the drug's label, which could be the brand or generic name. | `KRYSTEXXA` | `STRING` |
| **`MANUFACTURER`** | The name of the company that manufactures and/or markets the drug. | `Horizon Therapeutics` | `STRING` |
| **`NDC_CODE`** | The National Drug Code, a unique 11-digit product identifier for human drugs in the US. | `75987016001` | `STRING` |
| **`CHARGES`** | The total amount billed by the pharmacy to the payor for the prescription(s). | `15000.75` | `NUMERIC` |
| **`PAYMENTS`** | The total amount reimbursed by the payor to the pharmacy for the prescription(s). | `12500.50` | `NUMERIC` |
| **`PRESCRIPTIONS`** | The total count of prescriptions filled for the specified drug and physician within the time period. | `5` | `NUMERIC` |
| **`DAYS_SUPPLY`** | The aggregate number of days the supplied medication will last across all related prescriptions. | `150` | `NUMERIC` |
| **`UNIQUE_PATIENTS`** | The count of distinct patients receiving the specified drug from the physician. | `2` | `NUMERIC` |
| **`UNIQUE_DRUGS`** | The count of distinct drugs prescribed by the physician within the dataset. | `1` | `NUMERIC` |
| **`claim_date`** | A consolidated date field (YYYY-MM-01) created from `CLAIM_YEAR` and `CLAIM_MONTH` for partitioning. | `2023-01-01` | `DATE` |