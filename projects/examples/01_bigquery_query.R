# BigQuery Query Example - R Script
# This script demonstrates querying the PHYSICIANS_OVERVIEW table using R

# Load required libraries
library(bigrquery)
library(tidyverse)
library(DBI)

# Set up authentication using service account key from environment variable
# Make sure GOOGLE_APPLICATION_CREDENTIALS is set in common/.env
Sys.setenv(GOOGLE_APPLICATION_CREDENTIALS = Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

# Project and dataset information
PROJECT_ID <- "data-analytics-389803"
DATASET_ID <- "CONFLIXIS_309340"
TABLE_ID <- "PHYSICIANS_OVERVIEW"

# Authenticate with BigQuery
bq_auth(path = Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

# Method 1: Using DBI interface
con <- dbConnect(
  bigrquery::bigquery(),
  project = PROJECT_ID,
  dataset = DATASET_ID
)

# Query the table
query <- "SELECT * FROM `data-analytics-389803.CONFLIXIS_309340.PHYSICIANS_OVERVIEW` LIMIT 100"

# Execute query
df <- dbGetQuery(con, query)

# Display first few rows
print("First 6 rows of PHYSICIANS_OVERVIEW:")
print(head(df))

# Display structure
print("\nTable structure:")
print(str(df))

# Basic summary statistics
print("\nSummary statistics:")
print(summary(df))

# Method 2: Using bq_project_query (alternative approach)
df2 <- bq_project_query(PROJECT_ID, query) %>%
  bq_table_download()

# Verify both methods return same data
print("\nVerifying both methods return same results:")
print(paste("Rows match:", nrow(df) == nrow(df2)))
print(paste("Columns match:", ncol(df) == ncol(df2)))

# Save results to CSV (optional)
# write_csv(df, "physicians_overview_sample.csv")

# Disconnect
dbDisconnect(con)

print("\nQuery completed successfully!")