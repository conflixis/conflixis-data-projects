# BigQuery Example Script for R Users
# This script demonstrates how to connect to Google BigQuery and perform basic queries

# Load required libraries
library(bigrquery)
library(tidyverse)
library(DBI)

# Set your project ID
PROJECT_ID <- "your-project-id"  # TODO: Update this with your actual project ID

# Authenticate with BigQuery
# This will open a browser window for authentication if needed
bq_auth()

# Alternative: authenticate using a service account key
# bq_auth(path = "path/to/your/service-account-key.json")

# Connect to BigQuery
con <- dbConnect(
  bigrquery::bigquery(),
  project = PROJECT_ID,
  dataset = "your_dataset_name"  # Optional: specify a default dataset
)

# Example 1: Query a public dataset
# Using the same USA names dataset as the Python example
query1 <- "
SELECT 
    name,
    SUM(number) as total_count,
    COUNT(DISTINCT year) as years_popular
FROM `bigquery-public-data.usa_names.usa_1910_current`
WHERE year >= 2000
GROUP BY name
ORDER BY total_count DESC
LIMIT 10
"

# Execute query and get results
df1 <- bq_project_query(PROJECT_ID, query1) %>%
  bq_table_download()

# Display results
print("Top 10 Names Since 2000:")
print(df1)

# Example 2: Using dbplyr for dplyr-style queries
# First, create a reference to a BigQuery table
# tbl_ref <- tbl(con, "your_table_name")

# You can then use dplyr verbs
# result <- tbl_ref %>%
#   filter(year >= 2010) %>%
#   group_by(category) %>%
#   summarise(
#     total = sum(value, na.rm = TRUE),
#     avg = mean(value, na.rm = TRUE)
#   ) %>%
#   collect()

# Example 3: Parameterized query using glue
library(glue)

min_year <- 2010
limit <- 20

query2 <- glue("
SELECT 
    year,
    gender,
    SUM(number) as total_births
FROM `bigquery-public-data.usa_names.usa_1910_current`
WHERE year >= {min_year}
GROUP BY year, gender
ORDER BY year, gender
LIMIT {limit}
")

df2 <- bq_project_query(PROJECT_ID, query2) %>%
  bq_table_download()

# Example 4: Visualization
library(ggplot2)

# Create a plot from the results
if (nrow(df2) > 0) {
  p <- ggplot(df2, aes(x = as.factor(year), y = total_births, fill = gender)) +
    geom_bar(stat = "identity", position = "dodge") +
    theme_minimal() +
    labs(
      title = "Total Births by Year and Gender",
      x = "Year",
      y = "Total Births",
      fill = "Gender"
    ) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
  
  print(p)
  
  # Save the plot
  # ggsave("../outputs/births_by_gender_r.png", p, width = 10, height = 6, dpi = 300)
}

# Example 5: Working with your own data
# Template for querying your datasets
your_query <- glue("
SELECT 
    *
FROM `{PROJECT_ID}.your_dataset.your_table`
LIMIT 100
")

# Uncomment to run:
# df_your_data <- bq_project_query(PROJECT_ID, your_query) %>%
#   bq_table_download()

# Best practices for cost control
# 1. Always use LIMIT during development
# 2. Use bq_table_meta() to check table size before querying
# 3. Consider using bq_table_download(max_results = 1000) to limit results

# Example: Check table metadata
# table_meta <- bq_table_meta(
#   bq_table(PROJECT_ID, "dataset_name", "table_name")
# )
# print(paste("Table size:", table_meta$numBytes / 1e9, "GB"))

# Saving results
# write_csv(df1, "../data/top_names_r.csv")

# Close connection when done
dbDisconnect(con)

print("Script completed successfully!")