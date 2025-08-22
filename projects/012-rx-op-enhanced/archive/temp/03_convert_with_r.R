#!/usr/bin/env Rscript

# Convert RDS files to Parquet using R directly
# JIRA: DA-167

library(arrow)
library(dplyr)
library(tidyr)

# Configuration
input_dir <- "mfg-spec-data"
output_dir <- "parquet-data"
sample_size <- 10  # Number of files to process for sample

# Create output directory
dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)

# Function to standardize column names
standardize_columns <- function(df, manufacturer) {
  # Get column names
  cols <- names(df)
  
  # Find manufacturer-specific columns
  mfg_pattern <- paste0(manufacturer, "_avg_")
  
  # Replace manufacturer name with generic 'mfg'
  cols <- gsub(mfg_pattern, "mfg_avg_", cols)
  
  # Apply new names
  names(df) <- cols
  return(df)
}

# Get list of RDS files
rds_files <- list.files(input_dir, pattern = "\\.rds$", full.names = TRUE)

# Take sample (first 10 files)
sample_files <- head(rds_files, sample_size)

cat("========================================\n")
cat("Converting RDS to Parquet (Sample)\n")
cat("========================================\n")
cat(sprintf("Found %d RDS files\n", length(rds_files)))
cat(sprintf("Processing sample of %d files\n", length(sample_files)))
cat("----------------------------------------\n\n")

# Process each file
all_data <- list()
total_rows <- 0

for (i in seq_along(sample_files)) {
  file_path <- sample_files[i]
  file_name <- basename(file_path)
  
  cat(sprintf("[%2d/%d] Processing %s\n", i, length(sample_files), file_name))
  
  tryCatch({
    # Read RDS file
    df <- readRDS(file_path)
    
    # Extract manufacturer and specialty from filename
    # Format: df_spec_manufacturer_specialty.rds
    parts <- strsplit(gsub("df_spec_|\\.rds", "", file_name), "_")[[1]]
    manufacturer <- parts[1]
    specialty <- paste(parts[-1], collapse = "_")
    
    cat(sprintf("  Manufacturer: %s, Specialty: %s\n", manufacturer, specialty))
    cat(sprintf("  Rows: %d, Columns: %d\n", nrow(df), ncol(df)))
    
    # Standardize columns
    df <- standardize_columns(df, manufacturer)
    
    # Add source columns
    df$source_file <- file_name
    df$source_manufacturer <- manufacturer
    df$source_specialty <- specialty
    
    # Add to list
    all_data[[i]] <- df
    total_rows <- total_rows + nrow(df)
    
    cat("  ✓ Processed successfully\n\n")
    
  }, error = function(e) {
    cat(sprintf("  ✗ Error: %s\n\n", e$message))
  })
}

# Combine all dataframes
cat("Combining dataframes...\n")
combined_df <- bind_rows(all_data)

cat(sprintf("Combined %d rows from %d files\n", nrow(combined_df), length(all_data)))

# Write to Parquet
output_file <- file.path(output_dir, "sample_combined.parquet")
cat(sprintf("\nWriting to %s...\n", output_file))

write_parquet(combined_df, output_file)

# Get file size
file_size_mb <- file.info(output_file)$size / (1024^2)

cat("\n========================================\n")
cat("CONVERSION COMPLETE\n")
cat("========================================\n")
cat(sprintf("Output file: %s\n", output_file))
cat(sprintf("Total rows: %d\n", nrow(combined_df)))
cat(sprintf("Total columns: %d\n", ncol(combined_df)))
cat(sprintf("File size: %.1f MB\n", file_size_mb))

# Show column names
cat("\nColumns:\n")
print(names(combined_df))

# Show manufacturer distribution
cat("\nManufacturer distribution:\n")
print(table(combined_df$source_manufacturer))