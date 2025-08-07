# DOJ Data Processing Pipeline

This repository contains a sequence of Python scripts designed to scrape, parse, and analyze DOJ-related data, as well as to interface with Google Cloud Storage.

## Script Sequence

### 1. doj001_sitemap_update.py
- **Purpose**: This script updates the sitemap for DOJ articles to be scraped.
- **Output**: Updated sitemap file.

### 2. doj002_fetch_parse_article.py
- **Purpose**: Fetches and parses articles based on the updated sitemap.
- **Output**: Parsed articles in a structured format.

### 3. doj003_openai_parse.py
- **Purpose**: Uses OpenAI to further parse and understand the content of the articles.
- **Output**: Refined and enriched article data.

### 4. doj004_match_supplier_op.py
- **Purpose**: Matches supplier data with Open Payments data.
- **Output**: Matched supplier and Open Payments data.

### 5. doj005_pp_typology.py
- **Purpose**: Creates a typology based on parsed articles and matched supplier data.
- **Output**: Typology data file.

### 6. doj006_copy_typology_gcs.py
- **Purpose**: Copies the generated typology data file to Google Cloud Storage.
- **Output**: Typology data file stored in Google Cloud Storage.

## Requirements
- Python 3.x
- Google Cloud Storage SDK
- OpenAI SDK

## Execution
Run the scripts in the order specified above to ensure the correct data flow.

# OpenAI Fine-Tuning and Model Execution Pipeline

This repository contains a sequence of Python scripts designed for fine-tuning and running an OpenAI model.

## Script Sequence

### 1. openai_create_finetuningjsonl.py
- **Purpose**: This script creates a JSONL file required for fine-tuning the OpenAI model.
- **Output**: JSONL file for fine-tuning.

### 2. openai_finetune_upload.py
- **Purpose**: Uploads the generated JSONL file and initiates the fine-tuning process.
- **Output**: Fine-tuned OpenAI model.

### 3. openai_rundojmodel.py
- **Purpose**: Runs the fine-tuned OpenAI model on DOJ-related data.
- **Output**: Processed and analyzed DOJ data.

## Requirements
- Python 3.x
- OpenAI SDK

## Execution
Run the scripts in the order specified above to ensure the correct data flow.

# Company Name Matching Script Version 3 (c_name_matching_v3.py)

## Table of Contents
1. [Description](#description)
2. [Requirements](#requirements)
3. [Installation](#installation)
4. [Configurable Parameters](#configurable-parameters)
5. [Main Features](#main-features)
6. [Usage](#usage)
7. [Error Handling and Progress Tracking](#error-handling-and-progress-tracking)
8. [File Structure](#file-structure)

## Description
This Python script is designed for the purpose of matching company names in a robust and efficient manner. It employs various string similarity algorithms to compare names and identify matches. The script can handle large datasets and is fault-tolerant, capable of resuming from where it left off in case of failure.

## Requirements
- Python 3.x
- pandas
- jellyfish
- rapidfuzz

## Installation
Clone the repository and install the required Python packages. You can install the packages using pip:

\```bash
pip install pandas jellyfish rapidfuzz
\```

## Configurable Parameters
- `STOPWORDS`: A set of common words that should be ignored during the matching process, such as 'Inc', 'LLC', 'Ltd', etc.

## Main Features
### Data Preprocessing
- Text cleaning: Removes extra spaces, converts text to lowercase.
- Special character removal: Deletes special characters from the text.
- Stopword removal: Ignores common words specified in the `STOPWORDS` set.

### String Matching
- Utilizes the Jellyfish library to employ various string comparison algorithms such as Jaro-Winkler, Levenshtein distance, etc.
- Also leverages the Rapidfuzz library for additional string matching functionalities.

## Usage
### Steps
1. **Import Libraries**: Make sure you've installed all required libraries.
2. **Set Parameters**: Configure the `STOPWORDS` set as per your requirements.
3. **Run Functions**: Execute the main processing functions to perform the matching.
4. **Execute Script**: Finally, run the script.

\```python
# Example snippet
import pandas as pd
import jellyfish
from rapidfuzz import fuzz, process

# Configure parameters
STOPWORDS = set([...])

# Run main functions and execute script
\```

## Error Handling and Progress Tracking
- The script includes progress tracking to monitor how much of the task is completed.
- In case of a crash or failure, the script is designed to resume from where it left off.

## File Structure
- **Imports**: Libraries required for the script.
- **Configurable Parameters**: Section for setting variables like `STOPWORDS`.
- **Main Processing Functions**: Core logic for data preprocessing and string matching.
- **Script Execution**: Where the script execution takes place.
