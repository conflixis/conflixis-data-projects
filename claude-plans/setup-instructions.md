# Conflixis Analytics Repository Setup Instructions

## Overview

This repository is designed for collaborative data analytics work. Our team will analyze datasets stored in Google BigQuery, share findings via Google Drive, and use both Python and R for analysis.

## Prerequisites

### Required Software

1. **Git** - For version control
   - [Download Git](https://git-scm.com/downloads)

2. **Python 3.8+** - Primary analysis language
   - [Download Python](https://www.python.org/downloads/)

3. **Poetry** - Python dependency management
   - Installation: `curl -sSL https://install.python-poetry.org | python3 -`
   - Or via pip: `pip install poetry`

4. **R (Optional)** - For R users
   - [Download R](https://cran.r-project.org/)
   - Recommended: [RStudio](https://posit.co/download/rstudio-desktop/)

5. **Google Cloud SDK** - For BigQuery access
   - [Install gcloud CLI](https://cloud.google.com/sdk/docs/install)

### Required Access

1. **GitHub Repository Access**
   - Request access to: https://github.com/conflixis/conflixis-analytics.git

2. **Google BigQuery**
   - Ensure you have access to the project datasets
   - Authentication will be handled via gcloud CLI

3. **Google Drive**
   - Access to the shared findings folder (request link from team lead)

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/conflixis/conflixis-analytics.git
cd conflixis-analytics
```

### 2. Python Environment Setup

```bash
# Initialize poetry in the project (if not already done)
poetry init

# Install dependencies
poetry install

# Activate the virtual environment
poetry shell
```

### 3. Google Cloud Authentication

```bash
# Authenticate with Google Cloud
gcloud auth login

# Set default project (replace with actual project ID)
gcloud config set project YOUR_PROJECT_ID

# Authenticate for application default credentials
gcloud auth application-default login
```

### 4. Jupyter Setup

```bash
# Install Jupyter in the poetry environment
poetry add jupyter ipykernel

# Install the kernel for Jupyter
python -m ipykernel install --user --name=conflixis-analytics

# Start Jupyter
jupyter notebook
```

### 5. R Setup (For R Users)

```r
# In R or RStudio, install required packages
install.packages(c("bigrquery", "tidyverse", "reticulate"))

# For BigQuery authentication in R
library(bigrquery)
bq_auth()
```

## Project Structure

```
conflixis-analytics/
├── claude-plans/          # Planning and documentation
├── projects/              # Analysis projects (notebooks, scripts, outputs)
│   └── examples/         # Example projects with Python and R scripts
├── src/                  # Shared Python modules
│   ├── data/            # Data loading utilities
│   ├── analysis/        # Analysis functions
│   └── visualization/   # Plotting utilities
├── common/              # Shared secrets and config (git-ignored)
├── data/                # Local data cache (git-ignored)
├── pyproject.toml       # Poetry configuration
├── .gitignore          # Git ignore rules
└── README.md           # Project overview
```

## Development Workflow

### For Python Users

1. Always work within the Poetry environment:
   ```bash
   poetry shell
   ```

2. Add new dependencies via Poetry:
   ```bash
   poetry add pandas numpy matplotlib seaborn
   poetry add google-cloud-bigquery
   ```

3. Create your project folder in the `projects/` directory

4. Share reusable code in the `src/` directory

5. Store project-specific secrets in the `common/` folder

### For R Users

1. Create your project folder in the `projects/` directory alongside Python notebooks

2. Use the `bigrquery` package for BigQuery access

3. Consider using `reticulate` to interface with Python code when needed

4. Store R scripts in your project folder with `.R` extension

### Version Control Guidelines

1. **Commit frequently** with clear messages
2. **Create branches** for major analyses:
   ```bash
   git checkout -b analysis/customer-segmentation
   ```

3. **Never commit**:
   - Large data files (use `.gitignore`)
   - API keys or credentials
   - Personal configuration files

4. **Always commit**:
   - Analysis code and notebooks
   - Documentation
   - Small reference datasets

### Sharing Findings

1. **Organize your project**:
   - Keep all project files in a dedicated folder under `projects/`
   - Include notebooks, scripts, and outputs in your project folder

2. **Export notebooks** to HTML/PDF for sharing:
   ```bash
   jupyter nbconvert --to html notebook.ipynb
   ```

3. **Upload to Google Drive**:
   - Create a folder for your analysis
   - Include: exported notebooks, visualizations, summary documents

4. **Document in repository**:
   - Add a README in your project folder
   - Link to Google Drive resources

## Common Tasks

### Querying BigQuery from Python

```python
from google.cloud import bigquery

client = bigquery.Client()

query = """
SELECT *
FROM `project.dataset.table`
LIMIT 100
"""

df = client.query(query).to_dataframe()
```

### Querying BigQuery from R

```r
library(bigrquery)

con <- dbConnect(
  bigrquery::bigquery(),
  project = "your-project-id",
  dataset = "your-dataset"
)

df <- dbGetQuery(con, "SELECT * FROM table LIMIT 100")
```

## Troubleshooting

### Poetry Issues

- If `poetry: command not found`, ensure Poetry is in your PATH
- Run `poetry config virtualenvs.in-project true` to keep environments local

### BigQuery Authentication

- Run `gcloud auth list` to check active accounts
- Ensure your account has BigQuery Data Viewer permissions

### Jupyter Kernel Issues

- List kernels: `jupyter kernelspec list`
- Remove old kernel: `jupyter kernelspec uninstall kernel-name`

## Getting Help

- **Technical Issues**: Create an issue in the GitHub repository
- **Access Requests**: Contact the project administrator
- **Analysis Questions**: Use the team Slack channel

## Next Steps

1. Complete the setup process
2. Run the example notebook in `projects/examples/`
3. Create your own project folder under `projects/`
4. Set up any project-specific credentials in `common/`
5. Start exploring the available datasets
6. Create your first analysis branch