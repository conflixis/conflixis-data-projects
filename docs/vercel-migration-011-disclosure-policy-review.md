
# Vercel Migration Plan: 011-Disclosure-Policy-Review

**Status:** `Living Document - Partially Implemented`
**Last Updated:** 2025-08-12
**Author:** Gemini

## 1. Current State Analysis

This document outlines the strategy for migrating the **COI Disclosure Policy Review System** to a Vercel deployment. The project has evolved significantly from its initial conception.

### 1.1. Project Purpose

The project is a Python-based web application designed to:
1.  Extract Conflict of Interest (COI) data from Google BigQuery.
2.  Process the data using a sophisticated **Policy Decision Engine** to calculate risk scores based on configurable YAML policies.
3.  Leverage the **Google Gemini API** for advanced policy interpretation.
4.  Serve the processed data through an interactive web interface for compliance review.

### 1.2. Technology Stack

-   **Backend Framework:** **FastAPI** (migrated from Flask)
-   **API Adapter:** **Mangum** for Vercel serverless deployment.
-   **AI Integration:** **Google Gemini** for policy analysis.
-   **Data Processing:** Pandas, PyArrow
-   **Cloud Provider:** Google Cloud (BigQuery, GCS)
-   **Frontend:** Static HTML with a JavaScript API client.

### 1.3. Key Components

-   `api/index.py`: The Vercel serverless function entry point, using Mangum to adapt the FastAPI app.
-   `app/`: The core FastAPI application, containing a modular structure for API routes, services, and data loaders.
-   `app/api/services/policy_engine.py`: A comprehensive Policy Decision Engine that evaluates disclosures against configurable rules.
-   `app/frontend/`: Contains the static HTML, CSS, and JavaScript files for the user interface.
-   `public/`: The target directory for static assets for Vercel deployment (migration pending).
-   `config/`: Contains `coi-policies.yaml` and `coi-operational-thresholds.yaml`, which define the rules for the Policy Decision Engine.
-   `scripts/bigquery_pipeline.py`: The core ETL pipeline for data ingestion and processing.
-   `data/staging/disclosure_data.json`: The primary data artifact consumed by the frontend.
-   `requirements.txt`: Defines all Python dependencies, including `fastapi`, `mangum`, and `google-genai`.
-   `vercel.json`: Vercel deployment configuration file.

### 1.4. Authentication

The system currently has **no user authentication**. A planned integration with **Clerk** is pending.

## 2. Migration Goal & Architecture

The primary goal is to deploy the FastAPI application to **Vercel** as a serverless function and serve the static frontend. A key subsequent requirement is to introduce robust user authentication and management using **Clerk**.

### 2.1. Data Storage Architecture

Due to the ephemeral nature of Vercel's serverless filesystem, persistent data storage will be handled by **Google Cloud Storage (GCS)**.

-   **Data Generation:** A Vercel Cron Job will execute the `bigquery_pipeline.py` script on a schedule. This script will fetch and process the data, then upload the final `disclosure_data.json` artifact to a dedicated GCS bucket.
-   **Data Retrieval:** The FastAPI API, running as a Vercel serverless function, will fetch the `disclosure_data.json` from the GCS bucket upon request to serve data to the frontend.

This architecture ensures that the data is persistent and can be accessed by any Vercel function instance.

## 3. Migration Status & Next Steps

The migration is partially complete. Key components have been developed, but the final integration and deployment steps are pending.

### 3.1. Completed Tasks

-   **Framework Migration:** The backend has been successfully built with **FastAPI**, replacing the originally planned Flask framework.
-   **Project Restructuring:** The project has been structured to support Vercel deployment with an `api/` directory for the serverless function and a `public/` directory for static assets.
-   **Policy Decision Engine:** The core logic for evaluating disclosures against YAML-based rules is implemented in `app/api/services/policy_engine.py`.
-   **Vercel Configuration:** A `vercel.json` file has been created to define the build and routing configuration for the project.

### 3.2. Pending Tasks

-   **Authentication with Clerk:** The integration of Clerk for user management is the next major step.
-   **Static Asset Migration:** The frontend assets located in `app/frontend/` and the root directory need to be moved into the `public/` directory to be served by Vercel's static file handling.
-   **Data Pipeline Migration:** The `scripts/bigquery_pipeline.py` needs to be adapted to run as a **Vercel Cron Job** and integrate with GCS.

## 4. Deployment Plan

The deployment is planned in three phases.

### Phase 1: Authentication Integration (Pending)

1.  **Install Clerk SDK:**
    Uncomment `clerk-python>=0.7.0` in `requirements.txt`.

2.  **Configure Environment Variables:**
    Add `CLERK_API_KEY`, `CLERK_API_SECRET`, and `CLERK_JWT_KEY` to Vercel's project environment variables.

3.  **Protect FastAPI Routes:**
    Implement FastAPI dependencies to protect sensitive API endpoints, ensuring only authenticated users can access data.

    *Example in a FastAPI router:*
    ```python
    from fastapi import Depends, HTTPException
    from clerk_sdk import Clerk
    
    clerk = Clerk()
    
    @router.get("/data")
    async def get_sensitive_data(user: dict = Depends(clerk.get_session)):
        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")
        # Logic to return sensitive data
        pass
    ```

4.  **Create Sign-In/Sign-Up Flow:**
    Modify the frontend to include Clerk's pre-built UI components for user login and profile management.

### Phase 2: Data Pipeline & Frontend Migration (Pending)

1.  **Migrate Data Pipeline to Cron Job:**
    -   Adapt `scripts/bigquery_pipeline.py` to be executable as a Vercel Cron Job.
    -   The script will be modified to write the final `disclosure_data.json` artifact to a dedicated **Google Cloud Storage (GCS) bucket**.
    -   Update `vercel.json` to include the cron job schedule (e.g., daily).

2.  **Migrate Static Assets:**
    -   Move all `.html`, `.js`, and other static files from `app/frontend/` and the project root into the `public/` directory.
    -   Update any paths in the HTML files to reflect the new structure.

### Phase 3: Deployment to Vercel (Ready)

1.  **Connect Git Repository:**
    -   Connect the Git repository to a new Vercel project.
2.  **Configure Project Settings:**
    -   **Framework Preset:** FastAPI
    -   **Root Directory:** `projects/011-disclosure-policy-review`
3.  **Deploy:**
    -   Trigger a deployment. Vercel will use `vercel.json` to build the API, deploy static files, and set up routing.

## 5. Open Questions & Next Steps

-   **GCS Configuration:** Define the GCS bucket name and configure the necessary service account permissions for Vercel to read and write to it.
-   **Initial Action:** Begin by implementing **Phase 1**: integrating Clerk for authentication.
-   **Next Action:** Proceed with **Phase 2**: migrating the data pipeline and static assets.

## 6. Security Hardening with Semgrep

To enhance the security of the application, we will integrate **Semgrep**, a fast, open-source static analysis tool, into our development and deployment workflow. This will help us find and fix security vulnerabilities and code quality issues before they reach production.

### 6.1. Integration Steps

1.  **Add Semgrep Dependency:**
    Add `semgrep` to the development requirements file to ensure it's available for all developers.

    *In `requirements-dev.txt`:*
    ```
    semgrep>=1.0.0
    ```

2.  **Create Semgrep Configuration:**
    Create a `.semgrepignore` file in the project root (`projects/011-disclosure-policy-review/`) to exclude non-essential directories from scanning, such as `venv/` and `data/`.

    *`.semgrepignore`:*
    ```
    # Ignore virtual environment
    venv/
    env/

    # Ignore data files
    data/

    # Ignore test files if desired
    tests/
    ```

3.  **Initial Security Scan:**
    Run an initial scan using the default rules for Python and FastAPI to establish a security baseline.

    ```bash
    # Navigate to the project directory
    cd projects/011-disclosure-policy-review

    # Run the scan
    semgrep --config "p/python" --config "p/fastapi" .
    ```

4.  **CI/CD Integration (Future Step):**
    As a best practice, Semgrep should be integrated into the CI/CD pipeline to automatically scan every pull request. This can be done using Vercel Pre-deployment Commands or a dedicated CI service like GitHub Actions.

### 6.2. Benefits for This Project

-   **FastAPI Best Practices:** Semgrep has specific rules for FastAPI that can identify common security misconfigurations, such as missing authentication on sensitive routes.
-   **Dependency Scanning:** It can help identify vulnerabilities in third-party libraries listed in `requirements.txt`.
-   **Proactive Security:** By integrating static analysis, we can catch potential issues like injection vulnerabilities, insecure direct object references, and other common web application security risks early in the development process.

