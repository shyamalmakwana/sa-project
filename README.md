# sa-project
# Clickstream Analytics Pipeline with Aiven Kafka, PostgreSQL, and OpenSearch

## Overview

This project creates a real-time clickstream data pipeline using Aiven managed services:

- Set up **Kafka** to collect clickstream events.
- Stored raw events in **PostgreSQL**.
- Aggregated session metrics and saved them in PostgreSQL.
- Streamed aggregated data to **OpenSearch** for analytics and visualization.

## What We Did

1. **Provisioned Aiven Services using Terraform:**
   - Created Kafka, PostgreSQL, and OpenSearch services on Aiven cloud.
   - Configured them with the appropriate plans and regions.

2. **Built a Python Kafka Consumer:**
   - Listens to the Kafka topic for clickstream events.
   - Inserts raw events into PostgreSQL.
   - Aggregates session data (page views, clicks, scrolls).
   - Periodically saves aggregated session metrics to PostgreSQL.

3. **(This does not work as expected) Connected to OpenSearch:**
   - Created an OpenSearch service in Terraform.
   - Planned to send data to OpenSearch for analytics (requires updating consumer to send data).

## Setup Instructions

1. **Install Terraform:**
   - Download and install Terraform from [terraform.io](https://terraform.io).
2. **Install Python dependencies**
    - ```bash
        pip install -r requirements.txt
    ```
2. **Set up Aiven API Token:**
   - Sign up at [Aiven](https://aiven.io) and create an API token.
   - Export the token as an environment variable:
     ```bash
     export AIVEN_API_TOKEN="your_api_token_here"
     ```

3. **Configure Terraform variables:**
   - Create a `terraform.tfvars` file with:
     ```hcl
     aiven_api_token = "your_api_token_here"
     aiven_project   = "your_project_name"
     aiven_cloud     = "google-europe-west1"  # or your preferred cloud region
     project_name    = "sa-test-assignment"
     ```

4. **Run Terraform commands to provision services:**
   ```bash
   terraform init
   terraform plan
   terraform apply