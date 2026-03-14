
# Credit-Risk-Assessment-Pipeline-P2
## Project Overview

The Credit Risk Assessment Data Pipeline is an end-to-end Data Engineering project designed to process raw credit application datasets and transform them into analytics-ready datasets for credit risk analysis.

The pipeline follows the Medallion Architecture (Bronze → Silver → Gold) using AWS and Databricks Lakehouse platform. Raw data is ingested from AWS S3, processed using AWS Glue and PySpark, stored in Delta Lake tables, and structured for credit risk analytics.

This system helps financial institutions analyze loan risk, borrower behavior, and financial indicators to support better loan approval decisions.

## Project Objectives

Build a scalable batch ETL pipeline for credit risk datasets

Process raw datasets stored in AWS S3

Perform data cleaning and transformation using PySpark

Implement Medallion Architecture

Generate credit risk analytics datasets

Automate pipeline execution using Apache Airflow

## System Architecture

The pipeline follows a modern Lakehouse Data Engineering Architecture.

Pipeline Flow

AWS S3 (Raw CSV Data)
        ↓
AWS Glue (CSV → Parquet Conversion)
        ↓
Databricks Bronze Layer (Raw Tables)
        ↓
Databricks Silver Layer (Data Cleaning & Transformation)
        ↓
Databricks Gold Layer (Risk Analytics)
        ↓
Business Insights & Dashboards

## Technology Stack
Technology	Purpose
AWS S3	Data Lake Storage
AWS Glue	ETL Processing (CSV → Parquet)
Databricks	Data Engineering Platform
PySpark	Distributed Data Processing
Delta Lake	Optimized Storage Format
Apache Airflow	Pipeline Orchestration
SQL	Analytical Queries
Git / GitHub	Version Control

## Dataset

Dataset Used:

Credit Risk Dataset (Kaggle)

The dataset contains financial and credit-related information about loan applicants.

Dataset includes:

Applicant Profiles

Credit Applications

Credit History

Loan Details

Economic Indicators

Example Raw Storage Path:

s3://credit-risk-data/raw/credit_data/YYYY/MM/DD/

### Data Pipeline Layers
### Bronze Layer – Raw Data Ingestion

Purpose:

Store raw credit data with minimal transformation.

Tasks performed:

Read CSV datasets from AWS S3

Convert datasets into Parquet format using AWS Glue

Load data into Delta Bronze tables

Preserve original raw datasets

Example Bronze Tables:

Bronze Tables
bronze_applicant_profiles
bronze_credit_applications
bronze_credit_history
bronze_loan_details
bronze_economic_indicators

### Silver Layer – Data Cleaning and Transformation

Purpose:

Create clean and standardized datasets for analytics.

Transformations include:

Removing duplicate records

Handling missing values

Standardizing categorical values

Converting data types

Performing data validation checks

Joining related datasets

Feature Engineering Examples:

Debt-to-Income Ratio

Credit Score Bands

Risk Categories

Example Silver Tables:

Silver Tables
applicant_cleaned
economic_indicators_cleaned
risk_feature_dataset
data_quality_summary

### Gold Layer – Risk Analytics

Purpose:

Create analytics-ready datasets for credit risk analysis.

Example analytics outputs:

Credit risk classification

Borrower risk category

Expected default probability

Loan risk metrics

These datasets enable deeper analysis of borrower behavior and loan risk patterns.

## Business Insights Generated

The pipeline enables multiple insights for financial institutions.

## Descriptive Analytics

Total loan applications

Credit score distribution

Loan approval patterns

Applicant income distribution

Loan amount statistics

### Diagnostic Analytics

Debt-to-income ratio impact

Risk category analysis

Default risk patterns

Credit score trends

## Strategic Analytics

High-risk borrower identification

Loan portfolio risk analysis

Risk segmentation of applicants

## Pipeline Automation

Pipeline execution is automated using Apache Airflow.

Airflow DAG triggers pipelines sequentially:

Bronze Pipeline
        ↓
Silver Pipeline
        ↓
Gold Pipeline

Schedule:

Daily Batch Execution

Workflow tasks include:

Bronze ingestion

Silver transformation

Gold analytics generation

Data quality validation

## Data Quality and Monitoring

The pipeline includes multiple validation checks:

Record count validation

Schema validation

Null value detection

Duplicate detection

Data anomaly checks

Errors and pipeline issues are stored in log tables.

Alerts are triggered for pipeline failures.

## My Role in the Project

Role:

Data Engineer – AWS Glue & Data Integration

Responsibilities:

Designed AWS S3 data lake structure

Implemented AWS Glue Crawlers for schema discovery

Created Glue ETL jobs to convert CSV datasets to Parquet

Managed metadata using AWS Glue Data Catalog

Configured IAM roles for secure AWS integration

Integrated AWS data sources with Databricks pipelines

Assisted in pipeline orchestration using Apache Airflow

## Key Project Outcomes

Built a scalable Lakehouse data pipeline

Implemented Medallion Architecture

Developed reliable ETL workflows

Generated analytics-ready datasets

Automated data processing pipelines

## Future Improvements

Implement real-time streaming ingestion

Apply machine learning models for credit risk prediction

Deploy dashboards using Power BI or Streamlit

Implement CI/CD pipelines for data engineering workflows

## Conclusion

This project demonstrates how a modern data engineering pipeline can transform raw credit datasets into structured analytics datasets using the Lakehouse architecture.

The system integrates AWS S3, AWS Glue, Databricks, PySpark, Delta Lake, and Airflow to deliver scalable and efficient credit risk data processing for financial analytics.