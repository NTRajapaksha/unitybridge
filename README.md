# 🌉 UnityBridge: Cross-Platform Data Lakehouse Architecture
### *Zero-Copy Data Sharing & Analytics: Azure Databricks (Unity Catalog) ↔ Microsoft Fabric (OneLake & Direct Lake)*

[![Azure Databricks](https://img.shields.io/badge/Azure_Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://azure.microsoft.com/en-us/products/databricks)
[![Microsoft Fabric](https://img.shields.io/badge/Microsoft_Fabric-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://www.microsoft.com/en-us/microsoft-fabric)
[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![MLflow](https://img.shields.io/badge/MLflow-0194E2?style=for-the-badge&logo=mlflow&logoColor=white)](https://mlflow.org/)

---

## 📺 Project Demo Video
> 🎥 **Walkthrough & Pipeline Demo:**  
> [![Watch Demo Video](https://img.shields.io/badge/YouTube-Watch%20Demo%20Video-red?style=for-the-badge&logo=youtube)](YOUR_DEMO_VIDEO_LINK_HERE)  
> *(Click the badge above or replace `YOUR_DEMO_VIDEO_LINK_HERE` with your recorded demo video link)*

---

## 📖 Complete Step-by-Step Implementation Guide
For the full click-by-click deployment manual with all PySpark code blocks, configuration parameters, and setup screenshots, refer to:  
👉 [**fabric-databricks-unitybridge-step-by-step.md**](fabric-databricks-unitybridge-step-by-step.md)

---

## 📌 Executive Summary & Motivation

In enterprise analytics ecosystems, a common architectural divide exists:
* **Data Engineering & Machine Learning** teams standardize on **Azure Databricks** with **Unity Catalog** for scalable ETL, complex distributed computing, and ML lifecycle tracking.
* **Business Intelligence & Executive Reporting** teams standardize on **Microsoft Fabric & Power BI** for self-service dashboards and organizational semantic models.

Traditionally, bridging these two environments requires **building redundant ETL copy pipelines**, setting up scheduled batch syncs, and paying double for storage across platforms.

**UnityBridge** demonstrates a modern **Zero-Copy Lakehouse Architecture**:
By leveraging the open **Delta Lake** storage standard on **Azure Data Lake Storage Gen2 (ADLS Gen2)** and **OneLake Shortcuts** in Microsoft Fabric, data is processed once in Databricks and made instantly queryable in Power BI via **Direct Lake mode** without duplicating a single byte.

---

## 🏗️ End-to-End Architecture

```mermaid
flowchart TD
    subgraph Azure_Databricks_Tenant ["🏢 Azure Databricks Environment (Tenant A)"]
        Raw["📄 Raw Credit Risk Data (CSV)"] -->|PySpark Ingestion| Bronze[("🥉 Bronze Delta Table")]
        Bronze -->|Cleansing & Imputation| Silver[("🥈 Silver Delta Table")]
        Silver -->|Aggregation| Gold[("🥇 Gold Delta Summary")]
        Silver -->|Feature Pipeline| ML["🤖 MLflow Pipeline Model (Unity Catalog Volume)"]
        
        Job["⏱️ Databricks Workflow (Orchestrator)"] -.->|Automates| Bronze
        Job -.->|Automates| Silver
        Job -.->|Automates| Gold
        Job -.->|Automates| ML
    end

    subgraph ADLS_Gen2 ["☁️ Azure Data Lake Storage Gen2"]
        Storage[("📦 cnt-unitycatalog / __unitystorage\nDelta Parquet Files")]
    end

    subgraph Microsoft_Fabric_Tenant ["🏢 Microsoft Fabric Environment (Tenant B)"]
        Shortcut["🔗 OneLake ADLS Gen2 Shortcuts\n(Zero-Copy Link)"]
        Lakehouse[("🏛️ Fabric Lakehouse (lh_creditrisk_analytics)")]
        SemanticModel["🧠 Direct Lake Semantic Model\n(DAX KPIs)"]
        PowerBI["📊 Power BI Executive Dashboard\n(Sub-second Interactive BI)"]

        Shortcut --> Lakehouse
        Lakehouse --> SemanticModel
        SemanticModel --> PowerBI
    end

    Bronze & Silver & Gold -->|Managed Tables| Storage
    Storage -->|Direct Storage Link (Key Auth)| Shortcut

    classDef db fill:#FF3621,stroke:#333,stroke-width:1px,color:#fff;
    classDef fb fill:#0078D4,stroke:#333,stroke-width:1px,color:#fff;
    classDef pbi fill:#F2C811,stroke:#333,stroke-width:1px,color:#000;
    classDef stor fill:#00ADD8,stroke:#333,stroke-width:1px,color:#fff;

    class Bronze,Silver,Gold,Job,ML db;
    class Lakehouse,Shortcut fb;
    class SemanticModel,PowerBI pbi;
    class Storage stor;
```

---

## 🚀 Key Technical Capabilities

### 1. Medallion Architecture with PySpark
* **Bronze Layer (`bronze_credit_training`):** Ingests raw financial training records (`150,000+` rows) into Delta format with schema preservation.
* **Silver Layer (`silver_credit_training`):** Robust data engineering pipeline that parses literal `"NA"` strings into true SQL `NULL`s, casts columns to strict types (`double`, `integer`), and calculates median-imputed values for missing income.
* **Gold Layer (`gold_credit_age_summary`):** Pre-computes business-critical delinquency metrics binned by customer age cohorts for instant analytical queries.

### 2. MLflow & Unity Catalog Model Governance
* Formatted feature vectors using `pyspark.ml.feature.VectorAssembler`.
* Encapsulated feature engineering and Logistic Regression within a `pyspark.ml.Pipeline`.
* Logged model runs, parameters, metrics, and schema signatures to **Unity Catalog Volumes** using custom `dfs_tmpdir` paths.

### 3. Automated Workflow Orchestration
* Integrated all notebooks into an automated, multi-task **Databricks Workflow Job** (`job-creditrisk-pipeline`) with dependency enforcement (`Run_Bronze` $\rightarrow$ `Run_Silver` $\rightarrow$ `Run_Gold` & `Run_ML`).

### 4. Cross-Tenant Zero-Copy Lakehouse Bridge
* Secured ADLS Gen2 access using Azure Access Connectors & Managed Identities.
* Mounted physical Delta Lake storage directly into a Microsoft Fabric Lakehouse via **OneLake ADLS Gen2 Shortcuts**.
* Completely eliminated cross-platform data movement and duplicated storage costs.

### 5. Direct Lake Business Intelligence (Power BI)
* Created a **Custom Semantic Model** operating purely on **Direct Lake** mode (in-memory query speed directly over Parquet files on cloud storage).
* Built custom DAX metrics for credit risk analytics:
  ```dax
  Total Customers KPI = COUNT(Silver[id])
  Total Delinquent KPI = SUM(Silver[SeriousDlqin2yrs])
  Default Rate KPI % = DIVIDE([Total Delinquent KPI], [Total Customers KPI], 0)
  Average Monthly Income KPI = AVERAGE(Silver[MonthlyIncome])
  Average Debt Ratio KPI = AVERAGE(Silver[DebtRatio])
  ```
* Built an interactive executive dashboard (`rpt-creditrisk-dashboard`) with KPI cards, delinquency cohort analysis, and multi-factor risk scatter plots.

---

## 📂 Repository Structure

```text
├── GiveMeSomeCredit/                         # Raw dataset (cs-training.csv, cs-test.csv)
│   ├── cs-training.csv
│   └── cs-test.csv
├── fabric-databricks-unitybridge-step-by-step.md  # Comprehensive deployment manual
└── README.md                                 # Project documentation
```

---

## 🏷️ Enterprise Resource Naming Standard

Following the **Microsoft Cloud Adoption Framework (CAF)**:

| Resource Type | Resource Name | Purpose |
| :--- | :--- | :--- |
| **Azure Resource Group** | `rg-creditrisk-analytics` | Resource container for all compute & storage |
| **Databricks Workspace** | `dbw-creditrisk-analytics` | Premium workspace with Unity Catalog metastore |
| **ADLS Gen2 Storage** | `stcreditriskanalytics` | Hierarchical namespace storage account |
| **Storage Container** | `cnt-unitycatalog` | Root container for Unity Catalog managed tables |
| **Access Connector** | `id-unitybridge-connector` | Azure Managed Identity for Databricks IAM |
| **Storage Credential** | `cred-unitycatalog-storage` | Unity Catalog security credential |
| **External Location** | `extloc-unitycatalog-storage` | Registered storage location in Unity Catalog |
| **Unity Catalog** | `credit_analytics` | 3-level namespace root catalog |
| **Unity Catalog Schema** | `credit_risk` | Database schema for credit risk data |
| **Databricks Job** | `job-creditrisk-pipeline` | Multi-task automated pipeline orchestrator |
| **Fabric Workspace** | `fws-creditrisk-analytics` | Microsoft Fabric capacity workspace |
| **Fabric Lakehouse** | `lh_creditrisk_analytics` | Lakehouse holding OneLake shortcuts |
| **Semantic Model** | `sm-creditrisk-analytics` | Direct Lake dataset with DAX measures |
| **Power BI Dashboard** | `rpt-creditrisk-dashboard` | Executive reporting layer |

---

## 🛡️ Governance & Security Boundary Analysis

A critical architectural finding of this project is understanding **cross-platform governance boundaries**:
1. **Shortcuts Do Not Automatically Inherit Unity Catalog ACLs:** When mounting underlying ADLS Gen2 Delta files into Microsoft Fabric, Unity Catalog's table-level and row-level security permissions do not automatically flow across tenant boundaries.
2. **Explicit Governance in Fabric:** Data access control must be configured natively in Microsoft Fabric using **Workspace Roles**, **Lakehouse item permissions**, and **Row-Level Security (RLS)** in the Power BI Semantic Model.

---

## 👨‍💻 Author & Contact

**Data Engineer / Cloud Solution Architect**  
* Passionate about Lakehouse Architectures, Distributed Computing (Spark/Databricks), and Modern BI Platforms (Fabric/Power BI).
* Connect with me on [LinkedIn](https://linkedin.com) | [GitHub](https://github.com)
