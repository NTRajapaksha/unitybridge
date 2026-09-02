# Fully Detailed Step-by-Step Guide: UnityBridge Project (Azure Databricks + Microsoft Fabric)

This guide provides a comprehensive, click-by-click walkthrough for building the UnityBridge project. This architecture connects an Azure Databricks workspace (doing Data Engineering/ML with Unity Catalog) with a Microsoft Fabric workspace (doing BI & Analytics via Direct Lake) across two separate accounts, using Fabric's **OneLake ADLS Gen2 Shortcuts** for high-performance zero-copy data sharing.

## Prerequisites

Before starting, ensure you have:
1. **Azure Account (Databricks)**: An Azure subscription (Azure for Students with $100 credit works).
2. **Microsoft 365 / Fabric Account**: A separate account/tenant with Microsoft Fabric enabled (a free trial works).
3. **Dataset**: A sample dataset like "Give Me Some Credit" or Olist E-commerce data.

> **⚠️ Cost Management Warning (Student Accounts):** Databricks can quickly consume your $100 credit. Always use a **single-node, small VM-size cluster**, set **auto-termination to 15-20 minutes**, and shut down resources when not in use. Or utilize Serverless SQL Warehouses if applicable.

---

## Phase 0: Databricks Workspace & Unity Catalog Setup

### Step 1: Create an Azure Resource Group
1. Log into the Azure Portal with your Databricks account.
2. Search for **Resource groups** and click **Create**.
3. Choose your subscription, name your resource group `rg-creditrisk-analytics`, and select your region (e.g., East US).
4. Click **Review + create**, then **Create**. (You will place all your Databricks and Azure Storage resources into this group).

> 💡 **Why we did this:** A dedicated resource group establishes a single lifecycle and cost-containment boundary for all Databricks compute and storage infrastructure, making environment teardown and Role-Based Access Control (RBAC) clean and auditable.

### Step 2: Create Azure Databricks Workspace
1. In the Azure Portal, search for **Azure Databricks** and click **Create**.
2. Select the resource group `rg-creditrisk-analytics`, name your workspace `dbw-creditrisk-analytics`, and select the **Premium** pricing tier (required for Unity Catalog).
3. Click **Review + Create**, then **Create**. Wait for deployment to finish, then click **Launch Workspace**.

> 💡 **Why we did this:** The Premium pricing tier is mandatory because it enables **Unity Catalog**, which provides centralized metastore governance, data lineage, volumes, and 3-level namespace (`catalog.schema.table`) access control.

### Step 3: Set Up Azure Data Lake Storage Gen2 (ADLS Gen2)
1. Back in the Azure Portal, search for **Storage accounts** and click **Create**.
2. Select your resource group, name the account `stcreditriskanalytics` (no dashes allowed), choose your region, and crucially, in the **Advanced** tab, check the box for **Enable hierarchical namespace** (this makes it Gen2).
3. Click **Review + Create**, then **Create**.
4. Once deployed, go to the storage account, click **Containers** on the left, and create a new container named `cnt-unitycatalog`.

> 💡 **Why we did this:** Enabling the **hierarchical namespace** transforms basic blob storage into ADLS Gen2, providing true directory hierarchies, atomic file operations, and POSIX-compliant permissions essential for Delta Lake transaction logs.

### Step 4: Configure Unity Catalog Metastore & External Location
1. In the Databricks Workspace, navigate to **Catalog** (left sidebar).
2. If you don't have a metastore configured for your region, follow the prompts to create one (this is a one-time account-level setup).
3. To allow Databricks to access your ADLS Gen2 container, you first need an **Access Connector**:
   - In the Azure Portal, search for **Access Connector for Azure Databricks** and click **Create**. Name it `id-unitybridge-connector`.
   - Go to your storage account (`stcreditriskanalytics`) > **Access Control (IAM)** > **Add role assignment**.
   - Assign the **Storage Blob Data Contributor** role to the `id-unitybridge-connector` Managed Identity.
   - Go back to the `id-unitybridge-connector` Access Connector overview, click **Properties** (under Settings on the left), and copy its **Resource ID**.
4. Create the **Storage Credential** in Databricks:
   - Go to **Catalog** > **Create** (top right) > **Create a credential**.
   - **Credential Type**: Azure Managed Identity
   - **Credential name**: `cred-unitycatalog-storage`
   - **Access connector ID**: Paste the copied Resource ID here.
   - Click **Create**.
5. Create an **External Location**:
   - Go to **Catalog** > **Create** > **Create an external location**.
   - Name it `extloc-unitycatalog-storage`.
   - URL: `abfss://cnt-unitycatalog@stcreditriskanalytics.dfs.core.windows.net/`
   - Select the `cred-unitycatalog-storage` credential you just created.
   - **Note on File Events Error**: If you see an error about `Failed to provision file events resources`, check **Force Create** to skip Auto Loader event notifications (which are not needed for this project).

> 💡 **Why we did this:** Using Azure Access Connectors (Managed Identities) eliminates hardcoded secrets and access keys inside Databricks. Unity Catalog assumes the identity to access ADLS Gen2 securely, establishing a governed root storage location for managed tables.

### Step 5: Create Catalog and Schema
1. In the Databricks **Catalog Explorer**, click **Create Catalog** and name it `credit_analytics`.
2. Inside the `credit_analytics` catalog, click **Create Schema** and name it `credit_risk`.
3. **CRITICAL STEP**: On your Unity Catalog Metastore settings, ensure that **External Data Access** is **ENABLED**.

> 💡 **Why we did this:** Unity Catalog's 3-level namespace (`credit_analytics.credit_risk.table`) isolates project domains, organizes Medallion tables, and provides the foundation for unified catalog governance.

---

## Phase 1: Databricks Data Engineering & ML

### Step 1: Cluster Configuration
1. Go to **Compute** > **Create Compute**.
2. Select **Single Node** (to save costs) with a runtime of 13.3 LTS ML or higher.
3. Choose a small VM size.
4. Check **Terminate after 15 minutes of inactivity**. Start the cluster.

> 💡 **Why we did this:** Using Single-Node compute with aggressive auto-termination (15 mins) drastically minimizes cloud credit burn while still providing ML runtime libraries (PySpark ML, MLflow) for training.

### Step 2: Ingest Raw Data
1. Go to **Catalog** > `credit_analytics` > `credit_risk`.
2. Click **Create Volume** (name it `vol_raw_data`).
3. Upload `cs-training.csv` and `cs-test.csv` from the "Give Me Some Credit" dataset into this volume.

> 💡 **Why we did this:** Unity Catalog Volumes provide a secure, governed namespace for landing non-tabular/raw CSV files without exposing raw storage container keys or mounting DBFS.

### Step 3: Build the Medallion Pipeline (PySpark)
To build a production-grade pipeline, create **separate notebooks** for each layer in your Databricks Workspace (e.g., `01_bronze_ingestion`, `02_silver_cleaning`, `03_gold_aggregation`, `04_ml_training`). 

**01_bronze_ingestion Notebook** (Raw Ingestion):
```python
# Load Training Data
df_bronze = spark.read.csv("/Volumes/credit_analytics/credit_risk/vol_raw_data/cs-training.csv", header=True, inferSchema=True)
# Rename the unnamed index column
df_bronze = df_bronze.withColumnRenamed("_c0", "id")
# Save as Bronze table
df_bronze.write.format("delta").mode("overwrite").saveAsTable("credit_analytics.credit_risk.bronze_credit_training")
```

**02_silver_cleaning Notebook** (Cleaning & Imputation):
```python
from pyspark.sql.functions import col, when

# Read Bronze Data
df_silver = spark.table("credit_analytics.credit_risk.bronze_credit_training")

# 1. Replace literal "NA" strings with true Nulls and cast to proper numbers
df_silver = df_silver.withColumn("MonthlyIncome", when(col("MonthlyIncome") == "NA", None).otherwise(col("MonthlyIncome")).cast("double"))
df_silver = df_silver.withColumn("NumberOfDependents", when(col("NumberOfDependents") == "NA", None).otherwise(col("NumberOfDependents")).cast("double"))
df_silver = df_silver.withColumn("DebtRatio", col("DebtRatio").cast("double"))
df_silver = df_silver.withColumn("SeriousDlqin2yrs", col("SeriousDlqin2yrs").cast("integer"))

# 2. Impute missing MonthlyIncome with median, NumberOfDependents with 0
median_income = df_silver.filter(col("MonthlyIncome").isNotNull()).approxQuantile("MonthlyIncome", [0.5], 0.01)[0]
df_silver = df_silver.fillna({
    "MonthlyIncome": median_income,
    "NumberOfDependents": 0
})

# Save as Silver table
df_silver.write.format("delta").mode("overwrite").saveAsTable("credit_analytics.credit_risk.silver_credit_training")
```

**03_gold_aggregation Notebook** (Aggregated Data for BI):
```python
import pyspark.sql.functions as F

# Create Age Group Aggregations
df_gold_age_summary = spark.table("credit_analytics.credit_risk.silver_credit_training") \
    .withColumn("AgeGroup", F.expr("case when age < 30 then '<30' when age between 30 and 49 then '30-49' when age between 50 and 64 then '50-64' else '65+' end")) \
    .groupBy("AgeGroup") \
    .agg(
        F.count("id").alias("TotalCustomers"),
        F.sum("SeriousDlqin2yrs").alias("TotalDelinquent"),
        F.round(F.avg("SeriousDlqin2yrs") * 100, 2).alias("DefaultRate_Pct")
    )

# Save as Gold table
df_gold_age_summary.write.format("delta").mode("overwrite").saveAsTable("credit_analytics.credit_risk.gold_credit_age_summary")
```

> 💡 **Why we did this:** The Medallion architecture establishes a clean chain of data custody:
> * **Bronze:** Retains raw data as an immutable single source of truth.
> * **Silver:** Fixes dirty data (e.g. converting `"NA"` strings to real nulls and casting data types) to ensure downstream BI engines don't fail during math aggregations.
> * **Gold:** Pre-aggregates high-level business metrics (delinquency by age cohort) so executive dashboards load instantly.

### Step 4: Train ML Model with MLflow
Create a **04_ml_training Notebook** to train a simple Logistic Regression classifier and track it using native MLflow:

```python
import mlflow
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.functions import expr, col

# Prepare data
feature_cols = ["age", "DebtRatio", "MonthlyIncome", "NumberOfDependents", "RevolvingUtilizationOfUnsecuredLines"]
df_ml = spark.table("credit_analytics.credit_risk.silver_credit_training").dropna(subset=feature_cols).withColumn("MonthlyIncome", expr("try_cast(MonthlyIncome as float)")).fillna({'MonthlyIncome': 0}).withColumn("NumberOfDependents", expr("try_cast(NumberOfDependents as int)")).fillna({'NumberOfDependents': 0})

# Rename label column
df_ml = df_ml.withColumnRenamed("SeriousDlqin2yrs", "label")

# Build a Pipeline instead of transforming directly
from pyspark.ml import Pipeline
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid='keep')
lr = LogisticRegression(maxIter=10, featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[assembler, lr])

# Train & Log Model
with mlflow.start_run(run_name="ml-creditrisk-lr-model"):
    # Fit the pipeline
    model = pipeline.fit(df_ml)
    
    # Grab a sample of the RAW data (no vectors!) for the signature
    sample_data = df_ml.select(feature_cols).limit(5).toPandas()
    
    # Log model to Unity Catalog
    mlflow.spark.log_model(
        model, 
        "credit_risk_model", 
        registered_model_name="credit_analytics.credit_risk.ml_creditrisk_lr_model",
        dfs_tmpdir='/Volumes/credit_analytics/credit_risk/vol_raw_data/tmp',
        input_example=sample_data
    )
```

> 💡 **Why we did this:** 
> 1. Packaging `VectorAssembler` and `LogisticRegression` into a single `pyspark.ml.Pipeline` allows MLflow to extract an input signature directly from raw scalar columns, preventing Spark `VectorUDT` serialization conflicts.
> 2. Specifying `dfs_tmpdir` pointing to a Unity Catalog volume allows seamless model artifact logging even when running on Serverless or restricted root DBFS compute.

### Step 5: Orchestrate with Databricks Workflows (Jobs)
Now that you have separate notebooks, you will link them into an automated pipeline.
1. In the Databricks left sidebar, click **Workflows** > **Create Job**.
2. Name the Job `job-creditrisk-pipeline`.
3. Add your first task: 
   - **Task name**: `Run_Bronze`
   - **Type**: Notebook
   - **Path**: Select `01_bronze_ingestion`
   - **Compute**: Select the cluster you created earlier (or use a Job cluster to save money). Click **Create task**.
4. Add the second task:
   - Click the **+** button under the `Run_Bronze` task.
   - **Task name**: `Run_Silver`
   - **Path**: Select `02_silver_cleaning`.
   - **Depends on**: `Run_Bronze`. Click **Create task**.
5. Repeat for `03_gold_aggregation` (Depends on `Run_Silver`) and `04_ml_training` (Depends on `Run_Silver`).
6. Click **Run now** in the top right to execute the pipeline end-to-end.

> 💡 **Why we did this:** Databricks Workflows turn ad-hoc interactive notebooks into an automated, production-grade DAG (Directed Acyclic Graph) with task dependencies, retry policies, and execution history tracking.

---

## Phase 2: Fabric OneLake Shortcut Setup (Zero-Copy Architecture)

*Architectural Note: Because university Azure tenants block third-party OAuth and Service Principals for Guest users, we bypass the "Mirrored Catalog" feature. Instead, we use a **OneLake ADLS Gen2 Shortcut**. This achieves the exact same architectural goal—Zero-Copy data sharing—by pointing Fabric directly at the underlying Delta Lake files created by Databricks!*

### Step 1: Get Storage Account Key
1. Go to the Azure Portal and open your storage account (`stcreditriskanalytics`).
2. On the left menu, scroll down to **Security + networking** and click **Access keys**.
3. Click **Show** next to `key1` and copy the **Key** value. (This allows us to completely bypass the university's Entra ID blocks).

> 💡 **Why we did this:** Authenticating via Storage Account Keys allows Microsoft Fabric to securely bridge across two completely separate Microsoft Entra (Azure AD) tenants without requiring tenant federation, admin OAuth consent, or cross-tenant Service Principals.

### Step 2: Create a Lakehouse in Fabric
1. Log into your **Microsoft Fabric** workspace `fws-creditrisk-analytics`.
2. Click **New item** > **Lakehouse**. Name it `lh_creditrisk_analytics`.

> 💡 **Why we did this:** The Fabric Lakehouse acts as the unified analytics catalog on the Microsoft side, automatically provisioning a SQL Analytics Endpoint and enabling Direct Lake semantic modeling.

### Step 3: Create the ADLS Gen2 Shortcut
1. In your new Lakehouse, click on the **...** next to the **Tables** folder in the left Explorer pane.
2. Select **New shortcut**.
3. Choose **Azure Data Lake Storage Gen2**.
4. Set up the connection:
   - **URL**: `https://stcreditriskanalytics.dfs.core.windows.net`
   - **Connection credentials**: Create new connection.
   - **Authentication kind**: Account key.
   - **Account key**: Paste the `key1` you copied from Azure.
   - Click **Next**.
5. Browse into `cnt-unitycatalog` > `__unitystorage` > `catalogs` to find your table folders. Check the boxes for your Gold and Silver tables, then click **Create**.

> 💡 **Why we did this:** OneLake Shortcuts mount external Delta tables in-place. Instead of building a scheduled copy pipeline or paying double for storage, Fabric reads the parquet data directly where Databricks saved it (Zero-Copy Architecture).

### Step 4: Verify the Zero-Copy Link
1. In your Fabric Lakehouse, expand the **Tables** folder.
2. You will instantly see your Databricks Delta tables appear natively in Fabric!
3. **Test Sync**: Go back to Databricks, run your pipeline to update a table. Return to Fabric, refresh the Lakehouse, and verify the data changes instantly.

> 💡 **Why we did this:** Validating the live sync proves that because both platforms adhere to the open **Delta Lake** protocol, changes committed to the Delta transaction log by Databricks are immediately available in Fabric without ETL intervention.

---

## Phase 3: Power BI Direct Lake Analytics

*Architectural Note: Because Microsoft Fabric mounts Databricks Delta Lake tables directly via OneLake Shortcuts, Power BI can query the data using **Direct Lake** mode. This delivers the speed of in-memory Import mode without duplicating data or executing expensive SQL queries against Databricks compute!*

### Step 1: Create the Semantic Model
1. Open your Lakehouse (`lh_creditrisk_analytics`).
2. In the top ribbon, click **New semantic model**. Name it `sm-creditrisk-analytics`.
3. Check the boxes for your Databricks tables (`Gold`, `Silver`) and click **Confirm**.
4. In the Semantic Model view, click on your `Silver` table and add these five **New Measures**:

```dax
Total Customers KPI = COUNT(Silver[id])

Total Delinquent KPI = SUM(Silver[SeriousDlqin2yrs])

Default Rate KPI % = DIVIDE([Total Delinquent KPI], [Total Customers KPI], 0)

Average Monthly Income KPI = AVERAGE(Silver[MonthlyIncome])

Average Debt Ratio KPI = AVERAGE(Silver[DebtRatio])
```

> 💡 **Why we did this:** **Direct Lake** mode loads Delta parquet files directly into the Power BI Analysis Services engine memory on-demand. This eliminates the latency of DirectQuery SQL translations while bypassing the scheduled data refresh overhead of standard Import mode.

### Step 2: Build the Power BI Report Canvas
1. Click **New report** at the top of the semantic model page. Name it `rpt-creditrisk-dashboard`. 
2. Build these visuals:
   - **KPI Cards**:
     - Visual: Card $\rightarrow$ Field: `Total Customers KPI`
     - Visual: Card $\rightarrow$ Field: `Default Rate KPI %`
   - **Column Chart** (Proves your Databricks Gold Aggregation works):
     - **Title**: "Delinquency Rate by Age Group"
     - **Visual**: Clustered Column Chart
     - **X-axis**: `AgeGroup` (from `Gold` table)
     - **Y-axis**: `DefaultRate_Pct` (from `Gold` table)
   - **Scatter Plot** (Risk vs. Income Analysis):
     - **Title**: "Debt Ratio vs. Monthly Income by Default Status"
     - **Visual**: Scatter Chart
     - **X-axis**: `Average Monthly Income KPI`
     - **Y-axis**: `Average Debt Ratio KPI`
     - **Legend**: `SeriousDlqin2yrs` (0 = Non-Default, 1 = Default)
     - *Note: Leave the "Details" and "Values" wells empty so the chart aggregates cleanly without exceeding capacity limits.*

> 💡 **Why we did this:** Pairing high-level KPI cards and Gold cohort distributions with deep-dive Silver scatter plots proves that the Direct Lake model can seamlessly serve both aggregated executive summaries and granular exploratory analytics.

---

## Phase 4: Security & Governance (Crucial Explanation)

This is the most critical conceptual part of the project to understand:

1. **Governance Does Not Auto-Sync**: Shortcuts bypass Unity Catalog's fine-grained access control (row/column filters) because Fabric connects directly to the underlying ADLS Gen2 storage files. If a user in Databricks only has access to certain rows, that restriction is **not** automatically inherited in Fabric.
2. **Explicit Reconfiguration**: You must explicitly configure security on the Fabric side (workspace roles, Lakehouse permissions, and Row-Level Security on the semantic model) to match your enterprise data access boundaries.
3. Treat the Fabric-side Lakehouse as a separate governance surface. Understanding this boundary is what proves true enterprise architecture competence.

> 💡 **Why we did this:** Recognizing that storage-layer integration decouples compute-layer governance is a vital distinction in enterprise cloud architecture. It demonstrates a holistic understanding of data security across multi-tenant boundaries.

---

## Appendix: Enterprise Resource Naming Convention
This project adopts Microsoft Cloud Adoption Framework (CAF) guidelines for resource naming:

| Resource Type | Recommended Name | Pattern |
| :--- | :--- | :--- |
| **Resource Group** | `rg-creditrisk-analytics` | `rg-<domain>-<workload>` |
| **Databricks Workspace** | `dbw-creditrisk-analytics` | `dbw-<domain>-<workload>` |
| **Storage Account** | `stcreditriskanalytics` | `st<domain><workload>` |
| **Storage Container** | `cnt-unitycatalog` | `cnt-<purpose>` |
| **Access Connector** | `id-unitybridge-connector` | `id-<purpose>` |
| **Storage Credential** | `cred-unitycatalog-storage` | `cred-<purpose>` |
| **External Location** | `extloc-unitycatalog-storage` | `extloc-<purpose>` |
| **UC Catalog** | `credit_analytics` | `<domain>_<workload>` |
| **UC Schema** | `credit_risk` | `<domain>` |
| **UC Volume** | `vol_raw_data` | `vol_<type>` |
| **Bronze Table** | `bronze_credit_training` | `bronze_<entity>` |
| **Silver Table** | `silver_credit_training` | `silver_<entity>` |
| **Gold Table** | `gold_credit_age_summary` | `gold_<entity>_<aggregation>` |
| **MLflow Model** | `ml_creditrisk_lr_model` | `ml_<domain>_<algo>_model` |
| **Databricks Job** | `job-creditrisk-pipeline` | `job-<domain>-<purpose>` |
| **Fabric Workspace** | `fws-creditrisk-analytics` | `fws-<domain>-<workload>` |
| **Fabric Lakehouse** | `lh_creditrisk_analytics` | `lh_<domain>_<workload>` |
| **Semantic Model** | `sm-creditrisk-analytics` | `sm-<domain>-<workload>` |
| **Power BI Report** | `rpt-creditrisk-dashboard` | `rpt-<domain>-<purpose>` |



