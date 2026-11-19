# 🚴‍♂️ End-to-End SAP Bicycle Sales Analytics Platform on Microsoft Fabric

**Documentation Format:** `README.md` Project Portfolio  
**Certification Reference:** DP-600 (Microsoft Fabric Analytics Engineer)

---

## 📝 Executive Summary
This repository showcases an enterprise-grade Analytics Engineering project implemented within the Microsoft Fabric ecosystem. The solution ingests raw SAP bicycle sales data from flat files, processes it through a highly scalable Medallion Architecture (Bronze -> Silver -> Gold) using PySpark, builds a robust Star Schema Semantic Model, and delivers an interactive Power BI Executive Dashboard utilizing dynamic calculation groups for time intelligence.

---

## 📐 Data Pipeline & Medallion Architecture

### Data Flow Workflow
`[6 Raw CSV Files] ➔ 🥉 Bronze (Raw Parquet) ➔ 🥈 Silver (Cleansed Delta) ➔ 🥇 Gold (Star Schema) ➔ 📊 Power BI`

### 1. Bronze Layer (Ingestion & Land)
* **Source Data:** Ingested 6 core source CSV datasets: Sales (2 files), Products, Customers, Addresses, and Product Names.
* **Orchestration:** Built a Fabric Data Factory Pipeline featuring 6 parallel Copy Activities to land the raw files securely into the `LH_sap_bronze` Lakehouse as a single source of truth.
* **Shortcuts:** Created seamless shortcuts linking the Bronze layer files to the Silver environment for zero-copy data virtualization.

### 2. Silver Layer (Data Transformation & Cleansing)
Developed optimized PySpark Notebooks within the `LH_sap_silver` Lakehouse to clean, deduplicate, and merge relational structures:
* **Customer Dimension Merging:** Combined the raw Customers and Addresses datasets into a single, unified profile.
* **Product Dimension Enrichment:** Consolidated Products and Product Names tables to resolve multilingual or descriptive attributes.
* **Quality Assurance:** Handled missing values, standardized data types (dates/amounts), and strictly removed duplicate records to enforce enterprise data quality.

### 3. Gold Layer (Business Ready - Dimensional Modeling)
Leveraged a dedicated processing notebook to curate data from Silver into a high-performance Star Schema inside the `LH_aw_gold` Lakehouse:
* **Fact Table:** Engineered `Fact_Sales` containing key commercial metrics (SalesAmount, OrderQuantity).
* **Dimension Tables:** Established enriched dimension lookup structures: `dim_Product`, `dim_Customer`.
* **Time Intelligence:** Generated a dynamic, custom `dim_calender` table to support advanced chronological calculations.
* **Shortcuts:** Implemented Fabric Lakehouse shortcuts to map curated Gold tables directly into the analytical endpoint.

---

## 🧠 Semantic Modeling & Advanced DAX
Developed a high-performance Semantic Model coupled with business logic tailored for executive-level reporting:
* **Key Performance Indicators (KPIs):** Engineered measures for Total Net Revenue, Total Cost (modeled at a simulated 65% benchmark), Net Profit, and Net Profit Margin % inside structured display folders (`_Key Measures`).
* **Calculation Groups (Time Intelligence):** Formulated a specialized Time Calculation calculation group via the Model Explorer to dynamically toggle metrics across multiple time frames:
  * **Current** ➔ `SELECTEDMEASURE()`
  * **YTD** ➔ `TOTALYTD(SELECTEDMEASURE(), 'dim_calender'[DateKay])`
  * **PY** ➔ `CALCULATE(SELECTEDMEASURE(), SAMEPERIODLASTYEAR('dim_calender'[DateKay]))`
  * **YOY %** ➔ Dynamic year-over-year growth percentage using custom Dynamic Format Strings (`"0.0%"`).

---

## 📊 Business Intelligence Dashboard Design
The final presentation layer features an executive-focused dashboard designed under strict UI/UX industry standards:
<p align="center">
  <img src="images/Sap Bike Sales Performance Dashboard - Report .png" alt="Executive Dashboard" width="90%">
</p>
* **Layout Strategy:** Uses a clean, structured grid following the standard reading Z-pattern (1920x1080 Full HD Canvas).
* **Color Palette:** Professional corporate palette using Deep Royal Blue (`#1E3A8A`) for headers and structural layout, alongside neutral Gray (`#F3F4F6`) container backgrounds to avoid cognitive overload.
* **Visual Breakdown:**
  * **Executive Cards:** Horizontal arrangement of key health metrics (Revenue, Average Order, Profit, Margin).
  * **Trend Analysis:** A detailed Line Chart displaying monthly revenue shifts under chronological filters.
  * **Categorical Performance:** High-readability Horizontal Bar charts highlighting top-performing bike lines and product segments.

---

## 💡 Technical Notes & Constraints
* **Data Horizon Constraints:** The source SAP flat-file dataset natively contains transaction records spanning only a single fiscal year.
* **Time Intelligence Functionality:** While advanced Time Intelligence metrics (such as Previous Year `PY` and Year-Over-Year Growth `YOY %`) are fully engineered and dynamically coded inside the **Semantic Model using DAX Calculation Groups**, their visualization in the Power BI layer is naturally limited due to the absence of historical baseline years (e.g., prior fiscal years) in the raw source data. Current Year and `YTD` functionalities operate dynamically with absolute accuracy.

---

## 🛠️ Tech Stack & Skills Demonstrated
* **Platform:** Microsoft Fabric (OneLake, Lakehouse, Synapse Data Engineering)
* **Languages:** PySpark (Python), DAX
* **Data Ops:** Fabric Git Integration, GitHub Version Control
* **Visualization:** Power BI (DirectLake Mode)

---

## 🚀 Deployment Steps (Fabric Git Integration)

Since this project utilizes **Fabric Git Integration**, you can easily synchronize this repository into your own Fabric ecosystem:

### Prerequisites
1. An active **Microsoft Fabric Capacity** (or Trial license).
2. A blank **Fabric Workspace** mapped to that capacity.
3. Access to this GitHub repository.

### Synchronization & Setup
1. **Fork or Clone:** Fork this repository into your personal GitHub account.
2. **Configure Workspace Git Integration:**
   * Navigate to your empty Fabric Workspace.
   * Open **Workspace settings** > **Git integration**.
   * Select **GitHub**, choose your repository, and target your deployment branch.
3. **Sync All Items:**
   * Click on the **Source Control** button in the top menu of your workspace.
   * Click **Sync All**. Microsoft Fabric will automatically read the tracking metadata and instantly spin up the Data Pipelines, Lakehouses, Notebooks, and Semantic Models.
4. **Data Execution:**
   * Navigate to the `/Pipelines` folder to trigger data ingestion into the Bronze layer, or open the `/Notebooks` sequentially to execute the PySpark processing workflows.

---

## 📂 Repository Contents & Blueprint
* 📂 `/Pipelines` - Contains layout details/JSON definition files for the data factory pipelines.
* 📂 `/Notebooks` - Raw `.ipynb` files containing the PySpark cleansing and modeling code blocks.
* 📂 `/Semantic_Model` - Documented DAX scripts and display folder schema definitions.
* 📂 `/Screenshots` - High-resolution imagery of the data architecture diagram and final dashboard view.
