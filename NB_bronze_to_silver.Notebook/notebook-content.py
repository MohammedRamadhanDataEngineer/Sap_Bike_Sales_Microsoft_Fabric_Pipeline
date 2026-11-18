# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fb83fb78-3180-49cf-9805-f4ab03f1f5c1",
# META       "default_lakehouse_name": "LH_sap_silver",
# META       "default_lakehouse_workspace_id": "b1b1907f-4e26-4fd6-8452-4b0fdda51d95",
# META       "known_lakehouses": [
# META         {
# META           "id": "fb83fb78-3180-49cf-9805-f4ab03f1f5c1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

"""
Microsoft Fabric Analytics Engineering Project - DP-600 Architecture
Layer: Silver (Enrichment & Schema Transformation - SAP Bike Sales Official Content)
Description: Reads raw SAP Bike Sales CSV data via OneLake shortcuts, translates technical SAP 
             UPPERCASE naming conventions, sanitizes data types, and stores outputs as optimized Delta Tables.
"""
from pyspark.sql.functions import col, to_date, trim, when, initcap,expr
def process_bronze_to_silver():
    print("Starting SAP Official Bike Sales Bronze to Silver transformation pipeline...")



    # =========================================================================
    # 1. Process Sales Document Header (VBAK.csv <- SalesOrders.csv)
    # =========================================================================
    print("Processing Raw SAP Bike Sales Orders Header (VBAK)...")
    vbak_raw = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .load("Files/rawdata/VBAK.csv") 

    # Mapping SAP Uppercase fields: SALESORDERID, CREATEDBY, CREATEDAT, PARTNERID, GROSSAMOUNT, NETAMOUNT, TAXAMOUNT, CURRENCY
    silver_sales_header = vbak_raw.select(
        col("SALESORDERID").alias("Sales_Order_ID"),
        col("CREATEDBY").alias("Created_By_User"),
        expr("add_months(to_date(CREATEDAT,'yyyyMMdd'),96)").alias("Order_Date"),
        col("PARTNERID").alias("Customer_ID"),
        col("GROSSAMOUNT").cast("decimal(18,2)").alias("Gross_Amount"),
        col("NETAMOUNT").cast("decimal(18,2)").alias("Net_Amount"),
        col("TAXAMOUNT").cast("decimal(18,2)").alias("Tax_Amount"),
        col("CURRENCY").alias("Currency_Code")
    ).filter(col("SALESORDERID").isNotNull()).dropDuplicates(["Sales_Order_ID"])


    # =========================================================================
    # 2. Process Sales Document Item Details (VBAP.csv <- SalesOrderItems.csv)
    # =========================================================================
    print("Processing Raw SAP Bike Sales Items Details (VBAP)...")
    vbap_raw = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .load("Files/rawdata/VBAP.csv") 

    # Mapping SAP Uppercase fields: SALESORDERID, SALESORDERITEM, PRODUCTID, GROSSAMOUNT, NETAMOUNT, QUANTITY, QUANTITYUNIT
    silver_sales_items = vbap_raw.select(
        col("SALESORDERID").alias("Sales_Order_ID"),
        col("SALESORDERITEM").alias("Order_Item_Line"),
        trim(col("PRODUCTID")).alias("Product_ID"),
        col("GROSSAMOUNT").cast("decimal(18,2)").alias("Item_Gross_Amount"),
        col("NETAMOUNT").cast("decimal(18,2)").alias("Item_Net_Amount"),
        col("QUANTITY").cast("int").alias("Quantity"),
        col("QUANTITYUNIT").alias("Quantity_Unit")
    ).filter((col("Sales_Order_ID").isNotNull()) & (col("Quantity") > 0))


    # =========================================================================
    # 3. Process Business Partners (KNA1.csv) + Addresses (ADRC.csv)
    # =========================================================================
    print("Processing Raw SAP Business Partners (KNA1) and Joining Addresses (ADRC)...")
    
    kna1_raw =vbap_raw = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .load("Files/rawdata/KNA1.csv") 

    adrc_raw = vbap_raw = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .load("Files/rawdata/ADRC.csv") 


    
    customers_joined = kna1_raw.join(
        adrc_raw,
        kna1_raw["ADDRESSID"] == adrc_raw["ADDRESSID"],
        "left"
    ) 
    # Mapping SAP Uppercase fields: PARTNERID, COMPANYNAME, CITY, COUNTRY
    silver_customers = customers_joined.select(
        kna1_raw["PARTNERID"].alias("Customer_ID"),
        initcap(col("COMPANYNAME")).alias("Customer_Name"),
        adrc_raw["CITY"].alias("City"),
        when(adrc_raw["COUNTRY"].isNull() , "Unknown").otherwise(adrc_raw["COUNTRY"]).alias("Country")
    ).filter(col("Customer_ID").isNotNull()).dropDuplicates(["Customer_ID"])

    # =========================================================================
    # 4. Process Product Master Data (MARA.csv) + Categories (PCT.csv)
    # =========================================================================
    print("Processing Raw SAP Bike Products (MARA) and Joining Categories...")
    
    mara_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "false").load("Files/rawdata/MARA.csv")
    categories_raw = spark.read.format("csv").option("header", "true").option("inferSchema", "false").load("Files/rawdata/PCT.csv")

    mara_filtered = mara_raw.filter(col("PRODUCTID").isNotNull()).dropDuplicates(["PRODUCTID"])

    
    products_joined = mara_filtered.join(
        categories_raw,
        mara_filtered["PRODCATEGORYID"] == categories_raw["PRODCATEGORYID"],
        "left"
    )

    silver_products = products_joined.select(
        trim(mara_filtered["PRODUCTID"]).alias("Product_ID"),
        initcap(categories_raw["SHORT_DESCR"]).alias("Product_Category"), 
        mara_filtered["PRICE"].cast("decimal(18,2)").alias("Base_Price")
    ).dropDuplicates(["Product_ID"])


    # =========================================================================
    # 💾 Target Write: Saving to Silver Layer Tables as Delta Format
    # =========================================================================
    print("Committing processed Bike Sales tables to Silver Layer Workspace...")
    silver_sales_header.write.format("delta").mode("overwrite").saveAsTable("stg_sap_sales_header")
    silver_sales_items.write.format("delta").mode("overwrite").saveAsTable("stg_sap_sales_items")
    silver_customers.write.format("delta").mode("overwrite").saveAsTable("stg_sap_customers")
    silver_products.write.format("delta").mode("overwrite").saveAsTable("stg_sap_products")
    
    print("Silver Layer execution completed successfully for SAP Bike Data!")

if __name__ == "__main__":
    process_bronze_to_silver()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
