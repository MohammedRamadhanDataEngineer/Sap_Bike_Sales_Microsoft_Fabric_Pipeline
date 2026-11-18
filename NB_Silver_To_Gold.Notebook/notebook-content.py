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
# META           "id": "436cc12f-d32d-4272-83b3-897d9efed531"
# META         },
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
Layer: Gold (Dimensional Data Modeling - Star Schema)
Description: Reads cleaned Silver Staging tables, constructs analytical Facts 
             and Dimensions, and applies Microsoft Fabric's native V-Order layout optimizations.
"""

from pyspark.sql.functions import col, explode, sequence, to_date, year, month, quarter, dayofmonth

def process_silver_to_gold():
    print(" Starting SAP Silver to Gold modeling and dimensional transformation...")


    # =========================================================================
    # 1   . Building Dimension Tables
    # =========================================================================
    print(" Constructing dim_customers and dim_products...")
    
    dim_customers = spark.table("stg_sap_customers").select(
        col("Customer_ID"),
        col("Customer_Name"),
        col("Country")
    )

    dim_products = spark.table("stg_sap_products").select(
        col("Product_ID"),
        col("Product_Category"),
        col("Base_Price")
    )

    # =========================================================================
    # 2. Building Minimalist Calendar Dimension Key_Measures Table (dim_calendar)
    # =========================================================================
    print(" Generating highly optimized Numerical Calendar Dimension (dim_calendar)...")
    
    # Generates a continuous daily sequence from 2018\1 to 2018\10
    date_sequence_df = spark.sql(
        "SELECT explode(sequence(to_date('2026-01-01'), to_date('2026-10-31'), interval 1 day)) as Date"
    )
    
    # Keep it strictly numeric (Integer/Date) to drastically reduce storage and memory footprint
    dim_calendar = date_sequence_df.select(
        col("Date").alias("Date_Key"),
        year(col("Date")).cast("int").alias("Year"),
        quarter(col("Date")).cast("int").alias("Quarter"),
        month(col("Date")).cast("int").alias("Month"),
        dayofmonth(col("Date")).cast("int").alias("Day")
    )
    df = spark.createDataFrame([(1,)], ["DummyCol"])

    # =========================================================================
    # 3. Building Enterprise Fact Sales Table
    # =========================================================================
    print(" Constructing fact_sales via structural staging table joins...")
    
    header_stg = spark.table("stg_sap_sales_header")
    items_stg = spark.table("stg_sap_sales_items")

    fact_sales = items_stg.join(
        header_stg, 
        items_stg.Sales_Order_ID == header_stg.Sales_Order_ID, 
        "inner"
    ).select(
        items_stg.Sales_Order_ID,
        items_stg.Order_Item_Line,
        header_stg.Order_Date,
        header_stg.Customer_ID,
        items_stg.Product_ID,
        items_stg.Quantity,
        items_stg.Item_Net_Amount.alias("Net_Amount"),
        items_stg.Item_Gross_Amount.alias("Gross_Amount")
    )

    # =========================================================================
    # 4. Target Write: Saving optimized Star Schema tables with Fabric V-Order
    # =========================================================================
    print("Committing final model tables to Gold Layer Workspace...")
    
    # Fabric implicitly handles V-Order sorting optimization on native saveAsTable execution
    dim_customers.write.format("delta").mode("overwrite").saveAsTable("LH_sap_gold.dbo.dim_customers")
    dim_products.write.format("delta").mode("overwrite").saveAsTable("LH_sap_gold.dbo.dim_products")
    dim_calendar.write.format("delta").mode("overwrite").saveAsTable("LH_sap_gold.dbo.dim_calender")
    df.write.format("delta").mode("overwrite").saveAsTable("LH_sap_gold.dbo._Key_Measures")
    fact_sales.write.format("delta").mode("overwrite").saveAsTable("LH_sap_gold.dbo.fact_sales")

    print("Gold Layer Dimensional Modeling completed successfully!")

if __name__ == "__main__":
    process_silver_to_gold()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
