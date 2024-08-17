# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "04416f22-63fd-43b3-8cc5-1152453f9e02",
# META       "default_lakehouse_name": "WWI_LH2",
# META       "default_lakehouse_workspace_id": "6913b082-1a16-4129-a9c8-6931309f9daa",
# META       "known_lakehouses": [
# META         {
# META           "id": "04416f22-63fd-43b3-8cc5-1152453f9e02"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # EDA of WWI Sales records 
# ###  Checking out the dataset  


# CELL ********************

display(spark.sql("""
        
        SELECT * FROM WWI_LH2.FactSale LIMIT 5
        
        
        """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### QESTION 1. Total Number of Sales Records
# The first step in our EDA is to check the total number of sales records in the table. This will give us an understanding of the dataset size we are working with.


# CELL ********************

display(spark.sql("""
        
SELECT COUNT(*) AS TotalSalesRecords
FROM WWI_LH2.FactSale;
        
        
        """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### QUESTION 2. Date Range of Sales Data
# Understanding the date range of the data helps us to know the period covered by the dataset, which is important for any time series analysis or trend identification.


# CELL ********************

display(spark.sql("""
        
SELECT 
    MIN(`Invoice Date Key`) AS StartDateKey,
    MAX(`Invoice Date Key`) AS EndDateKey
FROM 
    WWI_LH2.FactSale;
        
        
        """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## QUESTION 3. Summary Statistics for Key Numerical Columns

# CELL ********************

display(spark.sql("""
        
SELECT 
    MIN(Quantity) AS MinQuantity,
    MAX(Quantity) AS MaxQuantity,
    AVG(Quantity) AS AvgQuantity,
    STDDEV(Quantity) AS StdDevQuantity,
    MIN(`Unit Price`) AS MinUnitPrice,
    MAX(`Unit Price`) AS MaxUnitPrice,
    AVG(`Unit Price`) AS AvgUnitPrice,
    STDDEV(`Unit Price`) AS StdDevUnitPrice,
    MIN(`Total Excluding Tax`) AS MinTotalExcludingTax,
    MAX(`Total Excluding Tax`) AS MaxTotalExcludingTax,
    AVG(`Total Excluding Tax`) AS AvgTotalExcludingTax,
    STDDEV(`Total Excluding Tax`) AS StdDevTotalExcludingTax,
    MIN(`Tax Amount`) AS MinTaxAmount,
    MAX(`Tax Amount`) AS MaxTaxAmount,
    AVG(`Tax Amount`) AS AvgTaxAmount,
    STDDEV(`Tax Amount`) AS StdDevTaxAmount,
    MIN(Profit) AS MinProfit,
    MAX(Profit) AS MaxProfit,
    AVG(Profit) AS AvgProfit,
    STDDEV(Profit) AS StdDevProfit
FROM 
    WWI_LH2.FactSale;
        
        
        """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Number of customers that has been sold to 
# - checking the number of unique customers in the sales table will help us understand how big the company's customer base is 
# - This could later help in deciding how to visualize customer related facts about the dataset 

# CELL ********************

display(spark.sql("""
        
SELECT 
    COUNT(DISTINCT `Customer key`) `Number of Customers`
FROM 
    WWI_LH2.FactSale
;
        
        """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

display(spark.sql("""
        
SELECT 
    DISTINCT dcu.`Category` `Customer Categories`
FROM 
    WWI_LH2.FactSale fs 
JOIN WWI_LH2.Dim_Customer dcu 
ON fs.`Customer key` = dcu.`Customer key`
;
        
        """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## QUESTION 4. Top 10 Customers by Total Sales Amount
#  
# -Identifying the top 10 customers by total sales amount can provide insights into the most valuable customers and help target marketing and sales efforts more effectively.

# CELL ********************

display(spark.sql("""
        
SELECT 
    dcu.`Customer`,
    SUM(`Total Excluding Tax`) AS TotalSalesAmount
FROM 
    WWI_LH2.FactSale fs
JOIN WWI_LH2.Dim_Customer dcu
ON fs.`Customer Key`= dcu.`Customer Key`
GROUP BY 
    `Customer`
ORDER BY 
    TotalSalesAmount DESC
LIMIT 10      ;
        
        """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## QUESTION 5. Top 10 Best-Selling Products by Quantity Sold
# Finding the top 10 best-selling products by quantity sold helps understand which products are the most popular among customers.

# CELL ********************

display(spark.sql("""
        
SELECT 
    dsi.`Stock Item`,
    SUM(Quantity) AS TotalQuantitySold
FROM 
    WWI_LH2.FactSale fs
JOIN WWI_LH2.Dim_Stock_Item dsi
ON fs.`Stock Item Key` = dsi.`Stock Item Key`
GROUP BY 
    `Stock Item`
ORDER BY 
    TotalQuantitySold DESC        
LIMIT 10        ;
        """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## QUESTION 6. Distribution of Sales Across Different Cities
# Analyzing the distribution of sales across different cities helps identify regional performance and opportunities for targeted regional marketing strategies.

# CELL ********************

display(spark.sql("""
        
SELECT 
    dc.`City`,
    COUNT(`Sale Key`) AS NumberOfSales,
    SUM(`Total Excluding Tax`) AS TotalSalesAmount
FROM 
    WWI_LH2.FactSale fs
JOIN
    WWI_LH2.Dim_City dc
ON dc.`City Key` = fs.`City Key`
GROUP BY 
    `City`
ORDER BY 
    TotalSalesAmount DESC;        
        
        """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Looking at the distinct region , Subregion , Sales Territory and Country in the City dimension table

# CELL ********************

display(spark.sql("""
        
SELECT 
    DISTINCT Region , 
        Subregion,
        `Sales Territory`,
        Country
FROM 
    WWI_LH2.Dim_City 
        """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## QUESTION 7. TOP 10 Products with Highest Average Profit
# Understanding the average profit per sale and identifying products with the highest average profit can help in optimizing product strategies and focusing on high-margin products.

# CELL ********************

display(spark.sql("""

SELECT 
    dsi.`Stock Item`,
    AVG(Profit) AS AvgProfit
FROM 
    WWI_LH2.FactSale fs
JOIN
    WWI_LH2.Dim_Stock_Item dsi
ON fs.`Stock Item Key` = dsi.`Stock Item Key`
GROUP BY 
    dsi.`Stock Item`
ORDER BY 
    AvgProfit DESC
LIMIT 10;
        
        """))




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## QUESTION 8. Monthly Sales Trend 
# calculates the monthly sales trend from 2013 to 2016 to identify seasonal patterns and trends.

# CELL ********************

display(spark.sql("""

SELECT 
    date_format(`Invoice Date Key`, 'MMM') MonthName,
    ROUND(SUM(`Total Excluding Tax`),2) AS `Monthly Sales Amount`
FROM 
    WWI_LH2.FactSale
WHERE 
    `Invoice Date Key` BETWEEN '2013-01-01' AND '2016-12-31'
GROUP BY 
    date_format(`Invoice Date Key`, 'MMM')
ORDER BY CASE WHEN MonthName = 'Jan' THEN 1
              WHEN MonthName = 'Feb' THEN 2
              WHEN MonthName = 'Mar' THEN 3
              WHEN MonthName = 'Apr' THEN 4
              WHEN MonthName = 'May' THEN 5
              WHEN MonthName = 'Jun' THEN 6
              WHEN MonthName = 'Jul' THEN 7
              WHEN MonthName = 'Aug' THEN 8
              WHEN MonthName = 'Sep' THEN 9
              WHEN MonthName = 'Oct' THEN 10
              WHEN MonthName = 'Nov' THEN 11
              WHEN MonthName = 'Dec' THEN 12
              Else null END    
       """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##   QUESTION 9. Top 10 Sales person by Sales Amount
# Identifies the top 10 employees based on their sales amount to recognize high performers.

# CELL ********************

display(spark.sql("""


SELECT 
    de.`Employee`,
    FORMAT_NUMBER(SUM(`Total Excluding Tax`),2) AS `Total Sales Amount`
FROM 
     WWI_LH2.FactSale fs
JOIN WWI_LH2.Dim_Employee de
ON  fs.`Salesperson Key` = de.`Employee Key`
GROUP BY 
    `Employee`
ORDER BY 
    `Total Sales Amount` DESC
LIMIT 10;


      """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## QUESTION 10. Sales Distribution by Payment Method
# This query calculates the sales distribution by payment method to understand customer payment preferences.

# CELL ********************

display(spark.sql("""

SELECT 
    dpm.`Payment Method` ,
    COUNT(`Sale Key`) AS `Number of Sales`,
    FORMAT_NUMBER(SUM(fs.`Total Excluding Tax`),2) AS `Total Sales Amount`

FROM WWI_LH2.FactSale fs

JOIN WWI_LH2.FactTransaction ft
ON fs.`WWI Invoice ID` = ft.`WWI Invoice ID`

JOIN WWI_LH2.Dim_Payment_Method dpm
ON ft.`Payment Method Key` = dpm.`Payment Method Key`

GROUP BY 
    dpm.`Payment Method`
ORDER BY 
    `Total Sales Amount` DESC;

         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
SELECT 
   
    SUM(`Total Including Tax`) AS TotalSalesAmount
FROM 
    WWI_LH2.FactSale 
      """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## QUESTION 11. Total Purchase per Supplier
# This query calculates the total items purchases (i.e orders finalized) for each supplier to identify key suppliers.

# CELL ********************

display(spark.sql(""" 

SELECT 
    ds.`Supplier`,
    Count(*) `Total orders placed`,
    SUM(fp.`Ordered Quantity`) AS `Total Items Ordered`
FROM 
    WWI_LH2.`FactPurchase` fp
JOIN WWI_LH2.Dim_Supplier ds 
ON fp.`Supplier Key` = ds.`Supplier Key`
WHERE fp.`Is Order Finalized`=1
GROUP BY 
    `Supplier`
ORDER BY 
    `Total Items Ordered` DESC;
         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 12. Inventory Levels for Stock Items
# This query calculates the current inventory levels for each stock item to manage stock effectively.

# CELL ********************


display(spark.sql(""" 

SELECT 
    `Stock Item`,
    FORMAT_NUMBER( SUM(`Quantity On Hand`), 0) AS `Total Quantity`
FROM 
    WWI_LH2.`FactStockHolding` fsh
JOIN WWI_LH2.Dim_Stock_Item dst 
ON fsh.`Stock Item Key` = dst.`Stock Item Key`
GROUP BY 
    `Stock Item`
ORDER BY 
    SUM(`Quantity On Hand`)  ASC;

         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 13. Transaction Type Distribution
# This query calculates the distribution of transaction types to understand the nature of business transactions.

# CELL ********************

display(spark.sql(""" 

SELECT 
    dtt.`Transaction Type`,
    COUNT(`Transaction Key`) AS `Number of Transactions`,
    SUM(`Total Excluding Tax`) AS `Total Transaction Amount`
FROM 
    WWI_LH2.`FactTransaction` ft
JOIN WWI_LH2.`Dim_Transaction_Type` dtt
ON ft.`Transaction Type Key` = dtt.`Transaction Type Key`
GROUP BY 
    `Transaction Type`
ORDER BY 
    `Total Transaction Amount` DESC;
         

         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 14. Customer Lifetime Value
# Calculating the lifetime value of each customer, considering their total purchases and profit margin

# CELL ********************

display(spark.sql(""" 

-- WITH CustomerSales AS (
--    SELECT
--        c.`Customer`,
--        SUM(s.`Total Including Tax`) AS `Total Revenue`,
--        SUM(s.`Profit`) AS `Total Profit`
--    FROM 
--        WWI_LH2.FactSale s
--    JOIN 
--        WWI_LH2.Dim_Customer c ON s.`Customer Key` = c.`Customer Key`
--    GROUP BY 
--        c.`Customer`
--)
--
--    SELECT
--        `Customer`,
--        `Total Revenue`,
--        `Total Profit`,
--        `Total Profit` / `Total Revenue` AS `Profit Margin`
--    FROM 
--        CustomerSales
--    ORDER BY 
--    `Total Profit` DESC;

-- PS:  i just found out about lateral solumn , tried it out in this version of thesame query below... its pretty cool 😎
    SELECT
       c.`Customer`,
       SUM(s.`Total Including Tax`) AS `Total Revenue`,
       SUM(s.`Profit`) AS `Total Profit`,
       SUM(s.`Profit`) / `Total Revenue` AS `Profit Margin`
   FROM 
      WWI_LH2.FactSale s
    JOIN 
        WWI_LH2.Dim_Customer c ON s.`Customer Key` = c.`Customer Key`
   GROUP BY 
        c.`Customer`
    ORDER BY 
        `Total Profit` DESC;
         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 15. Average Delivery Time
# Checking the average delivery time for all transactions in the sales table  

# CELL ********************

display(spark.sql(""" 

SELECT
    AVG(DATEDIFF(`Delivery Date Key`, `Invoice Date Key`)) AS `Avg Delivery Days`
FROM 
    WWI_LH2.`FactSale` 
         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Distribution of expected delivery days 
# This gives us information about how much of the different available products are expected to be delivered in different delivery date bins

# CELL ********************

display(spark.sql(""" 
SELECT distinct `Lead Time Days` ,
    COUNT(*)
FROM WWI_LH2.Dim_Stock_Item    
GROUP BY `Lead Time Days`  
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Number of Unique products
# Checking the number of unique products in stock .  
# Gives us a quick information about how big the number of products in stock are , could help decide whether to use the product name as a slicer on a report 

# CELL ********************

display(spark.sql(""" 
SELECT COUNT(distinct `Stock Item`) 
FROM WWI_LH2.Dim_Stock_Item    
  
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 
SELECT distinct `Brand` 
FROM WWI_LH2.Dim_Stock_Item    
  
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Investigating query results 

# MARKDOWN ********************

# ## Investigating the Result of Query 10 ( sales by Payment_method query)
# Our last query shows that all records in the sales fact table has a payment method of Unknown , now i want to trace the records from the payment method table to the fact sales table to see where the other payment methods got missing 
#  

# MARKDOWN ********************

# #### Checking all the category of payment methods in the payment_method dimensions table 

# CELL ********************

display(spark.sql(""" 

SELECT 
        DISTINCT `Payment Method` 

FROM WWI_LH2.Dim_Payment_Method

         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Checking the payment methods that are in the Transactions table

# CELL ********************

display(spark.sql(""" 

SELECT  `Payment Method`,
        COUNT(*) 

FROM WWI_LH2.FactTransaction ft 
JOIN WWI_LH2.Dim_Payment_Method dpm
ON ft.`Payment Method Key` = dpm.`Payment Method Key`
GROUP BY 1
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Confirming if there are any records with the 'EFT' payment method in the sales table as its the only other payment method available in the Transactions table which the Sales table is directly linked to 

# CELL ********************

display(spark.sql(""" 

SELECT `WWI Invoice ID`
FROM WWI_LH2.FactSale
WHERE `WWI Invoice ID` IN(
    SELECT   `WWI Invoice ID`
    FROM WWI_LH2.FactTransaction ft 
    JOIN WWI_LH2.Dim_Payment_Method dpm
    ON ft.`Payment Method Key` = dpm.`Payment Method Key`
    WHERE `Payment Method` = 'EFT')

         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Question 10 investigation conclusion:
# - The original query's result seems to be correct. all records in the sales table has a payment method of 'Unknown'. 
# - The 'EFT' payment method is in the transaction table but the records are not linked to the sales table.
# <br>
# <br>
# 
# ## Investigating the relationship between the sales and transactions fact tables ; and related dimension tables with `WWI Invoice ID` as column of interest


# MARKDOWN ********************

# #### Comparing the distribution of invoice id in the transaction and sales tables , this will help in understanding the type of relationship between the 2 tables  

# CELL ********************

display(spark.sql(""" 
SELECT COUNT( Distinct `WWI Invoice ID`) AS `Distinct Transactions table Invoice ID`,
        COUNT(`WWI Invoice ID`) AS `Total Transactions table Invoice ID`
FROM WWI_LH2.FactTransaction
         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 
SELECT COUNT( Distinct `WWI Invoice ID`) AS `Distinct sales table Invoice ID`,
        COUNT( `WWI Invoice ID`) AS `Total Sales table Invoice ID`
FROM WWI_LH2.FactSale
         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Checking all the transaction types in the transaction table , in a bid to know how the sales table links to the transaction type table through the transactions table 

# CELL ********************

display(spark.sql(""" 

SELECT  DISTINCT `Transaction Type Key`
FROM WWI_LH2.FactTransaction
        """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 

SELECT  DISTINCT `Transaction Type Key`
FROM WWI_LH2.FactTransaction
WHERE `WWI Invoice ID` IN (
    SELECT  Distinct `WWI Invoice ID`
    FROM WWI_LH2.FactSale
    )
         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Since all records in the sales table only links to transaction type 1 in the transaction table, checking to see if there are invoice ids for other transaction types in the transaction table. This will help to confirm if the transaction table's `WWI Invoice ID` column links to other tables in the database apart from sales table

# CELL ********************

display(spark.sql(""" 
SELECT COUNT(DISTINCT`WWI Invoice ID`) `No of invoices for other transactions`
FROM WWI_LH2.FactTransaction 
WHERE  `Transaction Type Key` IN(3,5,7)
        """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Looking at the distinct transaction types (in the transactions table) where the invoice id is not linked to the sales table

# CELL ********************

display(spark.sql(""" 

--SELECT  DISTINCT `Transaction Type Key`
--FROM WWI_LH2.FactTransaction
--WHERE `WWI Invoice ID` IS NULL OR `WWI Invoice ID` NOT IN (
--    SELECT  Distinct `WWI Invoice ID`
--    FROM WWI_LH2.FactSale
--    )

SELECT  DISTINCT `Transaction Type Key`
FROM WWI_LH2.FactTransaction
WHERE COALESCE(`WWI Invoice ID`, 'unknown') NOT IN (
    SELECT  Distinct `WWI Invoice ID`
    FROM WWI_LH2.FactSale
    )   
         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""

SELECT COUNT(`WWI Purchase Order ID`) 
FROM WWI_LH2.FactTransaction 
WHERE `WWI Invoice ID` IS NULL
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Checking to see if the sales table could be linked to the suppliers table via the transactions table 😑

# CELL ********************

display(spark.sql(""" 

SELECT  DISTINCT `Supplier Key`
FROM WWI_LH2.FactTransaction
WHERE `WWI Invoice ID` IN (
    SELECT  Distinct `WWI Invoice ID`
    FROM WWI_LH2.FactSale
    )
         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Conclusions on sales to transaction table relationship 
# 
# - The 'WWI Invoice ID' column in the Transactions table is not NULL only in rows where the transaction types is 'customer invoice' (70510 records which is also the number of distinct invoice ids in the sales table) , other transaction types in the transaction  table don't have an invoice ID  
# - This makes the sale table have a many to one relationship with the transactions table using the invoice id column
# - so every record in the sales table can be linked to the transaction type and payment method dimension tables using the Invoice ID column in the transactions table 
# - Records in the transaction table that dont have a `WWI Invoice ID` ie they are not a customer invoice transaction, has a `WWI Purchase Order ID` ie they are a supplier invoice transaction 
# The sales table cannot be linked to the suppliers table 

# MARKDOWN ********************

# ## INVESTIGATING THE DIFFERENCE BETWEEN STOCK ITEM AND PACKAGE IN THE SALES TABLE 

# CELL ********************

display(spark.sql(""" 
SELECT DISTINCT dsi.`Stock Item`,
                fs.Package
FROM WWI_LH2.FactSale fs
JOIN WWI_LH2.Dim_Stock_Item dsi
ON fs.`Stock Item Key` = dsi.`Stock Item Key`
         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Checking to see if each distinct invoice id (invoice ids are repeated in the sales table for each item sold for that invoice ) in the sales table will have the same packaging type

# CELL ********************

display(spark.sql(""" 
SELECT `WWI Invoice ID`,
        COUNT(DISTINCT `Package`) `Number of Packages`
FROM WWI_LH2.FactSale  
GROUP BY 1       
ORDER BY `Number of Packages` DESC
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## CONCLUSION ON PACKAGE VS STOCK ITEM
# - So stock item is the actual item that was sold and the package column tell us the kind of package the item was sold in 
# - Each distinct invoice id will appear on every row of item sold on the invoice
# - An invoice id might have multiple and different types of package associated with it based on the number of items on that invoice  

# CELL ********************

display(spark.sql(""" 

         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 

         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 

         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 

         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 

         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 

         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 

         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 

         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 

         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 

         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql(""" 

         
         """)) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
