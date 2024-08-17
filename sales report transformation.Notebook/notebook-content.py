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

# CELL ********************

df = spark.sql("""
SELECT 
    fs.`Sale Key`,
    fs.`City Key`,
    fs.`Customer Key`,
    fs.`Bill To Customer Key`,
    fs.`Stock Item Key`,
    fs.`Invoice Date Key`,
    fs.`Delivery Date Key`,
    fs.`Salesperson Key`,
    fs.`WWI Invoice ID`,
    fs.`Description`,
    fs.`Package`,
    fs.`Quantity`,
    fs.`Unit Price`,
    fs.`Tax Rate`,
    fs.`Total Excluding Tax`,
    fs.`Tax Amount`,
    fs.`Profit`,
    fs.`Total Including Tax`,
    fs.`Total Dry Items`,
    fs.`Total Chiller Items`,
    ft.`Payment Method Key`

FROM 
    WWI_LH2.FactSale as fs
JOIN 
    WWI_LH2.FactTransaction as ft
ON 
    fs.`WWI Invoice ID` = ft.`WWI Invoice ID`;

    """)

df.write.format("delta").option('delta.columnMapping.mode' , 'name').saveAsTable("Sales")
print("Table saved successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SELECT 
    fs.[Sale Key],
    fs.[City Key],
    fs.[Customer Key],
    fs.[Bill To Customer Key],
    fs.[Stock Item Key],
    fs.[Invoice Date Key],
    fs.[Delivery Date Key],
    fs.[Salesperson Key],
    fs.[WWI Invoice ID],
    fs.[Description],
    fs.[Package],
    fs.[Quantity],
    fs.[Unit Price],
    fs.[Tax Rate],
    fs.[Total Excluding Tax],
    fs.[Tax Amount],
    fs.[Profit],
    fs.[Total Including Tax],
    fs.[Total Dry Items],
    fs.[Total Chiller Items],
    ft.[Payment Method Key]
INTO [SalesReport].[FactSales2]
FROM 
    [WWI_Warehouse].[dbo].[Fact Sale] as fs
JOIN 
    [WWI_Warehouse].[dbo].[Fact Transaction] as ft
ON 
    fs.[WWI Invoice ID] = ft.[WWI Invoice ID];



""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
