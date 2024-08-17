CREATE TABLE [SalesReport].[FactSales2] (

	[Sale Key] bigint NULL, 
	[City Key] int NULL, 
	[Customer Key] int NULL, 
	[Bill To Customer Key] int NULL, 
	[Stock Item Key] int NULL, 
	[Invoice Date Key] date NULL, 
	[Delivery Date Key] date NULL, 
	[Salesperson Key] int NULL, 
	[WWI Invoice ID] int NULL, 
	[Description] varchar(8000) NULL, 
	[Package] varchar(8000) NULL, 
	[Quantity] int NULL, 
	[Unit Price] decimal(38,6) NULL, 
	[Tax Rate] decimal(38,6) NULL, 
	[Total Excluding Tax] decimal(38,6) NULL, 
	[Tax Amount] decimal(38,6) NULL, 
	[Profit] decimal(38,6) NULL, 
	[Total Including Tax] decimal(38,6) NULL, 
	[Total Dry Items] int NULL, 
	[Total Chiller Items] int NULL, 
	[Payment Method Key] int NULL
);

