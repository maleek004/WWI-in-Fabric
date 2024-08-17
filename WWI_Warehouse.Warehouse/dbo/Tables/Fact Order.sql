CREATE TABLE [dbo].[Fact Order] (

	[Order Key] bigint NULL, 
	[City Key] int NULL, 
	[Customer Key] int NULL, 
	[Stock Item Key] int NULL, 
	[Order Date Key] date NULL, 
	[Picked Date Key] date NULL, 
	[Salesperson Key] int NULL, 
	[Picker Key] int NULL, 
	[WWI Order ID] int NULL, 
	[WWI Backorder ID] int NULL, 
	[Description] varchar(8000) NULL, 
	[Package] varchar(8000) NULL, 
	[Quantity] int NULL, 
	[Unit Price] decimal(38,6) NULL, 
	[Tax Rate] decimal(38,6) NULL, 
	[Total Excluding Tax] decimal(38,6) NULL, 
	[Tax Amount] decimal(38,6) NULL, 
	[Total Including Tax] decimal(38,6) NULL, 
	[Lineage Key] int NULL
);

