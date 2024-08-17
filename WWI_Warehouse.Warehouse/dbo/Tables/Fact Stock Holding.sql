CREATE TABLE [dbo].[Fact Stock Holding] (

	[Stock Holding Key] bigint NULL, 
	[Stock Item Key] int NULL, 
	[Quantity On Hand] int NULL, 
	[Bin Location] varchar(8000) NULL, 
	[Last Stocktake Quantity] int NULL, 
	[Last Cost Price] decimal(38,6) NULL, 
	[Reorder Level] int NULL, 
	[Target Stock Level] int NULL, 
	[Lineage Key] int NULL
);

