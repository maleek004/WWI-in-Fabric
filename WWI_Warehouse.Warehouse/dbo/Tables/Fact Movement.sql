CREATE TABLE [dbo].[Fact Movement] (

	[Movement Key] bigint NULL, 
	[Date Key] date NULL, 
	[Stock Item Key] int NULL, 
	[Customer Key] int NULL, 
	[Supplier Key] int NULL, 
	[Transaction Type Key] int NULL, 
	[WWI Stock Item Transaction ID] int NULL, 
	[WWI Invoice ID] int NULL, 
	[WWI Purchase Order ID] int NULL, 
	[Quantity] int NULL, 
	[Lineage Key] int NULL
);

