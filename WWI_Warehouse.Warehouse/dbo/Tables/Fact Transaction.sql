CREATE TABLE [dbo].[Fact Transaction] (

	[Transaction Key] bigint NULL, 
	[Date Key] date NULL, 
	[Customer Key] int NULL, 
	[Bill To Customer Key] int NULL, 
	[Supplier Key] int NULL, 
	[Transaction Type Key] int NULL, 
	[Payment Method Key] int NULL, 
	[WWI Customer Transaction ID] int NULL, 
	[WWI Supplier Transaction ID] int NULL, 
	[WWI Invoice ID] int NULL, 
	[WWI Purchase Order ID] int NULL, 
	[Supplier Invoice Number] varchar(8000) NULL, 
	[Total Excluding Tax] decimal(38,6) NULL, 
	[Tax Amount] decimal(38,6) NULL, 
	[Total Including Tax] decimal(38,6) NULL, 
	[Outstanding Balance] decimal(38,6) NULL, 
	[Is Finalized] bit NULL, 
	[Lineage Key] int NULL
);

