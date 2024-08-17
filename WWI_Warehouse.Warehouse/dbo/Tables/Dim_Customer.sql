CREATE TABLE [dbo].[Dim_Customer] (

	[Customer Key] int NULL, 
	[WWI Customer ID] int NULL, 
	[Customer] varchar(8000) NULL, 
	[Bill To Customer] varchar(8000) NULL, 
	[Category] varchar(8000) NULL, 
	[Buying Group] varchar(8000) NULL, 
	[Primary Contact] varchar(8000) NULL, 
	[Postal Code] varchar(8000) NULL, 
	[Valid From] datetime2(6) NULL, 
	[Valid To] datetime2(6) NULL, 
	[Lineage Key] int NULL
);

