CREATE TABLE [dbo].[Dim_Supplier] (

	[Supplier Key] int NULL, 
	[WWI Supplier ID] int NULL, 
	[Supplier] varchar(8000) NULL, 
	[Category] varchar(8000) NULL, 
	[Primary Contact] varchar(8000) NULL, 
	[Supplier Reference] varchar(8000) NULL, 
	[Payment Days] int NULL, 
	[Postal Code] varchar(8000) NULL, 
	[Valid From] datetime2(6) NULL, 
	[Valid To] datetime2(6) NULL, 
	[Lineage Key] int NULL
);

