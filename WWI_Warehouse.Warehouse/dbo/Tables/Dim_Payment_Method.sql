CREATE TABLE [dbo].[Dim_Payment_Method] (

	[Payment Method Key] int NULL, 
	[WWI Payment Method ID] int NULL, 
	[Payment Method] varchar(8000) NULL, 
	[Valid From] datetime2(6) NULL, 
	[Valid To] datetime2(6) NULL, 
	[Lineage Key] int NULL
);

