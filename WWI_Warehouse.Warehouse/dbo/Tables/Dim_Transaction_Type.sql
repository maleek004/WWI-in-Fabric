CREATE TABLE [dbo].[Dim_Transaction_Type] (

	[Transaction Type Key] int NULL, 
	[WWI Transaction Type ID] int NULL, 
	[Transaction Type] varchar(8000) NULL, 
	[Valid From] datetime2(6) NULL, 
	[Valid To] datetime2(6) NULL, 
	[Lineage Key] int NULL
);

