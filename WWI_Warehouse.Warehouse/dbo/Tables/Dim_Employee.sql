CREATE TABLE [dbo].[Dim_Employee] (

	[Employee Key] int NULL, 
	[WWI Employee ID] int NULL, 
	[Employee] varchar(8000) NULL, 
	[Preferred Name] varchar(8000) NULL, 
	[Is Salesperson] bit NULL, 
	[Valid From] datetime2(6) NULL, 
	[Valid To] datetime2(6) NULL, 
	[Lineage Key] int NULL
);

