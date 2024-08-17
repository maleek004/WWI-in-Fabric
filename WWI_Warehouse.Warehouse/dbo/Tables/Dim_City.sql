CREATE TABLE [dbo].[Dim_City] (

	[City Key] int NULL, 
	[WWI City ID] int NULL, 
	[City] varchar(8000) NULL, 
	[State Province] varchar(8000) NULL, 
	[Country] varchar(8000) NULL, 
	[Continent] varchar(8000) NULL, 
	[Sales Territory] varchar(8000) NULL, 
	[Region] varchar(8000) NULL, 
	[Subregion] varchar(8000) NULL, 
	[Location] varchar(8000) NULL, 
	[Latest Recorded Population] bigint NULL, 
	[Valid From] datetime2(6) NULL, 
	[Valid To] datetime2(6) NULL, 
	[Lineage Key] int NULL
);

