CREATE TABLE [dbo].[Dim_Stock_Item] (

	[Stock Item Key] int NULL, 
	[WWI Stock Item ID] int NULL, 
	[Stock Item] varchar(8000) NULL, 
	[Color] varchar(8000) NULL, 
	[Selling Package] varchar(8000) NULL, 
	[Buying Package] varchar(8000) NULL, 
	[Brand] varchar(8000) NULL, 
	[Size] varchar(8000) NULL, 
	[Lead Time Days] int NULL, 
	[Quantity Per Outer] int NULL, 
	[Is Chiller Stock] bit NULL, 
	[Barcode] varchar(8000) NULL, 
	[Tax Rate] decimal(38,6) NULL, 
	[Unit Price] decimal(38,6) NULL, 
	[Recommended Retail Price] decimal(38,6) NULL, 
	[Typical Weight Per Unit] decimal(38,6) NULL, 
	[Valid From] datetime2(6) NULL, 
	[Valid To] datetime2(6) NULL, 
	[Lineage Key] int NULL
);

