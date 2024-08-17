CREATE TABLE [dbo].[Fact Purchase] (

	[Purchase Key] bigint NULL, 
	[Date Key] date NULL, 
	[Supplier Key] int NULL, 
	[Stock Item Key] int NULL, 
	[WWI Purchase Order ID] int NULL, 
	[Ordered Outers] int NULL, 
	[Ordered Quantity] int NULL, 
	[Received Outers] int NULL, 
	[Package] varchar(8000) NULL, 
	[Is Order Finalized] bit NULL, 
	[Lineage Key] int NULL
);

