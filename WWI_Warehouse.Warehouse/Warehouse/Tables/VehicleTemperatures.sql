CREATE TABLE [Warehouse].[VehicleTemperatures] (

	[VehicleTemperatureID] varchar(8000) NOT NULL, 
	[VehicleRegistration] varchar(8000) NOT NULL, 
	[ChillerSensorNumber] varchar(8000) NOT NULL, 
	[RecordedWhen] varchar(8000) NOT NULL, 
	[Temperature] varchar(8000) NOT NULL, 
	[FullSensorData] varchar(8000) NULL, 
	[IsCompressed] varchar(8000) NOT NULL, 
	[CompressedSensorData] varchar(8000) NULL
);

