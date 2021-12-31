

If (object_id('vETLDimCustomersData') is not null) Drop View vETLDimCustomersData;
go
CREATE VIEW vETLDimCustomersData
AS
SELECT
  [CustomerID] = CustomerID
, [CompanyName] = CAST(CompanyName AS nvarchar(200))
, [ContactFullName] = CAST(FirstName + ' ' + LastName AS nvarchar(200)) 
FROM [AdventureWorksLT2012].[SalesLT].[Customer]
go


If (object_id('vETLDimProductsData') is not null) Drop View vETLDimProductsData;
go
CREATE VIEW vETLDimProductsData
AS
SELECT
  [ProductID] = T1.ProductID
, [ProductName] = CAST(T1.Name AS nvarchar(50))
, [ProductColor] = CAST(IIF(T1.Color is NULL, 'Depends on other things', T1.Color) AS nvarchar(50))
, [ProductListPrice] = CAST(T1.ListPrice AS money)
, [ProductSize] = CAST(IIF(T1.Size is NULL, 'One Size Only', T1.Size) AS nvarchar(50))
, [ProductWeight] = CAST(T1.Weight AS decimal(8,2))
, [ProductCategoryID] = T1.ProductCategoryID
, [ProductCategoryName] = CAST(T2.Name AS nvarchar(50))
FROM 
[AdventureWorksLT2012].[SalesLT].[Product] as T1 
JOIN [AdventureWorksLT2012].[SalesLT].[ProductCategory] as T2
ON T1.ProductCategoryID = T2.ProductCategoryID
go


If (object_id('vETLFactSalesData') is not null) Drop View vETLFactSalesData;
go
CREATE VIEW vETLFactSalesData
AS
SELECT
  [SalesOrderID] = T1.SalesOrderID
, [SalesOrderDetailID] = T2.SalesOrderDetailID
, [CustomerKey] = T4.CustomerKey
, [ProductKey] = T3.ProductKey
, [OrderDateKey] = T5.CalendarDateKey
, [ShippedDateKey] = T6.CalendarDateKey
, [OrderQty] = T2.OrderQty
, [UnitPrice] = T2.UnitPrice
, [UnitPriceDiscount] = T2.UnitPriceDiscount
FROM [AdventureWorksLT2012].[SalesLT].SalesOrderHeader as T1
JOIN [AdventureWorksLT2012].[SalesLT].SalesOrderDetail as T2
ON T1.SalesOrderID = T2.SalesOrderID
JOIN [DWAdventureWorksLT2012Lab01].[dbo].[DimProducts] as T3
ON T2.ProductID = T3.ProductID
JOIN [DWAdventureWorksLT2012Lab01].[dbo].[DimCustomers] as T4
ON T1.CustomerID = T4.CustomerID
JOIN [DWAdventureWorksLT2012Lab01].[dbo].[DimDates] as T5
ON T1.OrderDate = T5.CalendarDate
JOIN [DWAdventureWorksLT2012Lab01].[dbo].[DimDates] as T6
ON T1.ShipDate = T6.CalendarDate
go


-- Create an ETL Stored Procedures
If (object_id('pETLFillDimCustomers') is not null) Drop Procedure pETLFillDimCustomers;
go
CREATE  -- ETL Stored Procedure for DimCustomers
PROCEDURE pETLFillDimCustomers
AS

Begin
 Declare 
   @RC int = 0;
 Begin Try 
  Begin Transaction; 
INSERT INTO [DWAdventureWorksLT2012Lab01].[dbo].[DimCustomers]
( [CustomerID]
, [CompanyName]
, [ContactFullName]
)
SELECT 
	  [CustomerID]
	, [CompanyName]
	, [ContactFullName]
FROM  [DWAdventureWorksLT2012Lab01].[dbo].[vETLDimCustomersData] 
  Commit Transaction;
  Set @RC = 100; -- Success
 End Try
 Begin Catch
  Rollback Tran;
  Set @RC = -100; -- Failure
 End Catch
 Return @RC;
End
;
go


If (object_id('pETLFillDimProducts') is not null) Drop Procedure pETLFillDimProducts;
go
CREATE  -- ETL Stored Procedure for DimProducts
PROCEDURE pETLFillDimProducts
AS

Begin
 Declare 
   @RC int = 0;
 Begin Try 
  Begin Transaction; 

INSERT INTO [DWAdventureWorksLT2012Lab01].[dbo].[DimProducts]
( [ProductID]
, [ProductName]
, [ProductColor]
, [ProductListPrice]
, [ProductSize]
, [ProductWeight]
, [ProductCategoryID]
, [ProductCategoryName]
)
SELECT 
	  [ProductID]
	, [ProductName]
	, [ProductColor]
	, [ProductListPrice]
	, [ProductSize]
	, [ProductWeight]
	, [ProductCategoryID]
	, [ProductCategoryName]
FROM  [DWAdventureWorksLT2012Lab01].[dbo].[vETLDimProductsData]
  
  Commit Transaction;
  Set @RC = 100; -- Success
 End Try
 Begin Catch
  Rollback Tran;
  Set @RC = -100; -- Failure
 End Catch
 Return @RC;
End
;
go


If (object_id('pETLFillFactSales') is not null) Drop Procedure pETLFillFactSales;
go
CREATE  -- ETL Stored Procedure for FactSales
PROCEDURE pETLFillFactSales
AS

Begin
 Declare 
   @RC int = 0;
 Begin Try 
  Begin Transaction; 

INSERT INTO [DWAdventureWorksLT2012Lab01].[dbo].[FactSales]
( [SalesOrderID]
, [SalesOrderDetailID]
, [CustomerKey]
, [ProductKey]
, [OrderDateKey]
, [ShipDateKey]
, [OrderQty]
, [UnitPrice]
, [UnitPriceDiscount]
)
SELECT 
	  [SalesOrderID]  
	, [SalesOrderDetailID] 
	, [CustomerKey]
	, [ProductKey]
	, [OrderDateKey]
	, [ShippedDateKey]
	, [OrderQty]
	, [UnitPrice]
	, [UnitPriceDiscount]
FROM [DWAdventureWorksLT2012Lab01].[dbo].[vETLFactSalesData] 
  
  Commit Transaction;
  Set @RC = 100; -- Success
 End Try
 Begin Catch
  Rollback Tran;
  Set @RC = -100; -- Failure
 End Catch
 Return @RC;
End
;
go


-- Drop Foreign Key Constraints
ALTER TABLE dbo.FactSales DROP CONSTRAINT
	fkFactSalesToDimProducts;

ALTER TABLE dbo.FactSales DROP CONSTRAINT 
	fkFactSalesToDimCustomers;

ALTER TABLE dbo.FactSales DROP CONSTRAINT
	fkFactSalesOrderDateToDimDates;

ALTER TABLE dbo.FactSales DROP CONSTRAINT
	fkFactSalesShipDateDimDates;			


TRUNCATE TABLE dbo.FactSales;
TRUNCATE TABLE dbo.DimCustomers;
TRUNCATE TABLE dbo.DimProducts; 
  

-- DimCustomers
Declare @ReturnCode int
Execute @ReturnCode = pETLFillDimCustomers
Select [Return Status for pETLFillDimCustomers ] = @ReturnCode
go

-- DimProducts
Declare @ReturnCode int
Execute @ReturnCode = pETLFillDimProducts
Select [Return Status for pETLFillDimProducts] = @ReturnCode
go


-- Fact Sales 
Declare @ReturnCode int
Execute @ReturnCode = pETLFillFactSales
Select [Return Status for pETLFillFactSales] = @ReturnCode
go


ALTER TABLE dbo.FactSales ADD CONSTRAINT
	fkFactSalesToDimProducts FOREIGN KEY (ProductKey) 
	REFERENCES dbo.DimProducts	(ProductKey);

ALTER TABLE dbo.FactSales ADD CONSTRAINT 
	fkFactSalesToDimCustomers FOREIGN KEY (CustomerKey) 
	REFERENCES dbo.DimCustomers (CustomerKey);
 
ALTER TABLE dbo.FactSales ADD CONSTRAINT
	fkFactSalesOrderDateToDimDates FOREIGN KEY (OrderDateKey) 
	REFERENCES dbo.DimDates(CalendarDateKey);

ALTER TABLE dbo.FactSales ADD CONSTRAINT
	fkFactSalesShipDateDimDates FOREIGN KEY (ShipDateKey)
	REFERENCES dbo.DimDates (CalendarDateKey);
 
 
-- Dimension Tables
SELECT * FROM [DWAdventureWorksLT2012Lab01].[dbo].[DimCustomers]; 
SELECT * FROM [DWAdventureWorksLT2012Lab01].[dbo].[DimProducts]; 
SELECT * FROM [DWAdventureWorksLT2012Lab01].[dbo].[DimDates]; 

-- Fact Tables 
SELECT * FROM [DWAdventureWorksLT2012Lab01].[dbo].[FactSales]; 
