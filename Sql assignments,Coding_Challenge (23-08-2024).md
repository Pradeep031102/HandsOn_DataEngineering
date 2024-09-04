# Sql assignments (23-08-2024)

-----1
select OrderID,sum(TotalAmount)as totalAmntspnt
from Orders
group by OrderID


---2
Select OrderID,SUM(TotalAmount) AS TotalSpent
FROM Orders
GROUP BY OrderID
having SUM(TotalAmount) > 1000;

---3
select Products.ProductID,count(Products.Category) AS TotalCateg
from Products
Group By ProductID
having Count(Category)> 5

---4
select Products.ProductName,supplierId 
count(Products.ProductID) AS totalProds
from products
group by ProductID,SupplierId;

---5
select Products.ProductName,Products.ProductID,sum(Products.StockQuantity) AS totalsales
FROM Products
group by ProductID,ProductName


---Procedure
-----1
Create Procedure GetAll
AS
BEGIN
SELECT* FROM Products
END;
EXEC GetAll

-----2
Create procedure GetProductByID
 @ProductID INT
 AS
 BEGIN
 SELECT * FROM Products
 WHERE ProductID =@ProductID
 end;

 exec GetProductByID @ProductId = 1;


-----3
 Create procedure GetProductByName
 @ProductName varchar(100)
 AS
 BEGIN
 SELECT * FROM Products
 WHERE ProductName =@ProductName
 end;

 exec GetProductByName @ProductName ='Laptop';

-------4
 create procedure GetCategoryandPrice
 @Category varchar(50),
 @Price Decimal(10,2)
 AS
 BEGIN
 SELECT * FROM Products
 WHERE Category = @Category
 AND Price =@Price
 end;


exec GetCategoryandPrice @Category ='Electronics',@Price = 75000.00;


----5
create Procedure insertOrder
@OrderId INT,
@ProductID INT,
@OrderDate Date,
@Quantity int,
@TotalAmount decimal(10,2)

as
begin
insert into Orders(OrderID,ProductID,OrderDate,Quantity,TotalAmount)
Values(@OrderID,@ProductID,@OrderDate,@Quantity,@TotalAmount)
end;

exec insertOrder 6,1,'2024-09-22',3,'22200.00';

----6
Create procedure UpdateOrder
@OrderID int,
@ProductID int,
@OrderDate date,
@Quantity int,
@TotalAmount decimal(10,2)

as
begin
update Orders
SET  ProductID = @ProductID,
OrderDate = @OrderDate,
Quantity = @Quantity,
TotalAmount =  @TotalAmount
where OrderID = @OrderID;
end;

exec UpdateOrder 1,1,'2023-05-30',7,'23333.00';

-----7
create procedure DeleteOrders

@OrderID int
AS
Begin

delete from Orders
where OrderID = @OrderID;


end;

Exec DeleteOrders 1;


# Coding Challenge 

---To Retrieve all products
SELECT *FROM Products
WHERE Category = 'Electronics'
AND Price > 500;
---Calculate the total quantity
Select sum(Quantity) as TotQuantity from Orders
---the total revenue generated 
SELECT ProductID, SUM(Quantity * TotalAmount) AS TotalRevenue
FROM Orders
GROUP BY ProductID;

---Order of Execution
SELECT ProductID,SUM(Quantity) AS TotalQuantity
FROM Orders
WHERE OrderDate <'2024-11-06'
GROUP BY ProductID
HAVING SUM(Quantity)<112
ORDER BY TotalQuantity Asc;

 ---Group and Filter Data
SELECT ProductID, COUNT(OrderID) AS OrderCount
FROM Orders
GROUP BY ProductID;


---Retrieve all customers who have placed more than 5 orders
SELECT c.CustomerID, c.FirstName, c.LastName, COUNT(o.OrderID) AS OrderCount
FROM Customers c
INNER JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID, c.FirstName, c.LastName
HAVING COUNT(o.OrderID) > 5;

------------------------------PROCEDURE

----1)a stored procedure named GetAllCustomers

CREATE PROCEDURE GetAllCustomers
AS
BEGIN
SELECT*FROM Customers;
END;

EXEC GetAllCustomers;


----2)Stored Procedure with Input Parameter
CREATE PROCEDURE GetOrderDetailsByOrderID
@OrderID INT
AS
BEGIN
SELECT Orders.OrderID,Customers.FirstName,Customers.LastName,Products.ProductName,Orders.Quantity,Orders.TotalAmount,Orders.OrderDate
FROM Orders
INNER JOIN Customers ON Orders.CustomerID = Customers.CustomerID
INNER JOIN Products ON Orders.ProductID = Products.ProductID
WHERE Orders.OrderID = @OrderID;
END;

EXEC GetOrderDetailsByOrderID @OrderID = 3;


---3  Stored Procedure with Multiple Input Parameters

CREATE PROCEDURE GetProductsByCategoryAndPrice
 @Category VARCHAR(50),
 @MinPrice DECIMAL(10, 2)
AS
BEGIN
SELECT ProductID, ProductName, Price, StockQuantity FROM Products
WHERE Category = @Category AND Price >= @MinPrice;
END;

EXEC GetProductsByCategoryAndPrice @Category = 'Electronics', @MinPrice = 20000.00;


---4)Stored Procedure with Insert Operation
CREATE PROCEDURE InsertNewProduct
 @ProductName VARCHAR(100),
 @Category VARCHAR(50),
 @Price DECIMAL(10, 2),
 @StockQuantity INT
AS
BEGIN
INSERT INTO Products (ProductName, Category, Price, StockQuantity)
VALUES (@ProductName, @Category, @Price, @StockQuantity);
END;

EXEC InsertNewProduct  @ProductName = 'Laptop',@Category = 'Electronics',@Price = 20000.00,@StockQuantity = 15;



---5)Stored Procedure with Update Operation
CREATE PROCEDURE UpdateCustomerEmail
@CustomerID INT,
@NewEmail VARCHAR(100)
AS
BEGIN
UPDATE Customers
SET Email = @NewEmail
WHERE CustomerID = @CustomerID;
END;

EXEC UpdateCustomerEmail @CustomerID = 3,@NewEmail = 'siddharth.singh@example.com';


----6. Stored Procedure with Delete Operation

CREATE PROCEDURE DeleteOrderByID
@OrderID INT
AS
BEGIN
DELETE FROM Orders
WHERE OrderID = @OrderID;
END;

EXEC DeleteOrderByID @OrderID = 3;


---7.stored Procedure with Output Parameter

CREATE PROCEDURE GetTotalProductsInCategory
@Category VARCHAR(50),
@TotalProducts INT OUTPUT
AS
BEGIN
SELECT @TotalProducts = COUNT(*)
FROM Products
WHERE Category = @Category;
END;

DECLARE @TotalProducts INT;
EXEC GetTotalProductsInCategory @Category = 'Furniture', @TotalProducts = @TotalProducts OUTPUT;
SELECT @TotalProducts AS TotalProducts;