-- Databricks notebook source
DROP TABLE if EXISTS Products;


-- COMMAND ----------

CREATE TABLE products USING csv 
OPTIONS (path "/FileStore/tables/Products.tsv", delimiter "\t", header "true")

-- COMMAND ----------

DROP TABLE if EXISTS OrderDetails;
CREATE TABLE orderdetails USING csv 
OPTIONS (path "/FileStore/tables/OrderDetails.tsv", delimiter "\t", header "true")

-- COMMAND ----------

select ProductID,Name,Weight from products order by Weight desc limit 15;


-- COMMAND ----------

select ProductCategoryID,count(ProductCategoryID) from products where ProductModelID is not NULL group by ProductCategoryID order by ProductCategoryID

-- COMMAND ----------

select count(distinct ProductModelID) from products where ProductModelID is not NULL

-- COMMAND ----------

select ProductModelID,count(ProductID) as total from products where ProductModelID is not NULL group by ProductModelID order by total desc limit 10

-- COMMAND ----------

select ProductID, Name, Color, Size, ListPrice from products where ProductModelID=20

-- COMMAND ----------

select sum(OrderQty) from orderdetails join products where orderdetails.ProductID=products.ProductID and products.ProductCategoryID=20

-- COMMAND ----------


