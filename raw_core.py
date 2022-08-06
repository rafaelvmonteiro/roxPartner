# -*- coding:utf-8 -*-
import sys
import pandas as pd
from io import StringIO
from IPython.display import display
import xlrd
from datetime import datetime, timedelta
from threading import Thread
import time
import os
os.system('ls -l')
import signal
import json
from google.cloud import storage
from google.cloud import bigquery
import ast
from pandas.io.json import json_normalize
from ast import literal_eval
import datetime as dt
from datetime import date, timedelta, datetime
import pytz

client = bigquery.Client() #Inicia o cliente para acesso ao cloud BigQuery
#Question 1
QUERY = """
SELECT
  count(distinct SalesOrderID) AS qtdSalesOrderID_Distintos
FROM dataset_roxpartner.tb_Sales_SalesOrderDetail
  WHERE OrderQty >= 3 
"""
query_job = client.query(QUERY)
#Converto o resultado da query para um dataframe Pandas
df = client.query(QUERY).to_dataframe() # converto o resultado da tabela no BigQuery pra pandas
print("Questão 1")
print(df)
#Comando para salvar o dataframe em um CSV no datalake na camada Core
df.to_csv (r'gs://roxpartner/Dl_RoxPartner/core/questao_1.csv', index = None, sep = ';', mode = 'a', header =True)

#Question 2
QUERY = """
WITH 
SalesOrderDetail AS (SELECT ProductID, OrderQty FROM `dataset_roxpartner.tb_Sales_SalesOrderDetail`),
SpecialOfferProduct AS (SELECT ProductID FROM `dataset_roxpartner.tb_Sales_SpecialOfferProduct`),
Production_Product AS (SELECT ProductID, DaysToManufacture FROM `dataset_roxpartner.tb_Production_Product`),

join1 AS (
SELECT  i.ProductID, i.OrderQty
FROM SalesOrderDetail AS i
LEFT JOIN SpecialOfferProduct ii
ON i.ProductID = ii.ProductID),

join2 AS (
SELECT  i.ProductID, i.OrderQty, ii.DaysToManufacture
FROM join1 AS i
LEFT JOIN Production_Product AS ii
ON i.ProductID = ii.ProductID)

SELECT ProductID,
SUM(OrderQty) AS total_vendas
FROM join2
GROUP BY ProductID
ORDER BY total_vendas DESC
LIMIT 3
"""
query_job = client.query(QUERY)

#Converto o resultado da query para um dataframe Pandas
df = client.query(QUERY).to_dataframe() # converto o resultado da tabela no BigQuery pra pandas
print("Questão 2")
print(df)
#Comando para salvar o dataframe em um CSV no datalake na camada Core
df.to_csv (r'gs://roxpartner/Dl_RoxPartner/core/questao_2.csv', index = None, sep = ';', mode = 'a', header =True)

#Question 3
QUERY = """
WITH 
Person_Person AS (SELECT FirstName, MiddleName, LastName, BusinessEntityID FROM `dataset_roxpartner.tb_Person_Person`),
Sales_Customer AS (SELECT CustomerID FROM `dataset_roxpartner.tb_Sales_Customer`),
Sales_SalesOrderHeader AS (SELECT CustomerID, SalesOrderID FROM `dataset_roxpartner.tb_Sales_SalesOrderHeader`),

join1 AS (
SELECT
  ii.FirstName,
  ii.MiddleName,
  ii.LastName,
  i.CustomerID
FROM Sales_Customer AS i
LEFT JOIN Person_Person AS ii
ON i.CustomerID = ii.BusinessEntityID),

join2 AS (
SELECT
    Concat(ii.FirstName," ",ii.LastName) as nome_cliente,
    i.SalesOrderID   
FROM Sales_SalesOrderHeader AS i
LEFT JOIN join1 AS ii
ON i.CustomerID = ii.CustomerID
WHERE FirstName IS NOT Null)

SELECT nome_cliente,
COUNT(Distinct SalesOrderID) AS totalSalesOrderID
FROM join2
GROUP BY nome_cliente
ORDER BY totalSalesOrderID DESC
"""
query_job = client.query(QUERY)

#Converto o resultado da query para um dataframe Pandas
df = client.query(QUERY).to_dataframe() # converto o resultado da tabela no BigQuery pra pandas
print("Questão 3")
print(df)
#Comando para salvar o dataframe em um CSV no datalake na camada Core
df.to_csv (r'gs://roxpartner/Dl_RoxPartner/core/questao_3.csv', index = None, sep = ';', mode = 'a', header =True)

#Question 4
QUERY = """
WITH 
SalesOrderDetail AS (SELECT ProductID, SalesOrderID, OrderQty FROM `dataset_roxpartner.tb_Sales_SalesOrderDetail`),
SalesOrderHeader AS (SELECT SalesOrderID, OrderDate FROM `dataset_roxpartner.tb_Sales_SalesOrderHeader`),
Production_Product AS (SELECT ProductID, DaysToManufacture FROM `dataset_roxpartner.tb_Production_Product`),

join1 AS (
SELECT  i.ProductID, i.OrderQty, ii.OrderDate
FROM SalesOrderDetail AS i
LEFT JOIN SalesOrderHeader AS ii
ON i.SalesOrderID = ii.SalesOrderID),

join2 AS (
SELECT  i.ProductID, i.OrderQty, ii.DaysToManufacture, i.OrderDate,
FROM join1 AS i
LEFT JOIN Production_Product AS ii
ON i.ProductID = ii.ProductID)

SELECT ProductID,
SUM(OrderQty) AS total_vendas,
OrderDate
FROM join2
GROUP BY ProductID, OrderDate
ORDER BY total_vendas DESC
"""
query_job = client.query(QUERY)
#
#Converto o resultado da query para um dataframe Pandas
df = client.query(QUERY).to_dataframe() # converto o resultado da tabela no BigQuery pra pandas
print("Questão 4")
print(df)
#Comando para salvar o dataframe em um CSV no datalake na camada Core
df.to_csv (r'gs://roxpartner/Dl_RoxPartner/core/questao_4.csv', index = None, sep = ';', mode = 'a', header =True)

#Question 5
QUERY = """
SELECT SalesOrderID, OrderDate, TotalDue 
FROM `roxpartner-358401.dataset_roxpartner.tb_Sales_SalesOrderHeader`
WHERE TotalDue > 1000
AND STRING(OrderDate) LIKE "2011-09%"
ORDER BY TotalDue DESC
"""
query_job = client.query(QUERY)
#Converto o resultado da query para um dataframe Pandas
df = client.query(QUERY).to_dataframe() # converto o resultado da tabela no BigQuery pra pandas
print("Questão 5")
print(df)
#Comando para salvar o dataframe em um CSV no datalake na camada Core
df.to_csv (r'gs://roxpartner/Dl_RoxPartner/core/questao_5.csv', index = None, sep = ';', mode = 'a', header =True)

