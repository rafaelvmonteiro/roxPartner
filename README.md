# Rox Partner
### Introdução 

<!--ts-->
  * [Caminho](#caminho)
  * [Ferramentas](#ferramentas)
  * [Analisando os dados](#Analisando-os-dados)
  * [Quantidade de questões](#Quantidade-de-questões) 
    * [Questão 1](#Questão-1)
    * [Questão 2](#Questão-2)
    * [Questão 3](#Questão-3)
    * [Questão 4](#Questão-4)
    * [Questão 5](#Questão-5)
    * [Ecossistema Google Cloud Platform](#Ecossistema-Google-Cloud-Platform)
<!--te-->
### Caminho:
Inicialmente foi criado o datalake "Dl_RoxPartner", em seguida importei os CSV do teste para camada "raw" do datalake, posteriormente carreguei os dados para dentro do BigQuery, e no último momento fazendo as querys para a resolução das questões, automatizando dentro do código python, e salvado o output em CSV na camada "core" do datalake 


### Ferramentas:

Ferramentas que foram ultilzadas para realização das atividades:

- Google Cloud Platform
- Cloud Storage 
- BigQuery
- Python
- Cloud Shell
- diagrams.net

### Analisando os dados
```# -*- coding:utf-8 -*-
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

client = bigquery.Client() ###Inicia o cliente para acesso ao cloud BigQuery depois de importado as Bibliotecas
```
# Questão 1
### Com base na solução implantada responda aos seguintes questionamentos: escreva uma query que retorna a quantidade de linhas na tabela Sales.SalesOrderDetail pelo campo SalesOrderID,desde que tenham pelo menos três linhas de detalhes.
 
Usado o comando distinct para evitar duplicidade, depois converto o resultado da query para um dataframe Pandas após isso é convertido o resultado da tabela no BigQuery pra pandas e por final é ultilizado  o comando para salvar o dataframe em um CSV no datalake na camada Core

![1 certo](https://user-images.githubusercontent.com/110641665/183318431-9832bb4d-6ce3-4d4b-91d4-f818e4242ec3.PNG)




```
QUERY = 
SELECT
  count(distinct SalesOrderID) AS qtdSalesOrderID_Distintos
FROM dataset_roxpartner.tb_Sales_SalesOrderDetail
  WHERE OrderQty >= 3 

query_job = client.query(QUERY)
df = client.query(QUERY).to_dataframe() # converto o resultado da tabela no BigQuery pra pandas
print("Questão 1")
print(df)
df.to_csv(r'gs://roxpartner/Dl_RoxPartner/core/questao_1.csv', index = None, sep = ';', mode = 'a', header =True)
```
# Questão 2

### Escreva uma query que ligue as tabelas Sales.SalesOrderDetail, Sales.SpecialOfferProduct e Production.Product e retorne os 3 produtos (Name) mais vendidos(pela soma de OrderQty), agrupados pelo número de dias para manufatura(DaysToManufacture)
Primeiro foi selecionado as tabelas e as colunas após isso, foi feito o left join para criar uma junção externa esquerda, assim resultando no total de vendas

![2 certo](https://user-images.githubusercontent.com/110641665/183318447-e3074752-94cb-43d5-8c4e-b587a3fd0bd1.PNG)


```
QUERY =

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
query_job = client.query(QUERY)

### Converto o resultado da query para um dataframe Pandas
df = client.query(QUERY).to_dataframe() # converto o resultado da tabela no BigQuery pra pandas
print("Questão 2")
print(df)
### Comando para salvar o dataframe em um CSV no datalake na camada Core
df.to_csv (r'gs://roxpartner/Dl_RoxPartner/core/questao_2.csv', index = None, sep = ';', mode = 'a', header =True)
```

# Questão 3
### Escreva uma query ligando as tabelas Person_Person, Sales_Customer e Sales_SalesOrderHeader de forma a obter uma lista de nomes de clientes e uma contagem de pedidos efetuados.
Seguindo os mesmos passos de cima, selecionando colunas e tabelas, fazendo o left join para junção esqueda de tabela, resultando na nova tabela nome_cliente

![3 certo](https://user-images.githubusercontent.com/110641665/183318696-d104e99a-ec47-4e28-bbae-9d98c6aee9ca.PNG)



```
QUERY =
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

query_job = client.query(QUERY)

#Converto o resultado da query para um dataframe Pandas
df = client.query(QUERY).to_dataframe() # converto o resultado da tabela no BigQuery pra pandas
print("Questão 3")
print(df)
#Comando para salvar o dataframe em um CSV no datalake na camada Core
df.to_csv (r'gs://roxpartner/Dl_RoxPartner/core/questao_3.csv', index = None, sep = ';', mode = 'a', header =True)
converter a coluna OrderQty para int,pois será posteriormente usada operaçao de soma 
```
# Questão 4
### Escreva uma query usando as tabelas Sales.SalesOrderHeader, Sales.SalesOrderDetail e Production.Product, de forma a obter a soma total de produtos (OrderQty) por ProductID e OrderDate. 
Retornando a query usada na ultima questão, fazendo a seleção, left join e no final resultando um total de vendas

![4 certo](https://user-images.githubusercontent.com/110641665/183318671-2c2cff8d-eed1-4645-97af-c68aa0625b25.PNG)



```
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
```
# Questão 5
### Escreva uma query mostrando os campos SalesOrderID, OrderDate e TotalDue da tabela Sales.SalesOrderHeader. Obtenha apenas as linhas onde a ordem tenha sido feita durante o mês de setembro/2011 e o total devido esteja acima de 1.000.Ordene pelo total devido decrescente.
Selecionando e filtrando com a data pedida no comando da questão

![5 certo](https://user-images.githubusercontent.com/110641665/183318473-264c047e-1eb3-43af-81e4-066c2f177c92.PNG)


```
QUERY = 
SELECT SalesOrderID, OrderDate, TotalDue 
FROM `roxpartner-358401.dataset_roxpartner.tb_Sales_SalesOrderHeader`
WHERE TotalDue > 1000
AND STRING(OrderDate) LIKE "2011-09%"
ORDER BY TotalDue DESC

query_job = client.query(QUERY)
#Converto o resultado da query para um dataframe Pandas
df = client.query(QUERY).to_dataframe() # converto o resultado da tabela no BigQuery pra pandas
print("Questão 5")
print(df)
#Comando para salvar o dataframe em um CSV no datalake na camada Core
df.to_csv (r'gs://roxpartner/Dl_RoxPartner/core/questao_5.csv', index = None, sep = ';', mode = 'a', header =True)


```
### Ecossistema Google Cloud Platform

![Feito Diagrama Ferramentas roxPartner drawio](https://user-images.githubusercontent.com/110641665/183254184-37044e5a-0059-443a-bf0c-cacdbcf053e3.png)




### Cloud Storage        (Datalake)
![storage](https://user-images.githubusercontent.com/110641665/183229261-753fdef5-9560-41aa-a2dd-d370cf3b6c12.PNG)



### Cloud Storage
![storage 2](https://user-images.githubusercontent.com/110641665/183229268-5e5e8eeb-b29d-4d30-b46b-a83859b0566b.PNG)



### BigQuery
![big query](https://user-images.githubusercontent.com/110641665/183244946-41b03c3d-d312-4f79-be3d-91ae1f56b70d.PNG)



### BigQuery
![dataset](https://user-images.githubusercontent.com/110641665/183245007-14f3b044-9d9c-4d6f-b1df-efedfd9dc39d.PNG)





### Python
![python](https://user-images.githubusercontent.com/110641665/183245015-cc4bef1f-a6c4-4888-aa12-175ec304786c.PNG)


# Diagramas 
### Questão 2
![Diagrama roxPartner-Página-1 drawio](https://user-images.githubusercontent.com/110641665/183317022-dc2f0245-763b-491b-bb72-3b74cdbd8523.png)

### Questão 3
![Diagrama roxPartner-Página-2 drawio](https://user-images.githubusercontent.com/110641665/183317035-537c4944-1ad8-4765-88f3-c92f0b91d957.png)

### Questão 4
![Diagrama roxPartner-Página-3 drawio](https://user-images.githubusercontent.com/110641665/183317054-bfbfefe7-eb40-4ad4-85e0-07785b0fa64f.png)

### Questão 5
![Diagrama roxPartner-Página-4 drawio](https://user-images.githubusercontent.com/110641665/183317089-c0769010-64a3-4476-9040-b10612ca9d7c.png)








