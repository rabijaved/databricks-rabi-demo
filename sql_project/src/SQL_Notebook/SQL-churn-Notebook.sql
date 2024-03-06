-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC ### 1/ Loading our data using Databricks 
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-1.png"/>
-- MAGIC </div>
-- MAGIC   
-- MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.

-- COMMAND ----------

USE CATALOG uc_rabi;
USE SCHEMA customer_churn_sql;

-- COMMAND ----------

-- DBTITLE 1,Ingest raw orders from ERP
CREATE TABLE IF NOT EXISTS churn_orders_bronze;

COPY INTO churn_orders_bronze
FROM "/dbdemos/retail/c360/orders"
FILEFORMAT = JSON
FORMAT_OPTIONS ("mergeSchema" = "true")
COPY_OPTIONS ("mergeSchema" = "true")

-- COMMAND ----------

SELECT * FROM churn_orders_bronze LIMIT 20

-- COMMAND ----------

-- DBTITLE 1,Ingest raw user data
CREATE TABLE IF NOT EXISTS churn_users_bronze;

COPY INTO churn_users_bronze
FROM "/dbdemos/retail/c360/users"
FILEFORMAT = JSON
FORMAT_OPTIONS ("mergeSchema" = "true")
COPY_OPTIONS ("mergeSchema" = "true")

-- COMMAND ----------

SELECT * FROM churn_users_bronze LIMIT 20

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Silver tables for Data Analysts
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-2.png"/>
-- MAGIC </div>
-- MAGIC
-- MAGIC The next layer often call silver is consuming **incremental** data from the bronze one, and cleaning up some information.
-- MAGIC
-- MAGIC These tables are clean and ready to be used by the BI team!

-- COMMAND ----------

-- DBTITLE 1,Clean and anonymise User data
CREATE TABLE IF NOT EXISTS churn_users 
COMMENT "User data cleaned and anonymized for analysis."
AS SELECT
  id as user_id,
  sha1(email) as email, 
  to_timestamp(creation_date, "MM-dd-yyyy HH:mm:ss") as creation_date, 
  to_timestamp(last_activity_date, "MM-dd-yyyy HH:mm:ss") as last_activity_date, 
  initcap(firstname) as firstname, 
  initcap(lastname) as lastname, 
  address, 
  canal, 
  country,
  cast(gender as int),
  cast(age_group as int), 
  cast(churn as int) as churn
from churn_users_bronze

-- COMMAND ----------

-- DBTITLE 1,Clean orders
CREATE TABLE IF NOT EXISTS churn_orders 
COMMENT "Order data cleaned and anonymized for analysis."
AS SELECT
  cast(amount as int),
  id as order_id,
  user_id,
  cast(item_count as int),
  to_timestamp(transaction_date, "MM-dd-yyyy HH:mm:ss") as creation_date

from churn_orders_bronze

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/ Aggregate and join data to create our Gold tables
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-3.png"/>
-- MAGIC </div>
-- MAGIC
-- MAGIC We need to enrich our user dataset with extra information, such as:
-- MAGIC
-- MAGIC * last command date
-- MAGIC * number of items bought
-- MAGIC * ...

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS churn_features
COMMENT "Final user table with all information for Analysis / ML"
AS 
  WITH 
    churn_orders_stats AS (SELECT user_id, count(*) as order_count, sum(amount) as total_amount, sum(item_count) as total_item, max(creation_date) as last_transaction
      FROM churn_orders GROUP BY user_id)

  SELECT *, 
         datediff(now(), creation_date) as days_since_creation,
         datediff(now(), last_activity_date) as days_since_last_activity
       FROM churn_users
         INNER JOIN churn_orders_stats using (user_id)

-- COMMAND ----------

SELECT * FROM churn_features
