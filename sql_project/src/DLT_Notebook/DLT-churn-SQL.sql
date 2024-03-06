-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC ### 1/ Loading our data using Databricks Autoloader (cloud_files)
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-1.png"/>
-- MAGIC </div>
-- MAGIC   
-- MAGIC Autoloader allow us to efficiently ingest millions of files from a cloud storage, and support efficient schema inference and evolution at scale.
-- MAGIC
-- MAGIC For more details on autoloader, run `dbdemos.install('auto-loader')`
-- MAGIC
-- MAGIC Let's use it to our pipeline and ingest the raw JSON & CSV data being delivered in our blob storage `/demos/retail/churn/...`. 

-- COMMAND ----------

-- DBTITLE 1,Ingest raw orders from ERP
CREATE STREAMING TABLE churn_orders_bronze (
  CONSTRAINT orders_correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "Spending score from raw data"
AS SELECT * FROM cloud_files("/dbdemos/retail/c360/orders", "json")

-- COMMAND ----------

-- DBTITLE 1,Ingest raw user data
CREATE STREAMING TABLE churn_users_bronze (
  CONSTRAINT correct_schema EXPECT (_rescued_data IS NULL)
)
COMMENT "raw user data coming from json files ingested in incremental with Auto Loader to support schema inference and evolution"
AS SELECT * FROM cloud_files("/dbdemos/retail/c360/users", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 2/ Enforce quality and materialize our tables for Data Analysts
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-2.png"/>
-- MAGIC </div>
-- MAGIC
-- MAGIC The next layer often call silver is consuming **incremental** data from the bronze one, and cleaning up some information.
-- MAGIC
-- MAGIC We're also adding an [expectation](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html) on different field to enforce and track our Data Quality. This will ensure that our dashboards are relevant and easily spot potential errors due to data anomaly.
-- MAGIC
-- MAGIC For more advanced DLT capabilities run `dbdemos.install('dlt-loans')` or `dbdemos.install('dlt-cdc')` for CDC/SCDT2 example.
-- MAGIC
-- MAGIC These tables are clean and ready to be used by the BI team!

-- COMMAND ----------

-- DBTITLE 1,Clean and anonymise User data
CREATE STREAMING TABLE churn_users (
  CONSTRAINT user_valid_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (pipelines.autoOptimize.zOrderCols = "id")
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
from STREAM(live.churn_users_bronze)

-- COMMAND ----------

-- DBTITLE 1,Clean orders
CREATE STREAMING LIVE TABLE churn_orders (
  CONSTRAINT order_valid_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW, 
  CONSTRAINT order_valid_user_id EXPECT (user_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Order data cleaned and anonymized for analysis."
AS SELECT
  cast(amount as int),
  id as order_id,
  user_id,
  cast(item_count as int),
  to_timestamp(transaction_date, "MM-dd-yyyy HH:mm:ss") as creation_date

from STREAM(live.churn_orders_bronze)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### 3/ Aggregate and join data to create our ML features
-- MAGIC <div style="float:right">
-- MAGIC   <img width="500px" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/lakehouse-retail/lakehouse-retail-churn-de-small-3.png"/>
-- MAGIC </div>
-- MAGIC
-- MAGIC We're now ready to create the features required for our Churn prediction.
-- MAGIC
-- MAGIC We need to enrich our user dataset with extra information which our model will use to help predicting churn, such as:
-- MAGIC
-- MAGIC * last command date
-- MAGIC * number of items bought
-- MAGIC * number of actions in our website
-- MAGIC * device used (iOS/iPhone)
-- MAGIC * ...

-- COMMAND ----------

CREATE LIVE TABLE churn_features
COMMENT "Final user table with all information for Analysis / ML"
AS 
  WITH 
    churn_orders_stats AS (SELECT user_id, count(*) as order_count, sum(amount) as total_amount, sum(item_count) as total_item, max(creation_date) as last_transaction
      FROM live.churn_orders GROUP BY user_id)

  SELECT *, 
         datediff(now(), creation_date) as days_since_creation,
         datediff(now(), last_activity_date) as days_since_last_activity
       FROM live.churn_users
         INNER JOIN churn_orders_stats using (user_id)
