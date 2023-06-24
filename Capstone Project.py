# Databricks notebook source
# MAGIC %md
# MAGIC # Capstone Project

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Create a DF(airlines_1987_to_2008) from this path
# MAGIC
# MAGIC           %fs ls dbfs:/databricks-datasets/asa/airlines/
# MAGIC
# MAGIC           (There are csv files in airlines folder. It contains 1987.csv to  2008.csv files. 
# MAGIC
# MAGIC           Create only one DF from all the files )

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/databricks-datasets/asa/airlines/*")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2. Create a PySpark Datatypes schema for the above DF

# COMMAND ----------

schema=StructType([
    StructField("Year",IntegerType()),
    StructField("Month",IntegerType()),
    StructField("DayofMonth",IntegerType()),
    StructField("DayofWeek",IntegerType()),
    StructField("DepTime",StringType()),
    StructField("CRSDepTime",IntegerType()),
    StructField("ArrTime",StringType()),
    StructField("CRSArrTime",IntegerType()),
    StructField("UniqueCarrier",StringType()),
    StructField("FlightNum",IntegerType()),
    StructField("TailNum",StringType()),
    StructField("ActualElapsedTime",StringType()),
    StructField("CRSElapsedTime",StringType()),
    StructField("AirTime",StringType()),
    StructField("ArrDelay",StringType()),
    StructField("DepDelay",StringType()),
    StructField("Origin",StringType()),
    StructField("Dest",StringType()),
    StructField("Distance",StringType()),
    StructField("TaxiIn",StringType()),
    StructField("TaxiOut",StringType()),
    StructField("Cancelled",IntegerType()),
    StructField("CancellationCode",StringType()),
    StructField("Diverted",IntegerType()),
    StructField("CarrierDelay",StringType()),
    StructField("WeatherDelay",StringType()),
    StructField("NASDelay",StringType()),
    StructField("SecurityDelay",StringType()),
    StructField("LateAircraftDelay",StringType()),
])

# COMMAND ----------

df = spark.read.format("csv").option("header", True).schema(schema).load("dbfs:/databricks-datasets/asa/airlines/*")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.View the dataframe

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.Return count of records in dataframe

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # 5.Select the columns - Origin, Dest and Distance 

# COMMAND ----------

df.select("origin","dest","distance").display()

# COMMAND ----------

# MAGIC %md 
# MAGIC # 6.Filtering data with 'where' method, where Year = 2001

# COMMAND ----------

dataframe = df.select("*").where("year = 2001")

# COMMAND ----------

dataframe.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 7.Create a new dataframe (airlines_1987_to_2008_drop_DayofMonth) exluding dropped column (“DayofMonth”) 

# COMMAND ----------

# Drop the "DayofMonth" column to create a new DataFrame
airlines_1987_to_2008_drop_DayofMonth = dataframe.drop("DayofMonth")

# COMMAND ----------

# MAGIC %md
# MAGIC # 8.Display new DataFrame

# COMMAND ----------

airlines_1987_to_2008_drop_DayofMonth.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 9.Create column 'Weekend' and a new dataframe(AddNewColumn) and display

# COMMAND ----------

AddNewColumn = airlines_1987_to_2008_drop_DayofMonth.withColumn(
    "Weekend",
    when((col("DayOfWeek") == 6) | (col("DayOfWeek") == 7), "Yes").otherwise("No")
)

# COMMAND ----------

AddNewColumn.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 10.Cast ActualElapsedTime column to integer and use printschema to verify

# COMMAND ----------

# Before change data type
AddNewColumn.printSchema()

# COMMAND ----------

datatypechange = AddNewColumn.withColumn("ActualElapsedTime",AddNewColumn["ActualElapsedTime"].cast('Integer'))

datatypechange.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC # 11.Rename 'DepTime' to 'DepartureTime'

# COMMAND ----------

datatypechange.display() #Before name changed

# COMMAND ----------

Renamedf = datatypechange.withColumnRenamed("DepTime","DeparatureTime") 

# COMMAND ----------

Renamedf.display() # After name changed

# COMMAND ----------

# MAGIC %md
# MAGIC # 12.Drop duplicate rows based on Year and Month and Create new df (Drop Rows)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import *

# COMMAND ----------

DropRows = Renamedf.dropDuplicates(["Year", "Month"]) # Drop duplicate rows based on Year and Month columns

# COMMAND ----------

DropRows.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 13.Display Sort by descending order for Year Column using sort()

# COMMAND ----------

Sortdf = DropRows.sort(desc("Year"))

# COMMAND ----------

Sortdf.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 14.Group data according to Origin and returning count

# COMMAND ----------

Groupdf = Sortdf.groupBy("origin").count()

# COMMAND ----------

Groupdf.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 15.Group data according to dest and finding maximum value for each 'Dest'

# COMMAND ----------

Maxdf = Sortdf.groupBy("Dest").agg({"Distance": "max"})

# COMMAND ----------

Maxdf.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 16.Write data in Delta format

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/odinschool/output/

# COMMAND ----------

Sortdf.write.mode("overwrite").parquet("dbfs:/FileStore/odinschool/output/")
