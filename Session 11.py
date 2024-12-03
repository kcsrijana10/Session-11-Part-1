# Databricks notebook source
# MAGIC %md 
# MAGIC Continue with Dataframe Transformation
# MAGIC Some important transformations related to perspective (Split, )
# MAGIC Combining tables (Union or join)
# MAGIC Leetcode problem

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql functions import *
from pyspark.sql.types import *
from pyspark.sql.window import window
from pyspark.sql functions import row_number


# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/srijanakckhatri35@gmail.com/BigMart_Sales-1.csv")
df1.display()

# COMMAND ----------

# MAGIC  %md list into string and string to list 

# COMMAND ----------

str = "I love Coding"
str_new = str.split(" ")
print(str_new)
print(str_new[2])

str_back_to_string = " ". join(str_new)
print(str_back_to_string)



# COMMAND ----------

# MAGIC %md Drop 

# COMMAND ----------

df1 = df1.drop("outlet_location_type").display()


# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/srijanakckhatri35@gmail.com/BigMart_Sales-1.csv")
df1.display()

# COMMAND ----------

df1 = df1.drop("outlet_location_type","Outlet_Establishment_year").display()

# COMMAND ----------

# MAGIC %md DropDuplicates 

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/srijanakckhatri35@gmail.com/BigMart_Sales-1.csv")
df1.display()

# COMMAND ----------

df1.dropDuplicates().display()

# COMMAND ----------

df1.dropDuplicates(subset=["Item_Type"]).display()

# COMMAND ----------

df1.distinct().display()

# COMMAND ----------

# MAGIC %md Union and UnionByName 

# COMMAND ----------

data1  = [(1,"Srijana"),
        (2,"Satish"),
        (3,"Guru"),
        (4,"Raju")]
        
schema = "id int, name string"

data2 = [(5,"Sunil"),
         (6, "Anusha"),
         (7,"Meguer"),
         (8,"Jasmine")]

# COMMAND ----------

df1_data1 = spark.createDataFrame(data1,schema)
df1_data2 = spark.createDataFrame(data2,schema)


# COMMAND ----------

df1_data1.display()

# COMMAND ----------

df1_data2.display()

# COMMAND ----------

df1_union = df1_data1.union(df1_data2)
df1_union.display()

# COMMAND ----------



# COMMAND ----------

data3 = [(5,"Sunil",500),
         (6, "Anusha",400),
         (7,"Meguer",600),
         (8,"Jasmine",600)]

         schema_new="id int, name string, salary int"
         

# COMMAND ----------



# COMMAND ----------

df1_union_new = df1_data1.union(df1_data3)
df_union_new.display()

# COMMAND ----------

data4 = [("Sunil",5),
         ( "Anusha",6),
         ("Meguer",7),
         ("Jasmine",8)]

schema_1 = "name string,id string"


data5  = [(1,"Srijana"),
        (2,"Satish"),
        (3,"Guru"),
        (4,"Raju")]
        

schema_2 = "id string,name string"

# COMMAND ----------

df1_1 = spark.createDataFrame(data4,schema_1)
df1_2 = spark.createDataFrame(data5,schema_2)

df1_1.display()


# COMMAND ----------

df1_2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC UnionByName

# COMMAND ----------

df1_my_union = df1_1.union(df1_2)
df1_my_union.display()
 
df1_my_union_name = df1_1.unionByName(df1_2)
df1_my_union_name.display()
 

# COMMAND ----------

# MAGIC %md 
# MAGIC distinct and drop dupliccates does the same thing

# COMMAND ----------

#df1_data3.dropDuplicates.display()
#df1_data3.distict().display()

# COMMAND ----------

# MAGIC %md
# MAGIC Null Values 

# COMMAND ----------

df1_fill = spark.createDataFrame([
    (10, 80.5, "Alice", None),
    (5, None, "Bob", None),
    (None, None, "Tom", None),
    (None, None, None, True)],
    schema=["age", "height", "name", "bool"])

# COMMAND ----------

df1_fill.display()

# COMMAND ----------


df1_fill.na.fill({'age': 100, 'name': 'unknown', 'height' : 8, "bool": False}).show()


# COMMAND ----------

# MAGIC %md initcap

# COMMAND ----------

from pyspark.sql.functions import *
df1.select(initcap("Item_Type")).display()



# COMMAND ----------

from pyspark.sql.functions import *
df1.select(lower("Item_Type")).display()

# COMMAND ----------

from pyspark.sql.functions import *
df1.select(upper("Item_Type")).display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC

# COMMAND ----------

df1.withColumn('curr_date', current_date())\
    .withColumn("weak_add",date_add(current_date(),6)).display()


# COMMAND ----------

df1.withColumn('curr_date', current_date())\
    .withColumn("weak_before",date_add(current_date(),-6)).display()


# COMMAND ----------

df1.withColumn('curr_date', current_date())\
    .withColumn("weak_before",date_sub(current_date(),-6)).display()


# COMMAND ----------

df1.withColumn('curr_date', current_date())\
    .withColumn("Month_after",date_add(current_date(),30)).display()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select date(current_timestamp())
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select month(current_timestamp())

# COMMAND ----------

df1 = df1.withColumn("week_add",date_add(current_date(),7)).withColumn("curr_date",current_date())
df1.display()
 

# COMMAND ----------

df1.withColumn("date_diff",datediff("week_add","curr_date")).display()

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.dropna().display()

# COMMAND ----------

df1.dropna("any").display()

# COMMAND ----------

df1.dropna(subset =["item_weight"]).display()

# COMMAND ----------

df1.na.fill("NotAvailable").display()

# COMMAND ----------

df1.na.fill({"Item_weight" : 100}).display()

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.na.fill(100).display()

# COMMAND ----------

# MAGIC %md Explode is important

# COMMAND ----------

df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
