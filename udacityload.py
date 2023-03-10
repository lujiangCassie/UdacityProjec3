# Databricks notebook source
filepath = "/FileStore/tables/stations.csv"
df_stations = spark.read.format("csv")\
              .option("inferSchema","false")\
              .option("header","false")\
              .option("sep",",")\
              .load(filepath)\
              .withColumnRenamed("_c0","station_id")\
              .withColumnRenamed("_c1","name")\
              .withColumnRenamed("_c2","latitude")\
              .withColumnRenamed("_c3","longtitude")

# COMMAND ----------

display(df_stations)

# COMMAND ----------

df_stations.write\
.format("delta")\
.mode("overwrite")\
.save("/delta/stations")

spark.sql("CREATE TABLE IF NOT EXISTS stations USING DELTA LOCATION '/delta/stations'")

# COMMAND ----------

filepath = "/FileStore/tables/riders.csv"
df_riders = spark.read.format("csv")\
            .option("inferSchema","true")\
            .option("header","false")\
            .option("sep",",")\
            .load(filepath)\
            .withColumnRenamed("_c0","rider_id")\
            .withColumnRenamed("_c1","first_name")\
            .withColumnRenamed("_c2","last_name")\
            .withColumnRenamed("_c3","address")\
            .withColumnRenamed("_c4","birthday")\
            .withColumnRenamed("_c5","start_date")\
            .withColumnRenamed("_c6","end_date")\
            .withColumnRenamed("_c7","is_member")

# COMMAND ----------

df_riders.write\
.format("delta")\
.mode("overwrite")\
.save("/delta/riders")

spark.sql("CREATE TABLE IF NOT EXISTS riders USING DELTA LOCATION '/delta/riders'")

# COMMAND ----------

filepath="/FileStore/tables/payments.csv"
df_payments = spark.read.format("csv")\
              .option("inferSchema","true")\
              .option("header","false")\
              .option("sep",",")\
              .load(filepath)\
              .withColumnRenamed("_c0","payment_id")\
              .withColumnRenamed("_c1","date")\
              .withColumnRenamed("_c2","amount")\
              .withColumnRenamed("_c3","rider_id")

# COMMAND ----------

df_payments.write\
.format("delta")\
.mode("overwrite")\
.save("/delta/payments")

spark.sql("CREATE TABLE IF NOT EXISTS payments USING DELTA LOCATION 'delta/payments'")

# COMMAND ----------

filepath = "/FileStore/tables/trips.csv"
df_trips = spark.read.format("csv")\
           .option("inferSchema","false")\
           .option("header","false")\
           .option("sep",",")\
           .load(filepath)\
           .withColumnRenamed("_c0","trip_id")\
           .withColumnRenamed("_c1", "rideable_type")\
           .withColumnRenamed("_c2","started_at")\
           .withColumnRenamed("_c3", "ended_at")\
           .withColumnRenamed("_c4", "start_station_id")\
           .withColumnRenamed("_c5", "end_station_id")\
           .withColumnRenamed("_c6", "rider_id")

# COMMAND ----------

df_trips.write\
.format("delta")\
.mode("overwrite")\
.save("/delta/trips")

spark.sql("CREATE TABLE IF NOT EXISTS trips USING DELTA LOCATION '/delta/trips'")
