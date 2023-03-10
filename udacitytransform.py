# Databricks notebook source
dim_rider = spark.sql('''
select rider_id, first_name, last_name, address, birthday, start_date, end_date, is_member from riders''')

dim_rider.dropDuplicates(["rider_id"]).write.format("delta").mode("overwrite").saveAsTable("dim_rider")

# COMMAND ----------

dim_station = spark.sql('''
select station_id, name, latitude, longtitude from stations''')

dim_station.dropDuplicates(["station_id"]).write.format("delta").mode("overwrite").saveAsTable("dim_station")

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.types import StringType

min_date_query = spark.sql('''select MIN(started_at) as started_at from trips''')
min_date = min_date_query.first().asDict()['started_at']

max_date_query = spark.sql('''select DATEADD(year, 5, MAX(started_at)) as started_at from trips''')
max_date = max_date_query.first().asDict()['started_at']

expression = f"sequence(to_date('{min_date}'), to_date('{max_date}'), interval 1 day)"

# COMMAND ----------

dim_time = spark.createDataFrame([(1,)], ["time_id"])

dim_time = dim_time.withColumn("dateinit", f.explode(f.expr(expression)))
dim_time = dim_time.withColumn("date", f.to_timestamp(dim_time.dateinit, "yyyy-MM-dd"))
dim_time = dim_time \
            .withColumn("dayofweek", f.dayofweek(dim_time.date)) \
            .withColumn("dayofmonth", f.dayofmonth(dim_time.date)) \
            .withColumn("weekofyear", f.weekofyear(dim_time.date)) \
            .withColumn("year", f.year(dim_time.date)) \
            .withColumn("quarter", f.quarter(dim_time.date)) \
            .withColumn("month", f.month(dim_time.date)) \
            .withColumn("time_id", dim_time.date.cast(StringType())) \
            .drop(f.col("dateinit"))
                  
display(dim_time)

# COMMAND ----------

dim_time.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("dim_time")

# COMMAND ----------

#from pyspark.sql.functions import col
trips = spark.sql('''select * from trips''')

fact_trip = spark.sql('''
    SELECT 
        trips.trip_id,
        riders.rider_id,
        trips.start_station_id, 
        trips.end_station_id, 
        trips.rideable_type, 
        trips.started_at, 
        trips.ended_at, 
        DATEDIFF(hour, trips.started_at, trips.ended_at) AS duration,
        DATEDIFF(year, riders.birthday, trips.started_at) AS rider_age
    FROM trips JOIN riders ON riders.rider_id = trips.rider_id
''')
display(fact_trip)

# COMMAND ----------

fact_trip.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("fact_trip")

# COMMAND ----------

fact_payment = spark.sql('''
    SELECT
        payment_id,
        payments.amount,
        payments.rider_id,
        dim_time.time_id
    FROM payments
    JOIN dim_time ON dim_time.date = payments.date
''')

display(fact_payment)

# COMMAND ----------

fact_payment.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("fact_payment")
