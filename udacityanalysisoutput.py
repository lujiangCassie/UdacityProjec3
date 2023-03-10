# Databricks notebook source
# 1. Analyze how much time is spent per ride
# - Based on date and time factors such as day of week and time of day
time_day = spark.sql('''
    SELECT 
        day(trips.started_at) as timeofday,
        avg(unix_timestamp(trips.ended_at, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(trips.started_at, 'yyyy-MM-dd HH:mm:ss'))/60 as duration_mins
    FROM trips 
    GROUP BY timeofday
    ORDER BY timeofday
''')

display(time_day)

time_week = spark.sql('''
    SELECT 
        dayOfWeek(trips.started_at) as dayofweek,
        avg(unix_timestamp(trips.ended_at, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(trips.started_at, 'yyyy-MM-dd HH:mm:ss'))/60 as duration_mins
    FROM trips 
    GROUP BY dayofweek
    ORDER BY dayofweek
''')

display(time_week)


# COMMAND ----------

# - Based on which station is the starting and / or ending station
start_station_spent = spark.sql('''
   SELECT 
        stations.name as start_station_name,
        avg(unix_timestamp(trips.ended_at, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(trips.started_at, 'yyyy-MM-dd HH:mm:ss'))/60 as duration_mins
    FROM trips LEFT JOIN stations on trips.start_station_id = stations.station_id 
    GROUP BY start_station_name
''')

display(start_station_spent)

end_station_spent = spark.sql('''
   SELECT 
        stations.name as end_station_name,
        avg(unix_timestamp(trips.ended_at, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(trips.started_at, 'yyyy-MM-dd HH:mm:ss'))/60 as duration_mins
    FROM trips LEFT JOIN stations on trips.end_station_id = stations.station_id 
    GROUP BY end_station_name
''')

display(end_station_spent)


# COMMAND ----------

# - Based on age of the rider at time of the ride
age_spent = spark.sql('''
   SELECT 
        datediff(year, riders.birthday, riders.start_date) as age, 
        avg(unix_timestamp(trips.ended_at, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(trips.started_at, 'yyyy-MM-dd HH:mm:ss'))/60 as duration_mins  
    FROM trips JOIN riders on trips.rider_id = riders.rider_id
    GROUP BY age
    ORDER BY age
''')

display(age_spent)

# COMMAND ----------

# - Based on whether the rider is a member or a casual rider
member_spent = spark.sql('''
    SELECT 
        riders.is_member as member, 
        avg(unix_timestamp(trips.ended_at, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(trips.started_at, 'yyyy-MM-dd HH:mm:ss'))/60 as duration_mins  
    FROM trips JOIN riders on trips.rider_id = riders.rider_id
    GROUP BY member
    ORDER BY member
''')

display(member_spent)

# COMMAND ----------

# 2. Analyze how much money is spent
# - Per month, quarter, year
year_spent = spark.sql('''
    SELECT 
        sum(payments.amount) as year_spent,  
        dim_time.year    
    FROM payments JOIN dim_time on payments.date = to_date(dim_time.date, "yyyy-MM-dd")
    GROUP BY dim_time.year
''')

quarter_spent = spark.sql('''
    SELECT 
        sum(payments.amount) as quarter_spent,  
        dim_time.quarter    
    FROM payments JOIN dim_time on payments.date = to_date(dim_time.date, "yyyy-MM-dd")
    GROUP BY dim_time.quarter
''')

month_spent = spark.sql('''
    SELECT 
        sum(payments.amount) as month_spent,  
        dim_time.month    
    FROM payments JOIN dim_time on payments.date = to_date(dim_time.date, "yyyy-MM-dd")
    GROUP BY dim_time.month
''')

display(month_spent)


# COMMAND ----------

# - Per member, based on the age of the rider at account start

riderage_spent = spark.sql('''
SELECT avg(rider_spent) as money_per_age, age FROM (
    SELECT 
        sum(payments.amount) as rider_spent, 
        payments.rider_id, 
        datediff(year, riders.birthday, riders.start_date) as age   
    FROM payments JOIN riders on payments.rider_id = riders.rider_id
    GROUP BY payments.rider_id, age ) 
    GROUP BY age
    ORDER BY age
''')

display(riderage_spent)

# COMMAND ----------

# EXTRA CREDIT - Analyze how much money is spent per member
# - Based on how many rides the rider averages per month

money_ridespermonth = spark.sql('''
SELECT avg(money_amount) as amount_ridespermonth, rides_per_month FROM (
SELECT sum(payments.amount) as money_amount , payments.rider_id, t.rides_per_month FROM payments JOIN (
SELECT rider_id, round(avg(number_of_trips), 0) as rides_per_month from (
    SELECT 
        count(trips.trip_id) as number_of_trips, 
        trips.rider_id, 
        dim_time.year, dim_time.month 
        FROM trips JOIN dim_time ON year(trips.started_at) = year(dim_time.date) and month(trips.started_at) = month(dim_time.date) and day(trips.started_at) = day(dim_time.date)
        GROUP BY trips.rider_id, dim_time.year, dim_time.month
     )  GROUP BY rider_id ) t 
     ON payments.rider_id = t.rider_id GROUP BY payments.rider_id, t.rides_per_month 
     ) GROUP BY rides_per_month ORDER BY rides_per_month
''')

display(money_ridespermonth)

# COMMAND ----------

# - Based on how many minutes the rider spends on a bike per month
minutes_riderpermonth = spark.sql('''
SELECT rider_id, avg(mins_spent) as mins_riderpermonth FROM ( 
    SELECT 
        trips.rider_id, 
        sum((unix_timestamp(trips.ended_at, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(trips.started_at, 'yyyy-MM-dd HH:mm:ss'))/60) as mins_spent, 
        dim_time.year, dim_time.month 
    FROM trips 
    JOIN dim_time ON year(trips.started_at) = year(dim_time.date) and month(trips.started_at) = month(dim_time.date) and day(trips.started_at) = day(dim_time.date)
    GROUP BY rider_id, dim_time.year, dim_time.month 
    ) GROUP BY rider_id
''')

amount_minutesriderpermonth = spark.sql('''
SELECT avg(total_spent) as amount, mins_riderpermonth FROM (
SELECT 
    payments.rider_id, sum(payments.amount) as total_spent, t.mins_riderpermonth 
FROM payments
JOIN (
    SELECT rider_id, round(avg(mins_spent),0) as mins_riderpermonth FROM ( 
        SELECT 
            trips.rider_id, 
            sum((unix_timestamp(trips.ended_at, 'yyyy-MM-dd HH:mm:ss') - unix_timestamp(trips.started_at, 'yyyy-MM-dd HH:mm:ss'))/60) as mins_spent, 
            dim_time.year, dim_time.month 
        FROM trips 
        JOIN dim_time ON year(trips.started_at) = year(dim_time.date) and month(trips.started_at) = month(dim_time.date) and day(trips.started_at) = day(dim_time.date)
        GROUP BY rider_id, dim_time.year, dim_time.month 
        ) GROUP BY rider_id ) t ON payments.rider_id = t.rider_id
        GROUP BY payments.rider_id, t.mins_riderpermonth
) GROUP BY mins_riderpermonth ORDER BY mins_riderpermonth      
''')

display(amount_minutesriderpermonth)
