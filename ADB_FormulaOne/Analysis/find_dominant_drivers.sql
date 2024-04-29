-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Most Dominant Driver throghout the seasons

-- COMMAND ----------

select driver_name,
count(driver_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points
from 
f1_presentation.calculated_race_result
group by driver_name
having count(driver_name)>50
order by avg_points desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Most Dominant Drivers inbetween seasons 2011 & 2020

-- COMMAND ----------

select driver_name,
count(driver_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points
from 
f1_presentation.calculated_race_result
where race_year between 2011 and 2020
group by driver_name
having count(driver_name)>50
order by avg_points desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Most Dominant Drivers inbetween seasons 2001 & 2010

-- COMMAND ----------

select driver_name,
count(driver_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points
from 
f1_presentation.calculated_race_result
where race_year between 2001 and 2010
group by driver_name
having count(driver_name)>50
order by avg_points desc;
