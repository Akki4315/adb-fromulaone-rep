-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Most Dominant Team throghout the seasons

-- COMMAND ----------

select team_name,
count(team_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points
from 
f1_presentation.calculated_race_result
group by team_name
having count(team_name)>100
order by avg_points desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Most Dominant Team inbetween seasons 2011 & 2020

-- COMMAND ----------

select team_name,
count(team_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points
from 
f1_presentation.calculated_race_result
where race_year between 2011 and 2020
group by team_name
having count(team_name)>50
order by avg_points desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Most Dominant Team inbetween seasons 2001 & 2010

-- COMMAND ----------

select team_name,
count(team_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points
from 
f1_presentation.calculated_race_result
where race_year between 2001 and 2010
group by team_name
having count(team_name)>50
order by avg_points desc;
