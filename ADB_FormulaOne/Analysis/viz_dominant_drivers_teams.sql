-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Most Dominant Drivers Visualization

-- COMMAND ----------

create view if not exists driver_dominant_view 
as
select driver_name,
count(driver_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points,
rank() over(order by round(avg(calculated_points),2) desc ) as Driver_Rank
from 
f1_presentation.calculated_race_result
group by driver_name
having count(driver_name)>50
order by avg_points desc;

-- COMMAND ----------

select race_year, driver_name,
count(driver_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points
from 
f1_presentation.calculated_race_result
where driver_name in (select driver_name from driver_dominant_view where Driver_Rank <=10)
group by race_year, driver_name
order by avg_points desc;

-- COMMAND ----------

select race_year, driver_name,
count(driver_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points
from 
f1_presentation.calculated_race_result
where driver_name in (select driver_name from driver_dominant_view where Driver_Rank <=10)
group by race_year, driver_name
order by avg_points desc;

-- COMMAND ----------

select race_year, driver_name,
count(driver_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points
from 
f1_presentation.calculated_race_result
where driver_name in (select driver_name from driver_dominant_view where Driver_Rank <=10)
group by race_year, driver_name
order by avg_points desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Most Dominant Team Visualization 

-- COMMAND ----------

create view if not exists team_dominant_view 
as
select team_name,
count(team_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points,
rank() over(order by round(avg(calculated_points),2) desc ) as Team_Rank
from 
f1_presentation.calculated_race_result
group by team_name
having count(team_name)>100
order by avg_points desc;

-- COMMAND ----------

select race_year, team_name,
count(team_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points
from 
f1_presentation.calculated_race_result
where team_name in (select team_name from team_dominant_view where Team_Rank <=5)
group by race_year, team_name
order by avg_points desc;

-- COMMAND ----------

select race_year, team_name,
count(team_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points
from 
f1_presentation.calculated_race_result
where team_name in (select team_name from team_dominant_view where Team_Rank <=5)
group by race_year, team_name
order by avg_points desc;

-- COMMAND ----------

select race_year, team_name,
count(team_name) as total_races,
sum(calculated_points) as total_points,
round(avg(calculated_points),2) as avg_points
from 
f1_presentation.calculated_race_result
where team_name in (select team_name from team_dominant_view where Team_Rank <=5)
group by race_year, team_name
order by avg_points desc;
