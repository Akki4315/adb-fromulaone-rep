-- Databricks notebook source
create table if not exists f1_presentation.calculated_race_result
using parquet
as
select 
races_tbl.year as race_year, 
constructors_tbl.name as team_name, 
concat(drivers_tbl.forename,' ',drivers_tbl.surname) as driver_name, 
results_tbl.position, 
results_tbl.points, 
(cast(11 as int) - results_tbl.position) as calculated_points
from 
f1_processed.results_tbl 
join f1_processed.drivers_tbl on results_tbl.driver_id = drivers_tbl.driver_id
join f1_processed.constructors_tbl on results_tbl.constructor_id = constructors_tbl.constructor_id
join f1_processed.races_tbl on results_tbl.race_id = races_tbl.race_id
where results_tbl.position <= 10;
