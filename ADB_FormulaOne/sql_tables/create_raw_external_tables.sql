-- Databricks notebook source
create database if not exists f1_raw;
describe database extended f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Circuit Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("/mnt/formulaoneadb/raw/circuits.csv", header = True)
-- MAGIC
-- MAGIC df.write.mode('overwrite').format('parquet').saveAsTable('circuit')

-- COMMAND ----------

drop table if exists f1_raw.ciruit_tbl;
create table if not exists f1_raw.ciruit_tbl (
  circuitId string,
  circuitRef String,
  name String,
  location String,
  country String,
  lat String,
  lng String,
  alt String,
  url String
)
using csv
options (path '/mnt/formulaoneadb/ext_tbl/circuit', header true);

insert into f1_raw.ciruit_tbl
select * from circuit;

-- COMMAND ----------

drop table if exists f1_raw.ciruit_tbl;

-- COMMAND ----------

create table if not exists f1_raw.ciruit_tbl (
  circuitId string,
  circuitRef String,
  name String,
  location String,
  country String,
  lat String,
  lng String,
  alt String,
  url String
)
using csv
options (path '/mnt/formulaoneadb/ext_tbl/circuit', header true);

-- COMMAND ----------

drop table circuit;

-- COMMAND ----------

select * from f1_raw.ciruit_tbl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Races Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("/mnt/formulaoneadb/raw/races.csv", header = True)
-- MAGIC
-- MAGIC df.write.mode('overwrite').format('parquet').saveAsTable('race')

-- COMMAND ----------

drop table if exists f1_raw.race_tbl;
create table if not exists f1_raw.race_tbl (
  raceId string,
  year string,
  round string,
  circuitId string,
  name string,
  string string,
  time string,
  url string
) using csv options (path '/mnt/formulaoneadb/ext_tbl/race', header true);

insert into
  f1_raw.race_tbl
select
  *
from
  race;

-- COMMAND ----------

drop table if exists race

-- COMMAND ----------

select * from f1_raw.race_tbl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Constructor Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.json("/mnt/formulaoneadb/raw/constructors.json")
-- MAGIC
-- MAGIC df.write.mode("overwrite").format("parquet").saveAsTable("constructor")

-- COMMAND ----------

drop table if exists f1_raw.constructor_tbl;
create table if not exists f1_raw.constructor_tbl (
  constructorId long,
  constructorRef string,
  name string,
  nationality string,
  url string
) using json options (path "/mnt/formulaoneadb/ext_tbl/constructor");
insert into
  f1_raw.constructor_tbl
select
  *
from
  default.constructor;

-- COMMAND ----------

drop table if exists default.constructor;

-- COMMAND ----------

select * from f1_raw.constructor_tbl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Driver Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.json("/mnt/formulaoneadb/raw/drivers.json")
-- MAGIC df.write.mode('overwrite').format('parquet').saveAsTable('drivers')

-- COMMAND ----------

drop table if exists f1_raw.driver_tbl;
create table if not exists f1_raw.driver_tbl (
  driverId string,
  driverRef string,
  number string,
  code string,
  name Struct<forename: string, surname: string>,
  dob string,
  nationality string,
  url string
) using json options (path "/mnt/formulaoneadb/ext_tbl/driver");
insert into
  f1_raw.driver_tbl
select
  *
from
  default.drivers;

-- COMMAND ----------

drop table if exists default.drivers;

-- COMMAND ----------

select * from f1_raw.driver_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Result Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.json("/mnt/formulaoneadb/raw/results.json")
-- MAGIC df.write.mode('overwrite').format('parquet').saveAsTable('results')

-- COMMAND ----------

drop table if exists f1_raw.result_tbl;
create table if not exists f1_raw.result_tbl (
  resultId long,
  raceId long,
  driverId string,
  constructorId string,
  number string,
  grid long,
  position string,
  positionText string,
  positionOrder string,
  points double,
  laps string,
  time string,
  milliseconds string,
  fastestLap string,
  rank string,
  fastestLapTime string,
  fastestLapSpeed string,
  statusId string
) using json options (path "/mnt/formulaoneadb/ext_tbl/result");
insert into
  f1_raw.result_tbl
select
  *
from
  default.results;

-- COMMAND ----------

drop table if exists default.results;

-- COMMAND ----------

select * from f1_raw.result_tbl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Pitstop Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.json("/mnt/formulaoneadb/raw/pit_stops.json", multiLine=True)
-- MAGIC
-- MAGIC df.write.mode('overwrite').format('parquet').saveAsTable('pitstops')

-- COMMAND ----------

drop table if exists f1_raw.pitstop_tbl;
create table if not exists f1_raw.pitstop_tbl (
  raceId long,
  driverId string,
  stop long,
  lap long,
  time string,
  duration string,
  milliseconds string
) using json options (path "/mnt/formulaoneadb/ext_tbl/pitstop");
insert into
  f1_raw.pitstop_tbl
select
  *
from
  default.pitstops;

-- COMMAND ----------

drop table if exists default.pitstops;

-- COMMAND ----------

select * from f1_raw.pitstop_tbl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Laptime Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import * 
-- MAGIC input_schema = StructType(
-- MAGIC     [
-- MAGIC         StructField("raceId", IntegerType()),
-- MAGIC         StructField("driverId", IntegerType()),
-- MAGIC         StructField("lap", IntegerType()),
-- MAGIC         StructField("position", IntegerType()),
-- MAGIC         StructField("time", StringType()),
-- MAGIC         StructField("milliseconds", IntegerType()),
-- MAGIC     ]
-- MAGIC )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("/mnt/formulaoneadb/raw/lap_times", schema = input_schema)
-- MAGIC df.write.mode('overwrite').format('parquet').saveAsTable('laptimes')

-- COMMAND ----------

drop table if exists f1_raw.laptime_tbl;
create table if not exists f1_raw.laptime_tbl (
raceId integer,
driverId integer,
lap integer,
position integer,
time string,
milliseconds integer
) using csv options (path "/mnt/formulaoneadb/ext_tbl/laptime");
insert into
 f1_raw.laptime_tbl
select
  *
from
  default.laptimes;

-- COMMAND ----------

drop table if exists default.laptimes;

-- COMMAND ----------

select * from f1_raw.laptime_tbl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Create Qualifying Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.json("/mnt/formulaoneadb/raw/qualifying/", multiLine=True)
-- MAGIC df.write.mode('overwrite').format("parquet").saveAsTable("qualifying")

-- COMMAND ----------

drop table if exists f1_raw.qualifying_tbl;
create table if not exists f1_raw.qualifying_tbl (
constructorId long,
driverId long,
number long,
position long,
q1 string,
q2 string,
q3 string,
qualifyId long,
raceId long
) using json options (path "/mnt/formulaoneadb/ext_tbl/qualifying");
insert into
f1_raw.qualifying_tbl
select
  *
from
  default.qualifying;

-- COMMAND ----------

drop table if exists default.qualifying;

-- COMMAND ----------

select * from f1_raw.qualifying_tbl;
