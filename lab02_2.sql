-- Databricks notebook source
DROP TABLE IF EXISTS diamonds;
CREATE TABLE diamonds USING csv
  OPTIONS (path "databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv",header "true", inferSchema "true")

-- COMMAND ----------

describe diamonds;

-- COMMAND ----------

select * from diamonds limit 5

-- COMMAND ----------

select distinct clarity from diamonds;

-- COMMAND ----------

select color, count(*) from diamonds  group by color;


-- COMMAND ----------

DROP TABLE IF EXISTS diamonds;


-- COMMAND ----------

DROP TABLE IF EXISTS iot_devices;


-- COMMAND ----------

DROP TABLE IF EXISTS departuredelays;
CREATE TABLE departuredelays USING csv
  OPTIONS (path "databricks-datasets/flights/departuredelays.csv",header "true")


-- COMMAND ----------

DROP TABLE IF EXISTS airportcodes;
CREATE TABLE airportcodes USING csv
  OPTIONS (path "databricks-datasets/flights/airport-codes-na.txt",header "true", delimiter "\t", inferSchema "true")


-- COMMAND ----------

select substring(date,0,2) as month,avg(delay) as delay
from departuredelays group by substring(date,0,2);

-- COMMAND ----------

/*count the number of flights arriving, departing from each airport each month*/
select * from
(select City, IATA, substring(date,0,2) as month, count(*) as arrivals_count
from departuredelays join airportcodes on origin=IATA group by City,IATA,substring(date,0,2))
natural join (select City,IATA,substring(date,0,2) as month,count(*) as departures_count
from departuredelays join airportcodes on destination=IATA group by City,IATA,substring(date,0,2))


-- COMMAND ----------

/*The busisest airport in the March month*/
select City,IATA, count(*) as total_flights from ((select City,IATA,date from airportcodes join departuredelays on IATA=origin)
union (select City,IATA,date from airportcodes join departuredelays on IATA=destination))
where substring(date,0,2)='03' group by City,IATA order by total_flights desc limit 1;

-- COMMAND ----------


