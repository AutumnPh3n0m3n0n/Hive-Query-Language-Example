-- ***************************************************************************
-- DO NOT modify the below provided framework and variable names/orders please
-- Loading Data:
-- create external table mapping for events.csv and mortality_events.csv

-- IMPORTANT NOTES:
-- You need to put events.csv and mortality.csv under hdfs directory
-- '/input/events/events.csv' and '/input/mortality/mortality.csv'
--
-- To do this, run the following commands for events.csv,
-- 2. hdfs dfs -mkdir -p /input/events
-- 3. hdfs dfs -chown -R root /input
-- 4. exit
-- 5. hdfs dfs -put /path-to-events.csv /input/events/
-- Follow the same steps 1 - 5 for mortality.csv, except that the path should be
-- '/input/mortality'
-- ***************************************************************************
-- create events table
DROP TABLE IF EXISTS events;
CREATE EXTERNAL TABLE events (
  patient_id STRING,
  event_id STRING,
  event_description STRING,
  time DATE,
  value DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/events';

-- create mortality events table
DROP TABLE IF EXISTS mortality;
CREATE EXTERNAL TABLE mortality (
  patient_id STRING,
  time DATE,
  label INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/mortality';

-- ******************************************************
-- Task 1A:
-- By manipulating the above two tables,
-- generate two views for alive and dead patients' events
-- ******************************************************
-- find events for alive patients
-- ***** your code below *****
DROP VIEW IF EXISTS alive_events;
CREATE VIEW alive_events

AS SELET * FROM events
LEFT JOIN mortality ON mortality.patient_id = events.patient_id
WHERE morality.patient_id IS NULL;

-- Task 1B:
-- find events for dead patients
-- ***** your code below *****
DROP VIEW IF EXISTS dead_events;
CREATE VIEW dead_events

AS SELECT * FROM mortality
RIGHT JOIN events on events.patient_id = mortality_id;

-- ************************************************
-- Task 2A: Event count metrics
-- Compute average, min and max of event counts
-- for alive and dead patients respectively
-- ************************************************
-- alive patients
-- ***** your code below *****
INSERT OVERWRITE LOCAL DIRECTORY 'event_count_alive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

DROP VIEW IF EXISTS event_count_alive;
CREATE VIEW event_count_alive
AS SELECT event_description, count(*) AS num_event_alive
FROM [alive_events] 
GROUP BY event_description;

-- Task 2B:
-- dead patients
-- ***** your code below *****
INSERT OVERWRITE LOCAL DIRECTORY 'event_count_dead'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

DROP VIEW IF EXISTS event_count_dead;
CREATE VIEW event_count_dead
AS SELECT event_description, count(*) AS num_event_dead
FROM [dead_events] 
RIGHT JOIN events on events.event_id = [dead_events].event_id
GROUP BY events.event_description;


-- ************************************************
-- Task 3A: Encounter count metrics
-- Compute average, min and max of encounter counts
-- for alive and dead patients respectively
-- ************************************************
-- alive
-- ***** your code below *****
INSERT OVERWRITE LOCAL DIRECTORY 'encounter_count_alive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

DROP VIEW IF EXISTS encounter_count_alive;
CREATE VIEW enounter_count_alive
AS SELECT patient_id, count(*) AS num_enounter_alive
FROM [alive_events] GROUP BY patient_id;

-- Task 3B:
-- dead
-- ***** your code below *****
INSERT OVERWRITE LOCAL DIRECTORY 'encounter_count_dead'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

DROP VIEW IF EXISTS encounter_count_dead;
CREATE VIEW event_count_dead
AS SELECT patient_id, count(*) AS num_enounter_dead
FROM [dead_events] GROUP BY patient_id;


-- ************************************************
-- Task 4: Record length metrics
-- Compute average, median, min and max of record lengths
-- for alive and dead patients respectively
-- ************************************************
-- alive
-- ***** your code below *****
INSERT OVERWRITE LOCAL DIRECTORY 'record_length_alive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

-- dead
-- ***** your code below *****
INSERT OVERWRITE LOCAL DIRECTORY 'record_length_dead'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

-- *******************************************
-- Task 5: Common diag/lab/med
-- Compute the 5 most frequently occurring diag/lab/med
-- for alive and dead patients respectively
-- *******************************************
-- alive patients
---- diag
-- ***** your code below *****
INSERT OVERWRITE LOCAL DIRECTORY 'common_diag_alive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

---- lab
-- ***** your code below *****
INSERT OVERWRITE LOCAL DIRECTORY 'common_lab_alive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

---- med
-- ***** your code below *****
INSERT OVERWRITE LOCAL DIRECTORY 'common_med_alive'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

-- dead patients
---- diag
-- ***** your code below *****
INSERT OVERWRITE LOCAL DIRECTORY 'common_diag_dead'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

---- lab
-- ***** your code below *****
INSERT OVERWRITE LOCAL DIRECTORY 'common_lab_dead'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE

---- med
-- ***** your code below *****
INSERT OVERWRITE LOCAL DIRECTORY 'common_med_dead'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
