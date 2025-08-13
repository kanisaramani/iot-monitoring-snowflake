USE DATABASE IOT_MONITORING;
USE SCHEMA RAW;         -- raw source lives here

--Curated schema + table--
CREATE OR REPLACE SCHEMA CURATED;

CREATE OR REPLACE TABLE CURATED.SENSOR_DATA_CURATED (
  device_id    STRING,
  event_time   TIMESTAMP_NTZ,
  temperature  FLOAT,
  vibration    FLOAT,
  power_usage  FLOAT,
  status       STRING  -- simple alert flag
);

--Stream on the raw table--

CREATE OR REPLACE STREAM RAW.SENSOR_DATA_STREAM
ON TABLE RAW.SENSOR_DATA_RAW;

-- Backfill the current raw data once--
INSERT INTO CURATED.SENSOR_DATA_CURATED
SELECT
  device_id,
  event_time,
  temperature,
  vibration,
  power_usage,
  CASE WHEN temperature > 78 THEN 'ALERT' ELSE 'OK' END AS status
FROM RAW.SENSOR_DATA_RAW;

--Task to process new rows from the stream--

CREATE OR REPLACE TASK CURATED.PROCESS_SENSOR_DATA_TASK
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
AS
INSERT INTO CURATED.SENSOR_DATA_CURATED
SELECT
  device_id,
  event_time,
  temperature,
  vibration,
  power_usage,
  CASE WHEN temperature > 78 THEN 'ALERT' ELSE 'OK' END AS status
FROM RAW.SENSOR_DATA_STREAM;

ALTER TASK CURATED.PROCESS_SENSOR_DATA_TASK RESUME;

--Test it works--

SELECT SYSTEM$TASK_FORCE_RUN('CURATED.PROCESS_SENSOR_DATA_TASK');

EXECUTE TASK CURATED.PROCESS_SENSOR_DATA_TASK;

SELECT * 
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
  TASK_NAME => 'CURATED.PROCESS_SENSOR_DATA_TASK',
  RESULT_LIMIT => 5
));

SELECT COUNT(*) AS curated_rows FROM CURATED.SENSOR_DATA_CURATED;
SELECT * FROM CURATED.SENSOR_DATA_CURATED ORDER BY event_time DESC LIMIT 20;
