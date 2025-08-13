---Create the Database & Schema---

CREATE OR REPLACE DATABASE iot_monitoring;IOT_MONITORING.CURATED.SENSOR_DATA_CURATEDIOT_MONITORING.CURATED.SENSOR_DATA_CURATED
USE DATABASE iot_monitoring;

CREATE OR REPLACE SCHEMA raw;
USE SCHEMA raw;

---Create the Raw Table for Sensor Data---
CREATE OR REPLACE DATABASE iot_monitoring;
USE DATABASE iot_monitoring;

CREATE OR REPLACE SCHEMA raw;
USE SCHEMA raw;

---Create a File Format (for JSON)---
CREATE OR REPLACE FILE FORMAT iot_json_format
  TYPE = 'JSON';

---Create a Stage to Upload Files---
IOT_MONITORING.RAW.MANUAL_IOT_STAGE

SELECT * FROM raw.sensor_data_raw ORDER BY event_time;
