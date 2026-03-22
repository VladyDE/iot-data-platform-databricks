-- Fully enriched iot data
CREATE OR REFRESH STREAMING TABLE full_iot_info_gold
COMMENT "Full enriched information with Late-Arriving Dimension handling"
AS
SELECT
  t.id_sensor,
  COALESCE(m.modelo, 'Pending Registration') AS modelo,
  COALESCE(m.ubicacion, 'Unknown Location') AS ubicacion,
  COALESCE(m.rango_max, 0) AS rango_max,
  t.temperatura,
  t.humedad,
  t.timestamp
FROM STREAM(live.silver_iot_telemetry) t
LEFT JOIN live.dim_sensors m ON t.id_sensor = m.id_sensor;

-- Aggregated data by month
CREATE OR REFRESH MATERIALIZED VIEW temp_humidity_bymonth_gold
PARTITIONED BY (date)
COMMENT "Daily averages of temperature and humidity"
AS
SELECT
  ubicacion,
  ROUND(AVG(temperatura),2) AS avg_temp,
  ROUND(AVG(humedad),2) AS avg_humidity,
  TRUNC(timestamp, 'MM') AS date
  FROM live.full_iot_info_gold
GROUP BY ubicacion, date;

-- Total sensors by location
CREATE OR REFRESH MATERIALIZED VIEW total_sensors_gold
COMMENT "Counts the total number of sensors on each location"
AS
SELECT
count(DISTINCT id_sensor) as total_sensors,
ubicacion
FROM live.full_iot_info_gold
GROUP BY ubicacion
ORDER BY ubicacion;

-- Total sensors by range category
CREATE OR REFRESH MATERIALIZED VIEW total_sensors_byrange_gold
COMMENT "Counts how much sensors are on each range category"
AS
SELECT
count(DISTINCT id_sensor) as total_sensors,
(CASE WHEN rango_max <=45 THEN 'bajo' WHEN rango_max >45 and rango_max <=50 THEN 'medio' WHEN rango_max >50 THEN 'alto' END) AS tiporango
FROM live.full_iot_info_gold
GROUP BY tiporango;
