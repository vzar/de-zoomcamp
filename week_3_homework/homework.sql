-- Setup
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp.ext_fhv_data` OPTIONS (
    format = 'CSV',
    uris =['gs://prefect-vzar-de-zoom/data/fhv_tripdata_2019-*.csv.gz']
);

CREATE OR REPLACE TABLE `dezoomcamp.fhv_data` AS
SELECT
    *
FROM
    `dezoomcamp.ext_fhv_data`;

-- Question 1
SELECT
    COUNT(pickup_datetime)
FROM
    `dezoomcamp.fhv_data`;

-- 43244696

-- Question 2
-- 0 MB FOR the External TABLE AND 317.94MB FOR the BQ TABLE

-- Question 3
SELECT
    count(*)
FROM
    `dezoomcamp.fhv_data`
WHERE
    PUlocationID IS NULL
    AND DOlocationID IS NULL
    
-- 717,748

-- Question 4
--        PARTITION BY pickup_datetime CLUSTER ON affiliated_base_number

-- Question 5
CREATE OR REPLACE TABLE dezoomcamp.fhv_data_partitoned_clustered 
        PARTITION BY DATE (pickup_datetime) 
        CLUSTER BY affiliated_base_number 
AS 
SELECT 
    * 
FROM 
    `dezoomcamp.ext_fhv_data`;

SELECT DISTINCT
    affiliated_base_number
FROM
    `dezoomcamp.fhv_data`
WHERE
    date(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

SELECT DISTINCT
    affiliated_base_number
FROM
    `dezoomcamp.fhv_data_partitoned_clustered`
WHERE
    date(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31';

-- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

-- Question 6
-- GCP Bucket

-- Question 7
-- False

-- Question 8
-- when converting csv.gz to parquet with pandas, used explicit Int64 type for SR_Flag, PUlocationId
-- and DOlocationId to avoid conversion to float in some files and typing problems with external table in BQ
-- see etl_fhv_to_gcs_parquet.py for details
-- then create external table from the set of parquet files
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp.extpq_fhv_data` OPTIONS (
    format = 'Parquet',
    uris =['gs://prefect-vzar-de-zoom/data/fhv_tripdata_2019-*.parquet']
);

-- BQ table from external with the same DDL command as in setup
CREATE OR REPLACE TABLE `dezoomcamp.pq_fhv_data` AS
SELECT
    *
FROM
    `dezoomcamp.extpq_fhv_data`;

