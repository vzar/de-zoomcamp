-- As most of the questions require SQL queries, 
-- command lines to answer questions 1 and 2 go here in sql source in comments

-- Question 1
--  $ docker build --help | grep 'Write the image ID'
--      --iidfile string          Write the image ID to the file

-- Question 2
-- $ docker run --entrypoint=/bin/bash -it python:3.9
-- root@fd835c73e18a:/# pip list
-- Package    Version
-- ---------- -------
-- pip        22.0.4
-- setuptools 58.1.0
-- wheel      0.38.4
-- WARNING: You are using pip version 22.0.4; however, version 22.3.1 is available.
-- You should consider upgrading via the '/usr/local/bin/python -m pip install --upgrade pip' command.


-- Question 3

SELECT
    count(*)
FROM
    yellow_taxi_data
WHERE
    date(lpep_pickup_datetime) = '2019-01-15'
    AND date(lpep_dropoff_datetime) = '2019-01-15';

-- "count"
-- 20530

-- Question 4

SELECT
    date(lpep_pickup_datetime),
    max(trip_distance)
FROM
    yellow_taxi_data
GROUP BY
    1
ORDER BY
    2 DESC LIMIT 1;

-- "date"	"max"
-- "2019-01-15"	117.99

-- Question 5

SELECT
    count(*) FILTER (WHERE passenger_count = 2) AS cnt_2,
    count(*) FILTER (WHERE passenger_count = 3) AS cnt_3
FROM
    yellow_taxi_data
WHERE
    date(lpep_pickup_datetime) = '2019-01-01';

-- "cnt_2"	"cnt_3"
-- 1282	254


-- Question 6
SELECT
    dz."Zone",
    max(tip_amount)
FROM
    yellow_taxi_data
    JOIN zones AS pz ON "PULocationID" = pz."LocationID"
    JOIN zones AS dz ON "DOLocationID" = dz."LocationID"
WHERE
    pz."Zone" = 'Astoria'
GROUP BY
    1
ORDER BY
    2 DESC LIMIT 1;


-- "Zone"	"max"
-- "Long Island City/Queens Plaza"	88




