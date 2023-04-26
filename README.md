## <div align="center"> Data engineering project #1 | ETL using Python and SQL </div>
(pipeline pic)
### About the project
I’ve created a small script which uses [The Space Devs API](https://thespacedevs.com/llapi) to retrieve data about rocket launches. Getting the data from the API, it’s possible to make some analysis, look at statistics, and answer questions such as:
1. In what year did the humanity launch the most rockets into the space?
2. What countries have the highest number of launches per year?
3. Is percentage of failures going down or up?
4. What rocket family has the biggest amount of launches?
5. What are the most popular spaceports?

### Stack:
1. Python
2. SQL (PostgreSQL)
3. Pandas
4. Psycopg2
5. API

### Database schema
![Database schema](https://images2.imgbox.com/66/3e/GKrEM9SJ_o.png)
Considering specifics of the data, Star Schema is the perfect fit for the DB.

### ETL Pipeline
#### Extraction:
1. The data is received from API using **requests** library;
2. The data is saved on a local storage as a backup.


#### Transformation:
1. SQL tables are created using **psycopg2** (with Primary and Foreign keys);
2. A dataframe for each table is created using **pandas**;
3. The data in dataframes is transformed (renaming columns, dealing with NaN values or empty strings (that also occur in the dataset) so postgres has no problems reading them as NULLs. Moreover, a new column last_update_db is added);

#### Loading:
1. Data is loaded to SQL with psycopg2 as a connector;
2. Indexes for each table and two views are created.

### Examples of queries:
Now it’s time to answer the questions in the _about_ section.
1. In what year did the humanity launch the most rockets into the space?
```
SELECT 
  EXTRACT(YEAR FROM launch_time), 
  count(*) total_launches
FROM launch
WHERE status_id = 3
GROUP BY 1
ORDER BY 2 DESC
```
2. What countries have the highest number of launches per year?
```
SELECT 
  EXTRACT(YEAR from la.launch_time) AS year, 
  count(*) AS total_launches, 
  lo.country_code AS leading_country
FROM launch la
INNER JOIN location lo ON lo.location_id = la.location_id
INNER JOIN (
  SELECT 
    EXTRACT(YEAR from launch_time) AS year, 
    location_id, 
    ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR from launch_time) ORDER BY count(*) DESC) AS row_num
  FROM launch
  WHERE status_id IN (3, 4, 7)
  GROUP BY 1, 2
) loc_count ON loc_count.year = EXTRACT(YEAR from la.launch_time) 
AND loc_count.location_id = la.location_id AND loc_count.row_num = 1
WHERE status_id IN (3, 4, 7)
GROUP BY 1, 3
ORDER BY 1 ASC
```
3. Is percentage of failures going down or up?
```
SELECT 
  EXTRACT(YEAR FROM launch_time) AS year,
  COUNT(CASE WHEN la.status_id = 3 THEN 1 ELSE NULL END) * 100 / COUNT(la.*) AS success_percentage
FROM launch la
INNER JOIN status s 
ON la.status_id = s.status_id
GROUP BY 1
ORDER BY 1 ASC;
```
4. What rocket family has the biggest amount of launches?
```
SELECT 
  r.rocket_family,
  count(la.*) FILTER (WHERE la.status_id IN (3, 4, 7)) AS total_launches,
  count(la.*) FILTER (WHERE la.status_id = 3) AS successful_launches,
  count(la.*) FILTER (WHERE la.status_id IN (4, 7)) AS failed_launches,
  count(la.*) FILTER (WHERE la.status_id = 3) * 100 / NULLIF(count(la.*) FILTER 
        (WHERE la.status_id IN (3, 4, 7)), 0) AS success_rate
FROM rocket r
JOIN launch la ON r.rocket_id = la.rocket_id
GROUP BY 1
ORDER BY 2 DESC
```
5. What are the most popular spaceports?
```
SELECT 
  lo.location_id, 
  lo.location_name, 
  count(*) total_launches
FROM location lo
INNER JOIN launch la
ON lo.location_id = la.location_id
GROUP BY 1, 2
ORDER BY 3 DESC
```
