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
#### 1. In what year did the humanity launch the most rockets into the space?
```
SELECT 
  EXTRACT(YEAR FROM launch_time) as year, 
  count(*) total_launches
FROM launch
WHERE status_id = 3
GROUP BY 1
ORDER BY 2 DESC;
```
![Query1](https://images2.imgbox.com/73/1f/v1BejpcY_o.png)

As we can see, 2022 and 2021 are leading in this chart.

#### 2. What countries have the highest number of launches per year?
```
SELECT 
    year, 
    country_code,
    country_launches
FROM (
    SELECT 
        EXTRACT(YEAR FROM launch.launch_time) AS year,
        location.country_code AS country_code,
        COUNT(*) AS country_launches,
        RANK() OVER (PARTITION BY EXTRACT(YEAR FROM launch.launch_time) ORDER BY COUNT(*) DESC) AS rank
    FROM launch
    INNER JOIN location ON launch.location_id = location.location_id
    WHERE  launch.status_id IN (3,4,7)
    GROUP BY 1, 2
) AS t
WHERE rank = 1
ORDER BY 1;
```
![Query2](https://images2.imgbox.com/96/b2/UKnBB5WN_o.png)

The USA and China take the lead from each other almost every year.

#### 3. Is percentage of failures going down or up?
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
![Query3](https://images2.imgbox.com/cd/7f/82UE2imO_o.png)

In this case it's not that easy to give a certain answer, so we'll leave it to analysts :)
#### 4. What rocket family has the biggest amount of launches?
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
ORDER BY 2 DESC;
```
![Query4](https://images2.imgbox.com/a3/fd/suUwcwzW_o.png)

For now, Top 3 is occupied by Soviet rocket families.
### 5. What are the most popular spaceports?
```
SELECT 
  lo.location_id, 
  lo.location_name, 
  count(*) total_launches
FROM location lo
INNER JOIN launch la
ON lo.location_id = la.location_id
GROUP BY 1, 2
ORDER BY 3 DESC;
```
![Query5](https://images2.imgbox.com/d1/47/MLjamWbV_o.png)

Here we can see top 5 spaceports used for more than 75% of all launches in human history!

### Conclusion
Using Python, Pandas, SQL, and psycopg2, I've created a small ETL pipeline to get _some_ data from API about rocket launches. The result may possess interest for analysts or just people interested in the area.  Theoretically, it is possible to broaden the project as only ~30% of the initial data was used. 
