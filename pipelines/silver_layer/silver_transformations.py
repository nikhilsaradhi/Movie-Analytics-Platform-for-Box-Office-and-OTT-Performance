import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# -----------------------------------------
# CONNECTION
# -----------------------------------------
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse="SNOWFLAKE_LEARNING_WH",
    database="MOVIE_ANALYTICS",
    schema="SILVER"
)

cursor = conn.cursor()

print("Connected to Snowflake")

# -----------------------------------------
# 1. TMDB MOVIES CLEAN
# -----------------------------------------
print("Running TMDB transformation...")

cursor.execute("""
CREATE OR REPLACE TABLE SILVER.tmdb_movies_clean AS
WITH cleaned_data AS (

SELECT
    TRY_TO_NUMBER(id) AS movie_id,
    INITCAP(TRIM(REGEXP_REPLACE(title,'[^A-Za-z0-9 :\\-]',''))) AS title,
    LOWER(TRIM(original_language)) AS language,
    TRY_TO_DATE(NULLIF(TRIM(release_date),'')) AS release_date,
    TRY_TO_NUMBER(NULLIF(TRIM(budget),'')) AS budget,
    TRY_TO_NUMBER(NULLIF(TRIM(revenue),'')) AS revenue,
    TRY_TO_NUMBER(NULLIF(TRIM(runtime),'')) AS runtime,
    ROUND(TRY_TO_NUMBER(NULLIF(TRIM(vote_average),'')),2) AS vote_average,
    TRY_TO_NUMBER(NULLIF(TRIM(vote_count),'')) AS vote_count,
    ROUND(TRY_TO_NUMBER(NULLIF(TRIM(popularity),'')),2) AS popularity,
    UPPER(TRIM(status)) AS status

FROM RAW.raw_tmdb_movies
),

deduplicated_data AS (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY release_date DESC) AS row_num
FROM cleaned_data
)

SELECT
movie_id,
title,
language,
release_date,
COALESCE(budget,0) AS budget,
COALESCE(revenue,0) AS revenue,
COALESCE(runtime,0) AS runtime,
COALESCE(vote_average,0) AS vote_average,
COALESCE(vote_count,0) AS vote_count,
COALESCE(popularity,0) AS popularity,
status
FROM deduplicated_data
WHERE row_num = 1
AND movie_id IS NOT NULL
AND title IS NOT NULL;
""")

print("tmdb_movies_clean done")

# -----------------------------------------
# 2. OTT PLATFORMS CLEAN
# -----------------------------------------
print("Running OTT transformation...")

cursor.execute("""
CREATE OR REPLACE TABLE SILVER.ott_platforms_clean AS
WITH cleaned_data AS (
SELECT
    TRY_TO_NUMBER(id) AS movie_id,
    INITCAP(TRIM(title)) AS title,
    TRY_TO_NUMBER(year) AS release_year,
    TRIM(age) AS age_rating,
    TRY_TO_NUMBER(SPLIT_PART(rotten_tomatoes,'/',1)) AS rotten_tomatoes_score,
    COALESCE(netflix,0) AS netflix,
    COALESCE(hulu,0) AS hulu,
    COALESCE(prime_video,0) AS prime_video,
    COALESCE(disney_plus,0) AS disney_plus,
    CASE
        WHEN type = 0 THEN 'Movie'
        WHEN type = 1 THEN 'TV Show'
        ELSE 'Unknown'
    END AS content_type
FROM RAW.raw_ott_platforms
),

deduplicated AS (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY release_year DESC) AS rn
FROM cleaned_data
)

SELECT
movie_id,
title,
release_year,
age_rating,
rotten_tomatoes_score,
netflix,
hulu,
prime_video,
disney_plus,
content_type
FROM deduplicated
WHERE rn = 1
AND movie_id IS NOT NULL
AND title IS NOT NULL;
""")

print("ott_platforms_clean done")

# -----------------------------------------
# 3. BOX OFFICE CLEAN
# -----------------------------------------
print("Running Box Office transformation...")

cursor.execute("""
CREATE OR REPLACE TABLE SILVER.boxoffice_clean AS
WITH cleaned_data AS (
SELECT
TRIM(movie_id) AS movie_id,
INITCAP(TRIM(title)) AS title,
year AS release_year,
TRY_TO_DATE(release_date) AS release_date,

CASE
WHEN run_time LIKE '%hr%' THEN
(TRY_TO_NUMBER(REGEXP_SUBSTR(run_time,'\\d+')) * 60)
+ TRY_TO_NUMBER(REGEXP_SUBSTR(run_time,'\\d+',1,2))
ELSE
TRY_TO_NUMBER(REGEXP_SUBSTR(run_time,'\\d+'))
END AS runtime_minutes,

INITCAP(TRIM(distributor)) AS distributor,
INITCAP(TRIM(director)) AS director,

budget,
domestic AS domestic_revenue,
international AS international_revenue,
worldwide AS worldwide_revenue,

INITCAP(TRIM(genre_1)) AS genre_1,
INITCAP(TRIM(genre_2)) AS genre_2,
INITCAP(TRIM(genre_3)) AS genre_3

FROM RAW.raw_boxoffice
),

deduplicated AS (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY release_year DESC) AS rn
FROM cleaned_data
)

SELECT
movie_id,
title,
release_year,
release_date,
runtime_minutes,
distributor,
director,
COALESCE(budget,0) AS budget,
COALESCE(domestic_revenue,0) AS domestic_revenue,
COALESCE(international_revenue,0) AS international_revenue,
COALESCE(worldwide_revenue,0) AS worldwide_revenue,
genre_1,
genre_2,
genre_3
FROM deduplicated
WHERE rn = 1
AND movie_id IS NOT NULL
AND title IS NOT NULL;
""")

print("boxoffice_clean done")

# -----------------------------------------
# 4. IMDB TITLES CLEAN
# -----------------------------------------
print("Running IMDB Titles transformation...")

cursor.execute("""
CREATE OR REPLACE TABLE SILVER.imdb_titles_clean AS
WITH cleaned_data AS (
SELECT
TRIM(tconst) AS movie_id,
INITCAP(TRIM(primaryTitle)) AS title,
LOWER(TRIM(titleType)) AS title_type,
CASE
WHEN isAdult = 1 THEN 'Adult'
ELSE 'Non Adult'
END AS content_rating,
TRY_TO_NUMBER(startYear) AS release_year,
TRY_TO_NUMBER(endYear) AS end_year,
TRY_TO_NUMBER(runtimeMinutes) AS runtime_minutes,
INITCAP(TRIM(genres)) AS genres
FROM RAW.raw_imdb_titles
),

deduplicated AS (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY release_year DESC) AS rn
FROM cleaned_data
)

SELECT
movie_id,
title,
title_type,
content_rating,
release_year,
end_year,
runtime_minutes,
genres
FROM deduplicated
WHERE rn = 1
AND movie_id IS NOT NULL
AND title IS NOT NULL;
""")

print("imdb_titles_clean done")

# -----------------------------------------
# 5. IMDB RATINGS CLEAN
# -----------------------------------------
print("Running IMDB Ratings transformation...")

cursor.execute("""
CREATE OR REPLACE TABLE SILVER.imdb_ratings_clean AS
WITH cleaned_data AS (
SELECT
TRIM(tconst) AS movie_id,
averageRating AS imdb_rating,
numVotes AS total_votes
FROM RAW.raw_imdb_ratings
),

deduplicated AS (
SELECT *,
ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY total_votes DESC) AS rn
FROM cleaned_data
)

SELECT
movie_id,
COALESCE(imdb_rating,0) AS imdb_rating,
COALESCE(total_votes,0) AS total_votes
FROM deduplicated
WHERE rn = 1
AND movie_id IS NOT NULL;
""")

print("imdb_ratings_clean done")

# -----------------------------------------
# CLOSE CONNECTION
# -----------------------------------------
cursor.close()
conn.close()

print("✅ All Silver Transformations Completed Successfully")