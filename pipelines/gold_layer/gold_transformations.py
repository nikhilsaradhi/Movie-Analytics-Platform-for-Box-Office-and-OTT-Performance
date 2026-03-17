from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sum_, avg, count, desc
import os
from dotenv import load_dotenv

load_dotenv()

connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "MOVIE_ANALYTICS",
    "schema": "SILVER"
}

session = Session.builder.configs(connection_parameters).create()

tmdb = session.table("SILVER.tmdb_movies_clean")
boxoffice = session.table("SILVER.boxoffice_clean")
ratings = session.table("SILVER.imdb_ratings_clean")
ott = session.table("SILVER.ott_platforms_clean")
titles = session.table("SILVER.imdb_titles_clean")


movie_metrics = (
    tmdb.join(ratings, tmdb["MOVIE_ID"] == ratings["MOVIE_ID"], "left")
    .join(boxoffice, tmdb["MOVIE_ID"] == boxoffice["MOVIE_ID"], "left")
    .join(ott, tmdb["MOVIE_ID"] == ott["MOVIE_ID"], "left")
    .join(titles, tmdb["MOVIE_ID"] == titles["MOVIE_ID"], "left")
    .select(
        tmdb["MOVIE_ID"],
        tmdb["TITLE"],
        tmdb["LANGUAGE"],
        tmdb["COUNTRY"],
        tmdb["RELEASE_DATE"],
        tmdb["BUDGET"],
        tmdb["REVENUE"],
        tmdb["RUNTIME"],
        tmdb["VOTE_AVERAGE"],
        tmdb["VOTE_COUNT"],
        tmdb["POPULARITY"],
        tmdb["STATUS"],
        ratings["IMDB_RATING"],
        ratings["TOTAL_VOTES"],
        boxoffice["RELEASE_YEAR"],
        boxoffice["DISTRIBUTOR"],
        boxoffice["DOMESTIC_REVENUE"],
        boxoffice["INTERNATIONAL_REVENUE"],
        boxoffice["WORLDWIDE_REVENUE"],
        ott["AGE_RATING"],
        ott["ROTTEN_TOMATOES_SCORE"],
        ott["NETFLIX"],
        ott["HULU"],
        ott["PRIME_VIDEO"],
        ott["DISNEY_PLUS"],
        ott["CONTENT_TYPE"],
        titles["TITLE_TYPE"],
        titles["RELEASE_YEAR"],
        titles["RUNTIME_MINUTES"],
        titles["GENRES"],
        
    )
)

movie_metrics.write.mode("overwrite").save_as_table("GOLD.movie_metrics")
session.table("GOLD.movie_metrics").show(10)


boxoffice_metrics = (
    boxoffice.group_by("RELEASE_YEAR")
    .agg(
        sum_("WORLDWIDE_REVENUE").alias("TOTAL_WORLDWIDE_REVENUE"),
        sum_("DOMESTIC_REVENUE").alias("TOTAL_DOMESTIC_REVENUE"),
        sum_("INTERNATIONAL_REVENUE").alias("TOTAL_INTERNATIONAL_REVENUE"),
        avg("WORLDWIDE_REVENUE").alias("AVG_WORLDWIDE_REVENUE"),
        count("MOVIE_ID").alias("TOTAL_MOVIES")
    )
    .sort(desc("TOTAL_WORLDWIDE_REVENUE"))
)

boxoffice_metrics.write.mode("overwrite").save_as_table("GOLD.boxoffice_metrics")
session.table("GOLD.boxoffice_metrics").show(10)


ott_availability = (
    ott.group_by("CONTENT_TYPE")
    .agg(
        sum_("NETFLIX").alias("NETFLIX_COUNT"),
        sum_("HULU").alias("HULU_COUNT"),
        sum_("PRIME_VIDEO").alias("PRIME_VIDEO_COUNT"),
        sum_("DISNEY_PLUS").alias("DISNEY_PLUS_COUNT"),
        avg("ROTTEN_TOMATOES_SCORE").alias("AVG_ROTTEN_TOMATOES"),
        count("MOVIE_ID").alias("TOTAL_TITLES")
    )
)

ott_availability.write.mode("overwrite").save_as_table("GOLD.ott_availability")
session.table("GOLD.ott_availability").show(10)


genre_distribution = (
    titles.group_by("GENRES")
    .agg(
        count("MOVIE_ID").alias("MOVIE_COUNT")
    )
    .sort(desc("MOVIE_COUNT"))
)

genre_distribution.write.mode("overwrite").save_as_table("GOLD.genre_distribution")

session.table("GOLD.genre_distribution").show(10)


movie_rankings = (
    tmdb.join(ratings, tmdb["MOVIE_ID"] == ratings["MOVIE_ID"], "left")
    .select(
        tmdb["MOVIE_ID"],
        tmdb["TITLE"],
        tmdb["COUNTRY"],
        tmdb["REVENUE"],
        ratings["IMDB_RATING"],
        ratings["TOTAL_VOTES"]
    )
    .sort(desc("IMDB_RATING"))
)

movie_rankings.write.mode("overwrite").save_as_table("GOLD.movie_rankings")
session.table("GOLD.movie_rankings").show(10)


studio_revenue_analysis = (
    boxoffice.group_by("DISTRIBUTOR")
    .agg(
        sum_("WORLDWIDE_REVENUE").alias("STUDIO_TOTAL_REVENUE"),
        sum_("DOMESTIC_REVENUE").alias("STUDIO_DOMESTIC_REVENUE"),
        sum_("INTERNATIONAL_REVENUE").alias("STUDIO_INTERNATIONAL_REVENUE"),
        count("MOVIE_ID").alias("TOTAL_MOVIES")
    )
    .sort(desc("STUDIO_TOTAL_REVENUE"))
)

studio_revenue_analysis.write.mode("overwrite").save_as_table("GOLD.studio_revenue_analysis")
session.table("GOLD.studio_revenue_analysis").show(10)


country_revenue_analysis = (
    tmdb.group_by("COUNTRY")
    .agg(
        sum_("REVENUE").alias("TOTAL_REVENUE"),
        avg("REVENUE").alias("AVG_REVENUE"),
        avg("VOTE_AVERAGE").alias("AVG_RATING"),
        count("MOVIE_ID").alias("TOTAL_MOVIES")
    )
    .sort(desc("TOTAL_REVENUE"))
)

country_revenue_analysis.write.mode("overwrite").save_as_table("GOLD.country_revenue_analysis")
session.table("GOLD.country_revenue_analysis").show(10)
session.close()