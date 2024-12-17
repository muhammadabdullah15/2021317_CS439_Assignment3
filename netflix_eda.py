from pyspark.sql.functions import (
    col,
    count,
    desc,
    year,
    when,
    sum,
    split,
    explode,
    isnan,
)
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import os

# Init Spark Session
spark = (
    SparkSession.builder.appName("NetflixEDA")
    .master("spark://spark-master:7077")
    .getOrCreate()
)


output_dir = "/opt/spark-data/results"
os.makedirs(output_dir, exist_ok=True)

# Load
data_path = "/opt/spark-data/netflix_titles.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# 1. Dataset Sample
interesting_cols = ["title", "type", "country", "release_year", "listed_in", "duration"]
sample_df = df.select(interesting_cols).limit(10)
print("Sample Data with Interesting Columns:")
sample_df.show(truncate=False)
sample_df.toPandas().to_csv(f"{output_dir}/sample.csv", index=False)

# 2. Schema
print("Schema:")
df.printSchema()
schema_str = str(df.schema)
with open(f"{output_dir}/schema.txt", "w") as f:
    f.write(schema_str)

# 3. Null Counts
null_counts = df.select(
    [sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
)
print("Null Counts for Each Column:")
null_counts.show()
null_counts.toPandas().to_csv(f"{output_dir}/null_counts.csv", index=False)

# 4. Type Count
valid_types = ["Movie", "TV Show"]
type_count = (
    df.filter((col("type").isNotNull()) & (col("type").isin(valid_types)))
    .groupBy("type")
    .agg(count("*").alias("count"))
    .orderBy(desc("count"))
)
print("Count of Movies and TV Shows:")
type_count.show()
type_count.toPandas().to_csv(f"{output_dir}/type_count.csv", index=False)
type_count_pd = type_count.toPandas()
plt.figure(figsize=(6, 4))
plt.bar(type_count_pd["type"], type_count_pd["count"], color="skyblue")
plt.title("Count of Movies and TV Shows")
plt.xlabel("Type")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig(f"{output_dir}/type_count.png")
plt.close()

# 5. Titles Per Year
titles_per_year = (
    df.filter((col("release_year").isNotNull()) & (~isnan("release_year")))
    .withColumn("year", col("release_year"))
    .groupBy("year")
    .agg(count("*").alias("count"))
    .orderBy("year")
)
print("Titles Per Year:")
titles_per_year.show()
titles_per_year.toPandas().to_csv(f"{output_dir}/titles_per_year.csv", index=False)
titles_per_year_pd = titles_per_year.toPandas()
plt.figure(figsize=(10, 6))
plt.plot(
    titles_per_year_pd["year"], titles_per_year_pd["count"], marker="o", linestyle="-"
)
plt.title("Number of Titles Released Per Year")
plt.xlabel("Year")
plt.ylabel("Count")
plt.grid(True)
plt.tight_layout()
plt.savefig(f"{output_dir}/titles_per_year.png")
plt.close()

# 6. Top 10 Countries with Most Titles
country_count = (
    df.filter(col("country").isNotNull() & (col("country") != ""))
    .groupBy("country")
    .agg(count("*").alias("count"))
    .orderBy(desc("count"))
    .limit(10)
)
print("Top 10 Countries with Most Titles:")
country_count.show()
country_count.toPandas().to_csv(f"{output_dir}/top_countries.csv", index=False)
country_count_pd = country_count.toPandas()
plt.figure(figsize=(8, 5))
plt.barh(country_count_pd["country"], country_count_pd["count"], color="orange")
plt.title("Top 10 Countries with Most Titles")
plt.xlabel("Count")
plt.ylabel("Country")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig(f"{output_dir}/top_countries.png")
plt.close()

# 7. Top 10 Popular Genres
genres_df = df.withColumn("genre", explode(split(col("listed_in"), ", ")))
popular_genres = (
    genres_df.filter(col("genre").isNotNull() & (col("genre") != ""))
    .groupBy("genre")
    .agg(count("*").alias("count"))
    .orderBy(desc("count"))
    .limit(10)
)
print("Top 10 Popular Genres:")
popular_genres.show()
popular_genres.toPandas().to_csv(f"{output_dir}/popular_genres.csv", index=False)
popular_genres_pd = popular_genres.toPandas()
plt.figure(figsize=(8, 5))
plt.barh(popular_genres_pd["genre"], popular_genres_pd["count"], color="green")
plt.title("Top 10 Popular Genres")
plt.xlabel("Count")
plt.ylabel("Genre")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig(f"{output_dir}/popular_genres.png")
plt.close()

# 8. Top 10 Directors with Most Titles
top_directors = (
    df.filter(col("director").isNotNull() & (col("director") != ""))
    .groupBy("director")
    .agg(count("*").alias("count"))
    .orderBy(desc("count"))
    .limit(10)
)
print("Top 10 Directors with Most Titles:")
top_directors.show()
top_directors.toPandas().to_csv(f"{output_dir}/top_directors.csv", index=False)
top_directors_pd = top_directors.toPandas()
plt.figure(figsize=(8, 5))
plt.barh(top_directors_pd["director"], top_directors_pd["count"], color="purple")
plt.title("Top 10 Directors with Most Titles")
plt.xlabel("Count")
plt.ylabel("Director")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig(f"{output_dir}/top_directors.png")
plt.close()

# 9. Titles Duration Analysis
duration_analysis = (
    df.filter(col("duration").isNotNull() & (col("duration") != ""))
    .groupBy("duration")
    .agg(count("*").alias("count"))
    .orderBy(desc("count"))
    .limit(10)
)
print("Top 10 Most Common Durations:")
duration_analysis.show()
duration_analysis.toPandas().to_csv(f"{output_dir}/duration_analysis.csv", index=False)
duration_analysis_pd = duration_analysis.toPandas()
plt.figure(figsize=(8, 5))
plt.barh(duration_analysis_pd["duration"], duration_analysis_pd["count"], color="red")
plt.title("Top 10 Most Common Durations")
plt.xlabel("Count")
plt.ylabel("Duration")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig(f"{output_dir}/duration_analysis.png")
plt.close()

# Stop session
spark.stop()
print(f"Results saved to: {output_dir}")
