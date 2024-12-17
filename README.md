# CS439 Assignment 3

## Author Details
|Reg Number|2021317|
|:-----------|:-----------|
|**Name**|Muhammad Abdullah|
|**Course Code**|CS439|
|**Course Title**|Data Science|
|**Semester**|Fall 2024|
|**Instructor**|Safia Shahal|

## Abstract
This project aims to perform *exploratory data analysis (EDA)* on the *Netflix TV Shows and Movies dataset* using *Python and Apache Spark*. It uses *docker* to host an instance of *spark* and which is used to run a *pyspark-python* program to perform EDA.

RDDs were not used for this dataset since the dataset is already structured in *CSV* format which works best with *DataFrames* because they leverage *Spark's built-in optimizations* for columnar operations.

## Dependencies
- Java
- Docker
- Python
- Pyspark
- Pandas
- Matplotlib


## Running Instructions
#### 1. Clone the repository and *cd* to it
#### 2. Run docker image
```bash
// Pull the spark docker image:
sudo docker pull bitnami/spark:latest 

// Build docker image:
sudo docker build -t spark-custom .

// Start the Spark master and workers
sudo docker compose up -d

// Run docker image:
sudo docker exec -it spark-master spark-submit /opt/spark-data/netflix_eda.py

// Shutdown spark
sudo docker compose down
```

## Output
#### 1. Dataset Sample
|title|type|country|release_year|listed_in|duration|
|:-----------:|:-----------:|:-----------:|:-----------:|:-----------:|:-----------:|
|Dick Johnson Is Dead|Movie|United States|2020|Documentaries|90 min|
|Blood & Water|TV Show|South Africa|2021|International TV Shows, TV Dramas, TV Mysteries|2 Seasons|
|Ganglands|TV Show|N/A||2021|Crime TV Shows, International TV Shows, TV Action & Adventure|1 Season|
|Jailbirds New Orleans|TV Show|N/A|2021|Docuseries, Reality TV|1 Season|
|Kota Factory|TV Show|India|2021|International TV Shows, Romantic TV Shows, TV Comedies|2 Seasons|
|Midnight Mass|TV Show|N/A||2021|TV Dramas, TV Horror, TV Mysteries|1 Season|
|My Little Pony: A New Generation|Movie|N/A||2021|Children & Family Movies|91 min|
|Sankofa|Movie|United States, Ghana, Burkina Faso, United Kingdom, Germany, Ethiopia|1993|Dramas, Independent Movies, International Movies|125 min|
|The Great British Baking Show|TV Show|United Kingdom|2021|British TV Shows, Reality TV|9 Seasons|
|The Starling|Movie|United States|2021|Comedies, Dramas|104 min|

#### 2. Schema
```bash
StructType(
  [StructField('show_id', StringType(), True),
   StructField('type', StringType(), True),
   StructField('title', StringType(), True), 
   StructField('director', StringType(), True), 
   StructField('cast', StringType(), True), 
   StructField('country', StringType(), True), 
   StructField('date_added', StringType(), True), 
   StructField('release_year', StringType(), True), 
   StructField('rating', StringType(), True), 
   StructField('duration', StringType(), True), 
   StructField('listed_in', StringType(), True), 
   StructField('description', StringType(), True)]
  )
  ```
#### 3. Null Counts
|Column|Number of null values|
|:-----------:|:-----------:|
|show_id|0|
|type|   1|
|title|  2|
|director|   2636|
|cast|   826|
|country|832|
|date_added| 13|
|release_year|   2|
|rating| 6|
|duration|   5|
|listed_in|  3|
|description|3|

#### 4. Type Count
![Type Count](/results/type_count.png)
#### 5. Titles Per Year
![Titles Per Year](/results/titles_per_year.png)
#### 6. Top 10 Countries
![Top 10 Countries](/results/top_countries.png)
#### 7. Top 10 Popular Genres
![Top 10 Popular Genres](/results/popular_genres.png)
#### 8. Top 10 Directors
![Top 10 Directors](/results/top_directors.png)
#### 9. Titles Duration Analysis
![Titles Duration Analysis](/results/duration_analysis.png)
