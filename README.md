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
sudo docker build -t netflix-spark-analysis .



// Run docker image:
sudo docker run --name spark-container netflix-spark-analysis 

// Copy over analysis results to local:
sudo docker cp spark-container:/opt/spark-data/results ./results 
```

## Output
#### 1. Dataset Sample
#### 2. Schema
#### 3. Null Counts
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
