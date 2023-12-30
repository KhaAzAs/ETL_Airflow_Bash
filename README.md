# Build an ETL Pipeline using Airflow with Bash and Python

## Scenario
As a data engineer at a data analytics consulting company. I have been assigned to a project that aims to de-congest the national highways by analyzing the road traffic data from different toll plazas. Each highway is operated by a different toll operator with a different IT setup that uses different file formats. Your job is to collect data available in different formats and consolidate it into a single file.

## Objectives
In this assignment I will author an Apache Airflow DAG that will:
* Extract data from a csv file
* Extract data from a tsv file
* Extract data from a fixed width file
* Transform the data
* Load the transformed data into the staging area
