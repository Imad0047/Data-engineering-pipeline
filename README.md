# Data-engineering-pipeline
An end-to-end data engineering pipeline with Kafka, Spark, Airflow, Cassandra and Docker.

# Introduction

This project serves as a comprehensive guide to building an end-to-end data engineering pipeline. It covers each stage from data ingestion to processing and finally to storage, utilizing a robust tech stack that includes Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra. Everything is containerized using Docker for ease of deployment and scalability.

# System Architecture
![System Architecture](./images/architecture.png)

⚙️ Component Description
    1. BikePoint API 🛰️
      External data source providing information about bike stations.
      Delivers raw data through HTTP API requests.
    2. Apache Airflow 🪶
      Orchestrates the end-to-end data pipeline.
      Schedules and executes ETL (Extract, Transform, Load) tasks.
      Streams collected data to Kafka for real‑time processing.
    3. PostgreSQL 🐘
      Relational database used to store metadata, logs, and intermediate results.
    4. Kafka 🔄
      Receives real‑time data flow from Airflow.
      Publishes messages to downstream consumers such as Spark
     5. Apache Spark ⚡
       Distributed data‑processing engine for handling streaming data.
       Performs aggregations, transformations, and enrichments before saving the processed data.
     6. Cassandra 👁️
       NoSQL, column‑oriented database.
       Stores the final processed data for scalable and high‑speed access.
     7. Control Center 🧩
       Helps in monitoring our Kafka streams.

# Technologies

  *Apache Airflow
  *Python
  *Apache Kafka
  *Apache Zookeeper
  *Apache Spark
  *Cassandra
  *PostgreSQL
  *Docker

# Getting Started

     
      
     
