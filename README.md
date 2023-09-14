# Data Pipeline: Analyzing Daily UK Air Quality Data

## Overview

This project focuses on retrieving daily air quality data from the World Air Quality Index (WAQI). The gathered data is acquired through the WAQI API on a daily basis. Only the air quality data specific to the United Kingdom is filtered, cleaned, and transformed before being updated into a PostgreSQL database. The entire Extract, Transform, Load (ETL) process is automated and scheduled to run daily using Apache Airflow. The final output is a heatmap visualization that displays pollution hotspots in the UK for the current day.

## Project Details

### Data Collection
- Daily air quality data is collected from the World Air Quality Index (WAQI) using their API.

### Data Filtering and Cleaning
- Only the air quality data relevant to the United Kingdom is filtered from the dataset.
- Data is cleaned to ensure accuracy and consistency in subsequent analysis.

### Data Transformation
- The cleaned data is transformed into a suitable format for storage and analysis.

### Database Integration
- The transformed data is updated into a PostgreSQL database for easy access and retrieval.

### Automation with Apache Airflow
- The entire ETL process is automated and scheduled to run daily using Apache Airflow.

### Data Visualization
- The processed data is visualized as a heatmap, providing an intuitive representation of pollution hotspots in the UK for the current day.

## Technologies Used
- World Air Quality Index (WAQI) API
- Python
- PostgreSQL
- Apache Airflow

## Acknowledgments
- Thanks to the World Air Quality Index (WAQI) for providing valuable air quality data.
- The PostgreSQL community for the robust database platform.
- Apache Airflow for automation capabilities.

