# UK_airquality_analysis
The project focuses on getting daily air quality data from World Air Quality index(WAQI).
The data is gathered using WAQI API on a daily basis.Only the UK data is filtered then cleaned and Transformed. Whcih is then Updated into a Postgres SQL database.
The Whole ETL process is automated and scheduled on a daily basis using apache Airflow.
The data is then Visualised as a heatmap Showing the Pollution hotspots of UK on the current day.
