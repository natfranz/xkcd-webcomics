About the project 
===


Xkcd, sometimes styled XKCD is a webcomic of romance, sarcasm, math, and language created in 2005 by American author Randall Munroe. The subject matter of the comic varies from statements on life and love to mathematical, programming, and scientific in-jokes. Some strips feature simple humor or pop-culture references. New cartoons are added three times a week, on Mondays, Wednesdays, and Fridays. Use the following link to go through the API documentation.

https://xkcd.com/json.html

Project Contents
================
This project consists of:
- `scr` folder that contains:
  - Python scripts for ingesting and processing data from Xkcd, generating dummy reviews data and views data, and loading data to a database.
  - Database scripts for creating tables, views, stored procedures, functions as well as sample queries for data quality checks.
- `dags` folder which contains the code for Airflow pipelines,
- `astro` and `Dockerfile` which contain configuration for Astro CLI.

How it works
================
You can execute the process manually by running the scripts in the `scr` folder, or run it in a local Airflow environment.

To initialize the Astro environment for the first time, run the following command:
```sh
    astro dev init
```
To start Astro:
```sh
    astro dev start
```
This will allow you to run the Airflow pipeline locally.

To pause all Docker containers running your local Airflow environment, run the command:
```sh
    astro dev stop
```

What it does
================
The ETL process fetches data from Xkcd. It checks whether any new comics have been published. If so, it processes them and writes them to a database. A stored procedure is used to combine comics data with reviews and views data. Aggregated results are written to a table, which is consumed by users via a view.

![img.png](res/img.png)

About Astro
========

This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes how to run Apache Airflow on your local machine.

### Deploy Your Project Locally


1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://docs.astronomer.io/astro/test-and-troubleshoot-locally#ports-are-not-available).

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

