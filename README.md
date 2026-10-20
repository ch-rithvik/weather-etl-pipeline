# Weather ETL Pipeline using Apache Airflow and PostgreSQL

## Overview

This project implements an end-to-end **Extract, Transform, Load (ETL) pipeline** using Python, Apache Airflow, PostgreSQL, Docker, and Astronomer CLI.

The pipeline retrieves current weather observations for **London, United Kingdom**, from the Open-Meteo REST API, transforms the JSON response into structured records, and loads the data into a PostgreSQL database.

Apache Airflow orchestrates the workflow through a DAG with three dependent tasks, scheduled to run daily.

## Tech Stack

- **Python:** ETL logic and data transformation
- **Apache Airflow:** Workflow orchestration and scheduling
- **PostgreSQL:** Relational data storage
- **SQL:** Table creation and data insertion
- **Open-Meteo REST API:** Weather data source
- **Docker:** Containerized local environment
- **Astronomer CLI (Astro CLI):** Local Airflow development and execution
- **Airflow HttpHook:** API communication
- **Airflow PostgresHook:** PostgreSQL connectivity
- **Airflow TaskFlow API:** Python-based task definitions and dependencies

## ETL Pipeline Architecture

```text
       Open-Meteo REST API
               |
               v
       Extract Weather Data
           (HttpHook)
               |
               v
       Transform Weather Data
             (Python)
               |
               v
        Load Weather Data
          (PostgresHook)
               |
               v
         PostgreSQL Database
          (weather_data)
```

**Airflow DAG:** `weather_etl_pipeline`

**Schedule:** `@daily`

**Start Date:** October 1, 2026 (UTC)

**Catchup:** Disabled

## Project Structure

```text
weather-etl-pipeline/
|
|-- dags/
|   |-- weather_etl_pipeline.py
|
|-- Dockerfile
|-- requirements.txt
|-- packages.txt
|-- airflow_settings.yaml
|-- include/
|-- plugins/
|-- README.md
```

The Python DAG filename shown above is illustrative; use the actual filename in your `dags/` directory.

## ETL Workflow

### 1. Extract Weather Data

The `extract_weather_data()` task retrieves weather information from the Open-Meteo REST API using Apache Airflow's `HttpHook`.

**API endpoint:**

`https://api.open-meteo.com/v1/forecast`

**Query parameters:**

- Latitude: `51.5074`
- Longitude: `-0.1278`
- Current weather: `true`

The API returns weather observations in JSON format. The response is validated for HTTP errors before being passed to the transformation task.

### 2. Transform Weather Data

The `transform_weather_data()` task extracts the required attributes from the API response and converts them into a structured Python dictionary.

The transformed record contains:

| Field | Description |
|---|---|
| latitude | Geographic latitude |
| longitude | Geographic longitude |
| temperature | Current temperature |
| windspeed | Current wind speed |
| winddirection | Wind direction |
| weathercode | Weather condition code |

This prepares the data for relational database storage.

### 3. Load Weather Data

The `load_weather_data()` task uses Airflow's `PostgresHook` to connect to PostgreSQL.

The task first creates a table if it does not already exist:

```sql
CREATE TABLE IF NOT EXISTS weather_data (
    latitude FLOAT,
    longitude FLOAT,
    temperature FLOAT,
    windspeed FLOAT,
    winddirection FLOAT,
    weathercode INT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

It then inserts the transformed weather observations using parameterized SQL queries.

The implementation also includes transaction management with `commit()`, `rollback()` on failure, and database connection cleanup.

## Airflow Task Dependencies

The workflow uses the **Airflow TaskFlow API** with the `@task` decorator.

```python
weather_data = extract_weather_data()
transformed_data = transform_weather_data(weather_data)
load_weather_data(transformed_data)
```

The tasks execute in the following order:

**Extract → Transform → Load**

Airflow manages task dependencies, scheduling, execution status, and logs.

## Running the Project Locally

### Prerequisites

Install the following:

- Docker Desktop
- Astronomer CLI
- Git
- Python (for local development)

### Step 1: Clone the Repository

```bash
git clone <your-repository-url>
cd <your-project-directory>
```

Replace the placeholders with your actual repository URL and directory.

### Step 2: Start Apache Airflow

From the project root directory, run:

```bash
astro dev start
```

Astronomer CLI starts the local Airflow environment using Docker containers.

### Step 3: Access the Airflow UI

Open:

`http://localhost:8080`

### Step 4: Configure Airflow Connections

Create or verify the following connections in Airflow:

**Open-Meteo API Connection**

- Connection ID: `open_meteo_api`
- Connection Type: HTTP
- Host: `https://api.open-meteo.com`

**PostgreSQL Connection**

- Connection ID: `postgres_default`
- Connection Type: Postgres
- Host: The hostname reachable from the Airflow containers
- Port: `5432`
- Database: Your target database
- Username and password: Credentials for your PostgreSQL instance

These connection IDs must match those defined in the DAG.

The project can also use `airflow_settings.yaml` to configure local Airflow connections.

**Note:** The Airflow metadata database and the database used to store weather observations serve different purposes. Ensure `postgres_default` points to the intended weather-data database.

### Step 5: Execute the DAG

1. Open the Airflow UI.
2. Locate `weather_etl_pipeline`.
3. Enable the DAG.
4. Trigger the DAG manually to test it, or allow its daily schedule to run.
5. Monitor task execution using the Grid view and task logs.

### Step 6: Verify PostgreSQL Data

Connect to the target PostgreSQL database and execute:

```sql
SELECT *
FROM weather_data
ORDER BY timestamp DESC;
```

This query displays the weather observations stored by the pipeline.

## Key Implementation Features

- Python-based end-to-end ETL workflow
- REST API integration using Airflow HttpHook
- Daily workflow scheduling using Apache Airflow
- TaskFlow API-based task orchestration
- JSON data extraction and transformation
- PostgreSQL storage using PostgresHook
- SQL table creation and parameterized inserts
- Database transaction handling and rollback
- Docker-based local development using Astronomer CLI

## Current Scope and Limitations

The pipeline currently retrieves weather observations for a fixed location: London.

It stores current weather values and a database insertion timestamp. The current implementation does not perform deduplication, so repeated or scheduled runs may insert additional records.

The DAG is configured for daily execution, but its successful execution and production readiness depend on the Airflow connections, database configuration, and runtime environment.

## Future Improvements

- Support multiple cities and geographic locations
- Add automated data quality checks
- Implement deduplication and incremental loading
- Add retry policies and failure notifications
- Create dashboards for weather trend analysis
- Deploy the pipeline to a hosted Airflow environment

## Project Demonstration

**Loom Video:** https://www.loom.com/share/276a98d7318f40e480a950b4d1ee4de8

## Conclusion

This project demonstrates practical experience with Python, SQL, REST API integration, Apache Airflow orchestration, PostgreSQL database operations, and Docker-based development.

It provides a foundation for building more scalable and reliable data engineering workflows.
