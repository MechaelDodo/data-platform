# Rick and Morty Data Platform

## Project Overview
This project is an **ETL pipeline** for collecting and organizing data from the public [Rick and Morty API](https://rickandmortyapi.com).  
It extracts data about **characters, locations, and episodes**, stores the full JSON in a raw layer, and then normalizes the data into structured tables for easier analysis.

The main idea of the project:

API → raw layer → normalized (staging) layer → DWH layer → Datamarts

- **Raw layer**: stores the full JSON as received from the API  
- **Staging layer**: normalized tables ready for analysis, with relationships between entities  
- **DWH layer**: dimensional model built on top of staging, with SCD Type 2 logic for historical tracking of attributes and preparation for fact tables and datamarts

## Tools and Technologies
- **Apache Airflow** – for orchestrating ETL workflows  
- **PostgreSQL** – main database for storing raw and normalized data  
- **Python** – for implementing data extraction, transformations, and custom operators  
- **RamAPI library** – simplifies access to the Rick and Morty API  
- **Docker & Docker Compose** – for running the environment locally in containers  

## Data Structure
- **Entities loaded**: Characters, Locations, Episodes  
- **Relationships captured**:  
  - Characters linked to their episodes  
  - Characters linked to their locations  
  - Locations linked to residents  
- **DWH Dimensions**:  
  - `dwh.dim_character`  
  - `dwh.dim_location`  
  - `dwh.dim_episode`  
  - `dwh.dim_location_role` (role of character relative to location: 'origin' | 'last')  
  - Each dimension includes surrogate keys, business keys, URL identifiers, and SCD Type 2 fields (`valid_from`, `valid_to`, `is_current`, `created_at`, `last_upd_at`)  
  - Historical tracking preserves previous versions of records and supports analytical queries
- **DWH Fact Tables**:
- `dwh.fact_char_loc` – links characters to locations with a `role_id`  
- `dwh.fact_char_ep` – links characters to episodes  
- Fact tables use surrogate keys from dimensions and `created_at` timestamps  
- They are incremental and idempotent, ensuring consistent analytical queries

## Resources
- [Rick and Morty API](https://rickandmortyapi.com) – official API for characters, locations, and episodes  
- [RamAPI Documentation](https://ramapi.readthedocs.io/en/latest/usage.html) – Python library for interacting with the API  
- [RamAPI GitHub](https://github.com/curiousrohan/ramapi) – official repository of the library  

## How to Run the Project

### 1. Clone the repository

```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo
```

### 2. Create environment file

Copy example environment variables:

```bash
cp .env.example .env
```

Adjust values if needed.

### 3. Start Docker containers

```bash
docker-compose up --build
```

### 4. Access Airflow

Airflow UI will be available at:

http://localhost:8080

Default credentials (if configured in docker-compose):
- Username: airflow
- Password: airflow

### 5. Configure the Database

**Important:** Before running any ETL DAGs, open the Airflow UI and manually run the `configure` DAG.  
This will create all necessary **schemas** and **tables** (raw, staging, DWH, datamarts) in PostgreSQL.