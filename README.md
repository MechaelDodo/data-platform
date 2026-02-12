# Rick and Morty Data Platform

## Project Overview
This project is an **ETL pipeline** for collecting and organizing data from the public [Rick and Morty API](https://rickandmortyapi.com).  
It extracts data about **characters, locations, and episodes**, stores the full JSON in a raw layer, and then normalizes the data into structured tables for easier analysis.

The main idea of the project:

API → raw layer → normalized (staging) layer

- **Raw layer**: stores the full JSON as received from the API  
- **Staging layer**: normalized tables ready for analysis, with relationships between entities

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