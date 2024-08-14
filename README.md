# APAC - OHDSI - OMOP - PASAR DATA

- Introduction: https://forums.ohdsi.org/t/call-for-volunteers-apac-community-wide-etl-project/22044
- PASAR: https://www.ncbi.nlm.nih.gov/pmc/articles/PMC10834714/

## OMOP

- Version: `5.4`
- DDL Artifacts: https://github.com/OHDSI/CommonDataModel/tree/main/ddl/5.4/bigquery
- Specific Git Commit: https://github.com/OHDSI/CommonDataModel/commit/c1c8e6a4f04e588d72fa9ae5df56b1631559548b
- Copied files to `etl/db` and removed prefix `@cdmDatabaseSchema.` in those files (since ingestion will happen via SqlAlchemy)

## Analysis

https://ohdsiorg.sharepoint.com/:x:/s/OHDSIAPAC-2024APACETLProject-Dataanalysis/EZWK6NCPUFtJpEOU54YmemcBp-B4JIk1Er_T7cDZApXVJQ?e=N32f6C&wdOrigin=TEAMS-MAGLEV.teams_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1722821798159&web=1

## Vocabulary Mapping

## ETL

### Pre-requisites
- bash
- Python >= `v3.10`

<i>For Windows users please adapt the following steps accordingly!</i>

### Setup Python environment

- Navigate to under `etl` folder

- Run the following commands:
	1. Create and activate virtual environment
	    - `python3 -m venv pypasarenv`
		- `source pypasarenv/bin/activate`
	2. Install python packages `pip install -r requirements.txt`
	4. Clone .env file from example `cp .env.example .env`
	5. Update `.env` file with credentials

### OMOP Database Setup

Postgres & Bigquery sql scripts are now stored at this point in time `etl/pypasar/db/sql` and will likely to change based on usage later

### Setup Postgres via Docker

- Ensure docker is installed
- Export envs `source .env` / Copy the value `POSTGRES_PORT` from .env and replace in the next line
- Run Postgres as docker container `docker run -v pg-pasar-data:/var/lib/postgresql/data --env-file .env -d --name pasar-postgres -p ${POSTGRES_PORT}:5432 postgres:16-alpine`

### Existing R Setup

If you have an existing R Setup and familiar with OHDSI Packages then setup the OMOP using https://github.com/OHDSI/CommonDataModel/blob/main/README.md

### Using Python

- Ensure 
	1. Environment variables are setup accordingly in `.env`
	2. Current working directory is under `etl` folder

- Create omop schema and tables 
	1. Run `python . db create_omop_schema`. 
	2. Schema defined as `POSTGRES_OMOP_SCHEMA` in `.env` will be created and OMOP tables populated.
	3. Verify through PGAdmin / psql client

- Drop omop schema and tables 
	1. `python . db drop_omop_schema`. 
	2. Schema defined as `POSTGRES_OMOP_SCHEMA` in `.env` will be dropped


### Execute ETL

- To begin contributing transformation to the various OMOP tables, go to `etl/pypasar/omop` and choose the appropriate python file
- <i>SQL can be used as well in the python class</i>
- Example is available for cdm_source table at `etl/pypasar/omop/cdm_source.py`


### ETL Development & Testing

- Please feel free to implement in whichever way you choose. The only <b>mandatory requirement</b> is that the `execute` must be the entrypoint to the respective omop class. <b>Because `execute` method will be called for each class from `__main__.py` file</b>

- Current working directory is under `etl` folder

- Run `python . etl <omop_table_name>`. 
	- Example `python . etl cdm_source`
	- Multiple tables for cdm_source and concept `python . etl cdm_source,concept`. <b>NO SPACES BETWEEN COMMA SEPARTED OMOP Tables</b>

### Cleanup

#### Remove Python environment
- Run `deactivate`
- Under `etl`, Run `rm -rf pypasarenv`
	
#### Remove Docker Container & Volume
- Remove container, Run `docker rm -f pasar-postgres`
- Remove volume (<b>CAUTION - ALL DATA WILL BE LOST!!</b>), Run `docker volume rm pg-pasar-data` 

