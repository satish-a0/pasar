# APAC - OHDSI - OMOP - PASAR DATA

- Introduction: https://forums.ohdsi.org/t/call-for-volunteers-apac-community-wide-etl-project/22044
- PASAR: https://www.ncbi.nlm.nih.gov/pmc/articles/PMC10834714/

## OMOP

- Version: `5.4`
- DDL Artifacts: https://github.com/OHDSI/CommonDataModel/tree/main/ddl/5.4/postgresql
- Specific Git Commit: https://github.com/OHDSI/CommonDataModel/commit/c1c8e6a4f04e588d72fa9ae5df56b1631559548b
- Copied files to `etl/db` and removed prefix `@cdmDatabaseSchema.` in those files (since ingestion will happen via SqlAlchemy)

## Data Analysis

- Releases from DA To ETL Team: https://github.com/sidataplus/PASAR_ETL_Spec

- Working Sheet: https://ohdsiorg.sharepoint.com/:x:/r/sites/OHDSIAPAC/_layouts/15/Doc2.aspx?action=edit&sourcedoc=%7B5aa99f90-c21c-4ce4-8edf-70b99dfa0ab4%7D&wdOrigin=TEAMS-MAGLEV.teamsSdk_ns.rwc&wdExp=TEAMS-TREATMENT&wdhostclicktime=1724247157445&web=1

## Vocabulary Mapping

- Tracking uploads: https://github.com/satish-a0/pasar/pull/21

## ETL

- Working Sheet: https://ohdsiorg.sharepoint.com/:x:/s/OHDSIAPAC/ERctK2EQDGlDg4yh79imNpkBfMY4Rg7IhA0lKfYmCpz1tg?e=fPzMTS&nav=MTVfe0VGMUNGNjA1LUZDNDgtNDMzNy05RDg4LTJGMTBCRTIxRDkyQn0
- Timelines: https://ohdsiorg.sharepoint.com/:x:/s/OHDSIAPAC/EctGt_SfFWlErKJWLO3xXp4Bsnh4tYu5sVFl2de1TOHq2w?e=ICTq4g&nav=MTVfezExMDFDNDc5LUNCNzEtNDA1MC1BOTkzLTc1NDgxQTFBNzlFNH0

## Quality Assurance

- Results: https://github.com/SantanM/qa-pasar/
- Working Sheet: https://ohdsiorg.sharepoint.com/:x:/s/OHDSIAPAC/ERctK2EQDGlDg4yh79imNpkBfMY4Rg7IhA0lKfYmCpz1tg?e=t8dmaj&nav=MTVfezA0MERGRkQxLTZCRTItNDkwRC1CMTEwLTE3NTE2QjY0OEFGM30

### Pre-requisites
- bash
- Python >= `v3.10`

<i>For Windows users please adapt the following steps accordingly! Recommend to install linux on windows:</i> https://learn.microsoft.com/en-us/windows/wsl/install

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

Postgres sql scripts are now stored at this point in time `etl/pypasar/db/sql` and will likely to change based on usage later

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

### Load Athena Vocab
1. Copy the `CONCEPT.csv`, `CONCEPT_RELATIONSHIP.csv`, `CONCEPT_ANCESTOR.csv` from the GCP Bucket `ohdsi_omop_2024/vocab_2024Nov03_v5` to the folder `etl/pypasar/db/sql/postgres/vocab`
2. Run `python . etl concept,concept_relationship,concept_ancestor`. Note: <i>You might run into foreign key constraints for the concept tables during truncation. Drop them, Truncate and Readd them again.</i>

### Cleanup

#### Remove Python environment
- Run `deactivate`
- Under `etl`, Run `rm -rf pypasarenv`
	
#### Remove Docker Container & Volume
- Remove container, Run `docker rm -f pasar-postgres`
- Remove volume (<b>CAUTION - ALL DATA WILL BE LOST!!</b>), Run `docker volume rm pg-pasar-data` 

## GCP Development

### Setup VM

In the below snippet at <b>Step 3</b>
1. Replace `<IP>` Based on group in the below snippet
2. Replace `<username>` (<i>filename of the private key / mentioned in ETL Development sheet</i>)
3. Copy the snippet to `~/.ssh/config` and save the file
```
Host pypasar
    HostName <IP>
    User <username>
    IdentityFile ~/.ssh/<username>
    ControlMaster     auto
    ControlPath       ~/.ssh/control-%C
    ControlPersist    yes
```

4. Copy public and private key files to `~/.ssh` folder
5. Test on terminal `ssh pypasar`. Should be able to login to home folder, run `pwd`.

6. For remote development https://code.visualstudio.com/docs/remote/ssh


### Setup Git repo in the local folder

1. Once you are in your home folder, refer to this document https://ohdsiorg.sharepoint.com/:p:/s/OHDSIAPAC/ESUGOh6Lza9FvxH1TyaoO7oBlMv_9Iq57tLQ-41V2HUFtA?e=QdjNOP and fork this repo to your org account.

2. Once the repo clone is done, navigate to the `<username>-pasar` repo
3. Enter the following configuration (<b>No GLOBAL!!</b>)

```
git config user.name "<username>"
git config user.email <email>
```

4. Follow the document in step 1 to create a new branch and push to repo.

### Setup Postgres GUI on Vscode GCP VM
1. How to browse and Install VS Code extensions: https://code.visualstudio.com/docs/editor/extension-marketplace#_browse-for-extensions
2. Recommended Postgres GUI extension: https://marketplace.visualstudio.com/items?itemName=ckolkman.vscode-postgres
