## Environment:
- Spark Version: 3.0.1
- Python Version: 3.7

## Read-Only Files:
- `src/app.py`
- `src/tests/test_pipeline.py`
- `src/main/__init__.py`
- `src/main/base/__init__.py`
- `src/main/job/__init__.py`
- `install.sh`
- `data/country_vaccinations.csv`

## Requirements:
In this challenge, you will to implement a PySpark pipeline that does some basic analysis on COVID-19 vaccine data. You could find data sample in folder `data`

- `country_vaccinations.csv` 
  - This is COVID-19 vaccine data which recorded the vaccination progress around the world over time.
  - The data format is `country,date,total_vaccinations,vaccines`
  - `country` : this is the country for which the vaccination information is provided
  - `date` : date for the data entry with format: `d/M/yy`
  - `total_vaccinations` : this is the absolute number of total immunizations in the country
  - `vaccines`: name of vaccine (national authority, international organization, local organization)
  
  
The project is partially completed and there are 4 methods and a spark session to be implemented in the class `main.job.pipeline.PySparkJob.py`:

- `init_spark_session(self) -> SparkSession`:
  - create a spark session with name `Covid19 Vaccination Progress`

- `count_available_vaccines(self, vaccines: DataFrame) -> int`:
  - count number of unique `vaccines` around the world

- `find_earliest_used_vaccine(self, vaccines: DataFrame) -> str`:
  - find earliest `vaccine` which has been used in the world
  - <b>Hint</b>: you could base on `date` to find the earliest used `vaccine` and return only name of `vaccine`
  - You should also convert `date` to timestamp data type 
  - For example: `Oxford/AstraZeneca` is the earliest used vaccine
  
    | country       | date        | ...  | vaccine           |
    | ------------- |:-----------:|---:  | -----------------:|
    | UK            | 01/12/20    | ...  | Oxford/AstraZeneca|
    | Russia        | 01/12/20    | ...  | Sputnik V         |
    | US            | 30/12/20    | ...  | Moderna           |
  

- `total_vaccinations_per_country(self, vaccines: DataFrame) -> DataFrame`:
  - Aggregate the vaccines data to see what is `total_vaccinations` for each `country`
  - <b>Note</b>: `total_vaccinations` could be missing in real data due to data entry error. Please ignore all of missing records!
  - For example:
    
    | country       | date        | total_vaccinations  | vaccine           |
    | ------------- |:-----------:|------------------:  | -----------------:|
    | UK            | 01/12/20    | 1000                | Oxford/AstraZeneca|
    | UK            | 01/12/20    | 2000                | Sputnik V         |
    | US            | 30/12/20    | 5000                | Moderna           |
    
  - Expected output:
  
    |country  | total_vaccinations |
    |---------|:------------------:|
    | UK      | 3000               |
    | US      | 5000               |
    
Your task is to complete the implementation of that job so that the unit tests pass while running the tests. You can use the give tests check your progress while solving problem.

## Commands
- run: 
```bash
source venv/bin/activate; python3 src/app.py data/country_vaccinations.csv
```
- install: 
```bash
bash install.sh; source venv/bin/activate; pip3 install -r requirements.txt
```
- test: 
```bash
source venv/bin/activate; py.test
```