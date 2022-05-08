# Moat Outcomes Platform Case Study

This document is designed to be read in parallel with the code. 
This write up gives a brief introduction on how the code to solve the case study was developed

## Problem Statement

Take the provided randomly generated datasets (demographics, transactions, cookie-individual mapping, household-individual mapping, online activity), and combine them to get one dataset at the household level. Using development techniques that you would apply in a production big data workflow, generate a table containing features that would be useful to measure the effectiveness of a campaign that was run from 2021-09-06 to 2021-09-13. Assume the transactions table will not exceed 100 MB, and the other tables could go over 10 GB. Data sent for this project is clean, however checks can be included as part of the production script. Also, be sure to specify the format in which the final table will be stored.


## ETL Project Structure

The basic project structure is as follows:

```bash
root/
 |-- configs/
 |   |-- etl_config.json
 |-- data/
 |   |-- activity.csv
 |   |-- cookie_ind.csv
 |   |-- demographics.csv
 |   |-- hh_ind.csv
 |   |-- transactions.csv
 |-- dependencies/
 |   |-- logging.py
 |   |-- spark.py
 |-- output/
 |   |-- part--*.csv
 |-- jobs/
 |   |-- etl_job.py
 |-- tests/
 |   |-- test_etl_job.py
 |   build_dependencies.sh
 |   packages.zip
 |   Pipfile
 |   Pipfile.lock
```

The main Python module containing the ETL job (which will be sent to the Spark cluster), is `jobs/etl_job.py`. Any external configuration parameters required by `etl_job.py` are stored in JSON format in `configs/etl_config.json`. 
Additional modules that support this job can be kept in the `dependencies` folder. In the project's root I included `build_dependencies.sh`, which is a bash script for building these dependencies into a zip-file to be sent to the cluster (`packages.zip`). Unit test modules are kept in the `tests` folder. The input data files are kept under  `data/` folder and the output csv file is generated under `output/` folder


## Running the ETL job

The ETL job can be run from the project's root directory using the following command from the terminal,

```bash
$SPARK_HOME/bin/spark-submit \
--master local[*] \
--py-files packages.zip \
--files configs/etl_config.json \
jobs/etl_job.py
```

## Automated Testing

I have created some Test Cases to check the data quality of the input data. To check the data quality, One of the use cases I have used for to check the null values in the column for each table that would be used for joining other tables. This is to check we are not joining on any nulls. To do this I have used `unittest` module in python to crete test cases with `setUp` and `tearDown` methods in `unittest.TestCase` to do this once per test-suite. Ideally we would be like this value of nulls to be 0

I have used the following command to run the test cases

```bash
pipenv run python -m unittest tests/test_etl_job.py
```
