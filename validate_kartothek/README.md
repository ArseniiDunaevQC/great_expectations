Validate Kartothek datasets using Great Expectations
================================================================================

GE makes it possible to read and validate data stored as [kartothek](https://kartothek.readthedocs.io/en/latest/index.html#) 
and include the validation into an automated pipeline testing.

Requirements
--------------------------------------------------------------------------------

To run the scripts following packages/environments should be installed:

- python >= 3.4
- great-expectations >= 0.9.7
- pyspark >= 2.3.2
- Java 8 (newer Java versions are not supported by [Spark](https://spark.apache.org/docs/latest/))

For data generation (optional):

- kartothek
- numpy
- pandas
- storefact


Prerequisites
--------------------------------------------------------------------------------

Scripts should be placed into the root directory of the project, alongside with the *data/* folder.


Getting started
--------------------------------------------------------------------------------

### Test Data Generation (optional)

This script is optional if you already have kartothek datasets you want to validate.

Should be executed only once.

1. Run `python generate_data.py`: test datasets will be generated in *data/* folder.


### GE Initialization

This script will initialize GE (i.e. build *great_expectations/* folder of the project), build some generic Expectations 
about provided data, and start Jupyter Notebook, where you can add custom Expectations about your data.

If you already have a GE context, you don't have to execute this script.

1. Run `python init_and_edit.py` 
(substitute for `great_expectations init` and `great_expectations suite edit 'YOUR_EXPECTATION_SUITE_NAME'`).
    
2. Follow the instructions given by GE: You can choose between (Py)Spark and pandas and should provide path 
to your initial dataset, which will be the starting point to build Expectations by GE. 

    - Unfortunately, the current version (0.9.7) of GE does not allow user to initialize GE without providing a data 
    file, what is (hopefully) to come soon.
    - Test data includes *dummy_dataset*, which could be used for that purpose; GE requires **exact** path to data file, 
    so the following should be presented: *data/dummy_dataset/table/D=bad/E=foobar/PARTITION_ID.parquet*.
    
3. Go to the initialized Jupyter Notebook, reconsider the exemplary Expectations, add your own ones, and rerun 
the Notebook!
    
4. After the Data Docs appear, you can kill the Notebook by pressing **Ctr-C** in your terminal.
    

### Data Validation

This script should be executed as many times as many datasets you want to validate: It will ask you information about data 
to validate, and then open the Data Docs with results.

1. Run `python validate_data.py`.

2. First, you should choose the backend that will be used to read data (Spark or pandas).
    > Backend should be chosen according to the configuration of your GE project (see *GE Initialization*), in other cases an error will be raised.
    However, you can still add new datasources (using other backends) with `great_expectations datasource new`.

3. Choose your Datasource (only sources according to the backend will be shown).

4. In case of Spark backend, you should choose the Validation Mechanism:

    - file: separate Validation Result for each '.parquet' file in the dataset.
    - partition: separate Validation Result for each partition (e.g. 'column?value') of the dataset.
    - dataset: one Validation Result for the whole dataset/table.
    
        - > Mechanisms from the above are considered deprecated since they are not applicable to nested (i.e. 'real') 
          kartothek datasets.

    - nested dataset: one Validation Result for the whole dataset/table (works for typical kartothek datasets).
    - nested dataset (using wildcard): one Validation Result for the whole dataset/table, but is more conform with GE architecture.
    
    Pandas backend produces one Validation Result for the whole dataset/table.
    
5. Choose Expectation Suite and Dataset from the source you want to validate.

6. Data Docs will open, displaying the Validation Result.
 
7. Repeat 1.-4. as many times you need.
