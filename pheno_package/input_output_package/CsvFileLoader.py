# COMMAND ----------
"""To load csv files depending on either PySpark Docker or Databricks Community Edition
"""
# %%
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame


# COMMAND ----------
def csv_loader_spark(spark_session, table_name: str, path: str) -> DataFrame:
    """

    Args:
        spark_session:
        table_name:
        path:

    Returns:

    """
    # spark = SparkSession.builder.master("local[1]").appName("NHS_TRE_Simulation").getOrCreate()
    df_out = spark_session.read.format("csv").option("header", "true").load(f'''{path}/{table_name}''')
    return df_out


# COMMAND ----------
def csv_loader_databricks(spark_session, table_name: str, path: str = "/FileStore/tables/fake_data") -> DataFrame:
    """

    Args:
        spark_session:
        table_name:
        path:

    Returns:

    """
    # File location and type
    file_location = f'''{path}/{table_name}'''
    # CSV options
    infer_schema = "false"
    first_row_is_header = "true"
    delimiter = ","
    # The applied options are for CSV files. For other file types, these will be ignored.
    df = spark_session.read.format(file_type) \
        .option("inferSchema", infer_schema) \
        .option("header", True) \
        .option("sep", delimiter) \
        .load(file_location)
    return (df)


# COMMAND ----------
def import_csv(spark_session, table_name, path, databricks_import=True):
    """

    Args:
        spark_session:
        table_name:
        import_type:

    Returns:

    """

    if databricks_import:
        # spark_session = spark
        return csv_loader_databricks(spark_session, table_name, path=path)
    else:
        spark_session = SparkSession.builder.master("local[1]").appName("NHS_TRE_Simulation").getOrCreate()
        return csv_loader_spark(spark_session, table_name, path=path)


# COMMAND ----------

def cell_csv_import(imported_text: str, drop_header: bool = True, delimiter: str = "\t",
                    format: str = "tre_masterlist") -> Tuple[list, list]:
    """To import csv file pasted in Databricks clipboard

    Args:
        imported_text: By default, tab-delimited text copied from the csv file
        drop_header: If the first row is the header, set this flag to True.
        delimiter: Indicates the delimiter used. Only tab is used in allowed in the current version
        format: Indicates the structure of the file.
            Option 1: "tre_masterlist" indicates the structure used in the TRE master codelist.
                It has five columns:
                    1) name: the name of the phenotype
                    2) terminology: SNOMED, ICD-10, or other terminologies
                    3) code: the associated code with the condition/medication/operation/biomarker
                    4) term: free-text description of the code
                    5) code-type: For diagnostic codes: code_type = 1 means an incident/first-time diagnosis,
                        while a code_type = 0 means a prevalent diagnosis (e.g., history of or pre-existing condition)
                    5) RecordDate: in the yyyyMMdd, indicating the date the phenotype list is defined. This can be used
                        for versioning of the phenotype in tre_masterlist




    Returns:
        A codelist table in the specified format (entered in format parameter)

    """
    line_list = imported_text.splitlines()
    if line_list[0] == '':
        line_list.pop(0)
    if drop_header:
        # Todo: check/test order and format
        header = line_list.pop(0)
    if format == "tre_masterlist":
        header = ['name', 'terminology', 'code', 'term', 'code_type', 'RecordDate']
    codelist = []
    for line in line_list:
        # list of tuples
        codelist.append(tuple(line.split(delimiter)))
    return codelist, header


def list_to_pyspark_df(spark_session: SparkSession, vals_list_of_tuples: list, mastercodelist_cols: list) -> DataFrame:
    """Makes a Spark dataframe with the list of tuples containing the imported csv text

    Args:
        spark_session:
        vals_list_of_tuples:
        mastercodelist_cols:

    Returns:

    """

    new_codes = spark_session.createDataFrame(vals_list_of_tuples, mastercodelist_cols)
    return new_codes
