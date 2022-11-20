# %%
"""To load csv files depending on either PySpark Docker or Databricks Community Edition
"""
# %%
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


# %%
def csv_loader_spark(spark_session, table_name: str, path: str = "/opt/project/fake_data/NHSD_BHF_DSC") -> DataFrame:
    """

    Args:
        spark_session:
        table_name:
        path:

    Returns:

    """
    #spark = SparkSession.builder.master("local[1]").appName("NHS_TRE_Simulation").getOrCreate()
    df_out = spark_session.read.format("csv").option("header", "true").load(f'''{path}/{table_name}''')
    return df_out


# %%
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


# %%
def import_csv(table_name, databricks_import=True):
    """

    Args:
        spark_session:
        table_name:
        import_type:

    Returns:

    """

    if databricks_import:
        spark_session = spark
        return csv_loader_databricks(spark_session, table_name, path="/FileStore/tables/fake_data")
    else:
        spark_session = SparkSession.builder.master("local[1]").appName("NHS_TRE_Simulation").getOrCreate()
        return csv_loader_spark(spark_session, table_name, path="/opt/project/fake_data/NHSD_BHF_DSC")
