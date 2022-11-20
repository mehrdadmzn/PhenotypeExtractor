import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import yaml


class ParameterSet:
    def __init__(self, parameter_yaml, table_tag):
        pyaml = yaml.load(parameter_yaml, Loader=yaml.SafeLoader)
        self.table_tag = pyaml.get(table_tag).get("table_tag")
        self.production_date_str = pyaml.get(table_tag).get("production_date_str")
        self.index_col = pyaml.get(table_tag).get("index_col")
        self.evdt_col_raw = pyaml.get(table_tag).get("evdt_col_raw")
        self.evdt_col_list = pyaml.get(table_tag).get("evdt_col_list")
        self.evdt_pheno = pyaml.get(table_tag).get("evdt_pheno")
        self.start_date_qc = pyaml.get("quality_control").get("start_date_qc")
        self.end_date_qc = pyaml.get("quality_control").get("end_date_qc")
        self.full_report = pyaml.get("optional_settings").get("full_report")
        self.spark_cache_midway = pyaml.get("optional_settings").get("spark_cache_midway")
        self.impute_multi_col_null_dates = pyaml.get("optional_settings").get("impute_multi_col_null_dates")
        self.impute_multi_col_invalid_dates = pyaml.get("optional_settings").get("impute_multi_col_invalid_dates")
        self.drop_null_ids = pyaml.get("optional_settings").get("drop_null_ids")
        self.drop_remaining_null_dates = pyaml.get("optional_settings").get("drop_remaining_null_dates")
        self.drop_remaining_invalid_dates = pyaml.get("optional_settings").get("drop_remaining_invalid_dates")