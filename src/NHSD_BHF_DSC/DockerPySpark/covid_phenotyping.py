# COMMAND ----------
'''
To load data in Pyspark Docker or Databricks Community Edition
'''
# COMMAND ----------
import datetime
# findspark.init()
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from pheno_package.helper_funcitons.DateHelperModule import str_to_date
from pheno_package.input_output_package.CsvFileLoader import import_csv
# from pheno_package.nhsd_docker_pyspark_package.PhenoExtractionHelperFuncitons import make_dfset_object
import yaml

from pheno_package.nhsd_docker_pyspark_package.DataFrameSet import make_all_qc_eventdate_pheno
from pheno_package.nhsd_docker_pyspark_package.FacadeFunctions import covid_pheno_date_based, show_dfset_dfs, \
    make_date_base_pheno
from pheno_package.pyspark_databricks_interface.DockerPysparkToDatabricks import display

# COMMAND ----------

# df = SparkSession.read.format("csv").load("fake_data/NHSD_BHF_DSC/GDPPR.csv")
# sc = pyspark.SparkContext()
# sq = pyspark.SQLContext(sc)
spark_pyspark = SparkSession.builder.master("local[1]").appName("NHS_TRE_Simulation").getOrCreate()

# Load skinny table
skinny_df = import_csv(spark_session=spark_pyspark, table_name="skinny.csv", path="../../../fake_data/NHSD_BHF_DSC",
                       databricks_import=False)
# skinny_df = spark_pyspark.read.format("csv").option("header", "true").load("../../../fake_data/NHSD_BHF_DSC/skinny.csv")

# Load gdppr
gdppr_df = import_csv(spark_session=spark_pyspark, table_name="GDPPR.csv", path="../../../fake_data/NHSD_BHF_DSC",
                      databricks_import=False)

# Load sgss
sgss_df = import_csv(spark_session=spark_pyspark, table_name="sgss.csv", path="../../../fake_data/NHSD_BHF_DSC",
                     databricks_import=False)

# Load chess
chess_df = import_csv(spark_session=spark_pyspark, table_name="chess.csv", path="../../../fake_data/NHSD_BHF_DSC",
                      databricks_import=False)

# COMMAND ----------


# Params for SGSS
sgss_yaml = """\
phenotype_name: covid
table_tag: sgss
codelist_format: none
pheno_details:
  evdt_pheno: sgss_evdt
  pheno_pattern: date_based_diagnosis # Todo
  terminology: none
  check_code_type: none
  code_type: none
  limit_pheno_window: no # if set to yes, the following two options must be set
  pheno_window_start: '2020-01-01'
  pheno_window_end: '2021-06-12'
table_details:
  table_tag: sgss
  index_col: PERSON_ID_DEID
  evdt_col_raw: Specimen_Date
  evdt_col_list:
    - Specimen_Date
    - Lab_Report_Date
  production_date_str: '2022-08-31'
quality_control:
  # Time window for event date quality check. Any dates before or after this window
  # must be excluded for quality assurance.
  start_date_qc: "2019-12-01" # start of the pandemic
  end_date_qc: "2022-08-31"  # final_production date
  # valid table tag: gdppr, hes_apc, sgss, chess, pillar2
  # valid pheno_pattern: code_based_diagnosis, date_based
  # valid phenotype_codelist_format: bhf_tre, hdruk
  # valid data type: yyyy-mm-dd
optional_settings:
  full_report: yes
  spark_cache_midway: yes
  impute_multi_col_null_dates: yes
  impute_multi_col_invalid_dates: yes
  drop_null_ids: yes
  drop_remaining_null_dates: yes
  drop_remaining_invalid_dates: yes
"""
sgss_set = make_date_base_pheno(df_raw=sgss_df, table_tag="sgss",
                                param_yaml=sgss_yaml,
                                list_extra_cols_to_keep=["details"])
display(sgss_set.df_pheno_beta)
