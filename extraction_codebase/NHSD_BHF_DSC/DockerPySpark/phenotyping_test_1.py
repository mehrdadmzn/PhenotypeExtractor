# %%
'''
To load data in Pyspark Docker or Databricks Community Edition
'''
# %%
import datetime
# findspark.init()
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from pheno_package.helper_funcitons.DateHelperModule import str_to_date
from pheno_package.input_output_package.CsvFileLoader import import_csv
from pheno_package.nhsd_docker_pyspark_package.DateBasedPhenoFunctions import event_pheno_extractor
import yaml

from pheno_package.nhsd_docker_pyspark_package.FacadeFunctions import covid_pheno_date_based, show_dfset_dfs

# %%

# df = SparkSession.read.format("csv").load("fake_data/NHSD_BHF_DSC/GDPPR.csv")
# sc = pyspark.SparkContext()
# sq = pyspark.SQLContext(sc)
spark_pyspark = SparkSession.builder.master("local[1]").appName("NHS_TRE_Simulation").getOrCreate()

# Load skinny table
# skinny_df = spark_pyspark.read.format("csv").option("header", "true").load("/opt/project/fake_data/NHSD_BHF_DSC/skinny.csv")
skinny_df = import_csv("skinny.csv", databricks_import=False)

# Load gdppr
gdppr_df = import_csv("GDPPR.csv", databricks_import=False)

# gdppr_df = spark_pyspark.read.format("csv").option("header", "true").load("/opt/project/fake_data/NHSD_BHF_DSC/GDPPR.csv")

# Load sgss
sgss_df = import_csv("sgss.csv", databricks_import=False)
sgss_df = spark_pyspark.read.format("csv").option("header", "true").load("/opt/project/fake_data/NHSD_BHF_DSC/sgss.csv")

# %%
# %%

# Params
param_yaml = """\
quality_control:
  start_date_qc: "2019-12-01"  # Start of the pandemic
  end_date_qc: "2022-08-31"  # final_production date
sgss:
  table_tag: sgss
  production_date_str: '2022-08-31'
  index_col: PERSON_ID_DEID
  evdt_col_raw: Specimen_Date
  evdt_col_list:
    - Specimen_Date
    - Lab_Report_Date
  evdt_pheno: sgss_evdt
final_table:
  table_tag: covid
  index_col: NHS_NUMBER_DEID
  evdt_pheno: covid_evdt
optional_settings:
  full_report: yes
  spark_cache_midway: yes
  impute_multi_col_null_dates: yes
  impute_multi_col_invalid_dates: yes
  drop_null_ids: yes
  drop_remaining_null_dates: yes
  drop_remaining_invalid_dates: yes
"""
pyaml = yaml.load(param_yaml, Loader=yaml.SafeLoader)
# Set dates
start_date_str = pyaml.get("quality_control").get("start_date_qc")
end_date_str = pyaml.get("quality_control").get("end_date_qc")

start_date = str_to_date(start_date_str)
end_date = str_to_date(end_date_str)

# %%

# Basic covid_df
covid_df_1 = skinny_df.select(["NHS_NUMBER_DEID"])
covid_df_1 = covid_df_1.withColumn("start_date", F.to_date(F.lit(start_date_str))).withColumn("end_date", F.to_date(
    F.lit(end_date_str)))
covid_df_1 = covid_df_1.withColumn("isin_skinny", F.lit(1))
covid_df_1.show()

print(sgss_df.dtypes)
print(sgss_df.filter(F.col("PERSON_ID_DEID").isNull()).count())

# %%


# Add SGSS
sgss_dfset, pheno_full_long = covid_pheno_date_based(sgss_df, param_yaml, table_tag="sgss", distinct_dates=True)
pheno_full_long.show()
print(sgss_dfset.pheno_df_full.collect())
sgss_pheno_first= sgss_dfset.make_first_eventdate_pheno(add_isin_flag=True)
sgss_pheno_first.show()

sgss_pheno_last= sgss_dfset.make_last_eventdate_pheno(add_isin_flag=True)
sgss_pheno_last.show()

sgss_pheno_distinct= sgss_dfset.make_distinct_eventdate_pheno(add_isin_flag=True)
sgss_pheno_distinct.show()

sgss_pheno_all= sgss_dfset.make_all_qc_eventdate_pheno(add_isin_flag=False)
sgss_pheno_all.show()

