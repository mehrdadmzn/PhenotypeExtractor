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
from pheno_package.nhsd_docker_pyspark_package.DateBasedPhenoFunctions import event_pheno_extractor
import yaml

from pheno_package.nhsd_docker_pyspark_package.FacadeFunctions import covid_pheno_date_based, show_dfset_dfs
from pheno_package.pyspark_databricks_interface.DockerPysparkToDatabricks import display

# COMMAND ----------

# df = SparkSession.read.format("csv").load("fake_data/NHSD_BHF_DSC/GDPPR.csv")
# sc = pyspark.SparkContext()
# sq = pyspark.SQLContext(sc)
spark_pyspark = SparkSession.builder.master("local[1]").appName("NHS_TRE_Simulation").getOrCreate()

# Load skinny table
skinny_df = import_csv(spark_session= spark_pyspark, table_name="skinny.csv", path= "../../../fake_data/NHSD_BHF_DSC", databricks_import=False)
#skinny_df = spark_pyspark.read.format("csv").option("header", "true").load("../../../fake_data/NHSD_BHF_DSC/skinny.csv")

# Load gdppr
gdppr_df = import_csv(spark_session= spark_pyspark, table_name="GDPPR.csv", path= "../../../fake_data/NHSD_BHF_DSC", databricks_import=False)

# Load sgss
sgss_df = import_csv(spark_session= spark_pyspark, table_name="sgss.csv", path= "../../../fake_data/NHSD_BHF_DSC", databricks_import=False)

# Load chess
chess_df = import_csv(spark_session= spark_pyspark, table_name="chess.csv", path= "../../../fake_data/NHSD_BHF_DSC", databricks_import=False)

# COMMAND ----------


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
chess:
  table_tag: chess
  production_date_str: '2022-08-31'
  index_col: PERSON_ID_DEID
  evdt_col_raw: HospitalAdmissionDate
  evdt_col_list:
    - HospitalAdmissionDate
    - InfectionSwabDate
    - LabTestDate
  evdt_pheno: chess_evdt
gdppr:
  table_tag: gdppr
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
# COMMAND ----------
start_date_str = pyaml.get("quality_control").get("start_date_qc")
end_date_str = pyaml.get("quality_control").get("end_date_qc")

start_date = str_to_date(start_date_str)
end_date = str_to_date(end_date_str)

# COMMAND ----------

# Basic covid_df
covid_df_1 = skinny_df.select(["NHS_NUMBER_DEID"])
covid_df_1 = covid_df_1.withColumn("start_date", F.to_date(F.lit(start_date_str))).withColumn("end_date", F.to_date(
    F.lit(end_date_str)))
covid_df_1 = covid_df_1.withColumn("isin_skinny", F.lit(1))
covid_df_1.show()

print(sgss_df.dtypes)
print(sgss_df.filter(F.col("PERSON_ID_DEID").isNull()).count())

# COMMAND ----------


# Add SGSS
def run_sgss():
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
#run_sgss()
# COMMAND ----------
def run_chess():
    chess_dfset, pheno_full_long = covid_pheno_date_based(chess_df, param_yaml, table_tag="chess", distinct_dates=True)
    print("df_master")
    print(chess_dfset.pheno_df_full.collect())

    chess_pheno_first = chess_dfset.make_first_eventdate_pheno(add_isin_flag=True)
    chess_pheno_first.show()

    chess_pheno_last = chess_dfset.make_last_eventdate_pheno(add_isin_flag=True)
    chess_pheno_last.show()

    chess_pheno_distinct = chess_dfset.make_distinct_eventdate_pheno(add_isin_flag=True)
    chess_pheno_distinct.show()

    chess_pheno_all = chess_dfset.make_all_qc_eventdate_pheno(add_isin_flag=False)
    chess_pheno_all.show()

#run_chess()
# COMMAND ----------
# Check master code list
# this is similar to the master codelist in TRE
# For test, it includes HF and IS (bith with SNOMED and ICD-10 codes) and Diabetes with only SNOMED code.
# We will add icd-10 codes later.

masterdf = import_csv(spark_session= spark_pyspark, table_name="master_codelist.csv", path= "../../../codelists/v_01", databricks_import=False)
display(masterdf)
#masterdf.show(n=30)
# COMMAND ----------

display(masterdf.select(F.col("name")).distinct().orderBy("name"))
# COMMAND ----------

display(masterdf.filter(F.col("name")=="HF").select(F.col("terminology")).distinct())
# COMMAND ----------

display(masterdf.filter(F.col("name")=="diabetes").select(F.col("terminology")).distinct())

# COMMAND ----------
display(masterdf.filter(F.col("name")=="diabetes").filter(F.col("code_type")==0))



# COMMAND ----------
# Add GDPPR
display(gdppr_df)


