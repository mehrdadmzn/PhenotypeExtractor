# COMMAND ----------
'''
To load data in Pyspark Docker or Databricks Community Edition
'''
# COMMAND ----------
import datetime

# findspark.init()
import pyspark.sql.functions as F
import yaml
from pyspark.sql import SparkSession

from pheno_package.input_output_package.CsvFileLoader import import_csv, cell_csv_import, list_to_pyspark_df
from pheno_package.nhsd_docker_pyspark_package.DataFrameSet import make_code_based_pheno
from pheno_package.pyspark_databricks_interface.DockerPysparkToDatabricks import display

# COMMAND ----------

spark_pyspark = SparkSession.builder.master("local[1]").appName("NHS_TRE_Simulation").getOrCreate()

# Load skinny table
skinny_df = import_csv(spark_session=spark_pyspark, table_name="skinny.csv", path="../../../fake_data/NHSD_BHF_DSC",
                       databricks_import=False)

# Load gdppr
gdppr_df = import_csv(spark_session=spark_pyspark, table_name="GDPPR.csv", path="../../../fake_data/NHSD_BHF_DSC",
                      databricks_import=False)

# COMMAND ----------
# Check master code list
# this is similar to the master codelist in TRE
# For test, it includes HF and IS (bith with SNOMED and ICD-10 codes) and Diabetes with only SNOMED code.
# We will add icd-10 codes later.

masterdf = import_csv(spark_session=spark_pyspark, table_name="master_codelist.csv", path="../../../codelists/v_01",
                      databricks_import=False)
display(masterdf)
# masterdf.show(n=30)
# COMMAND ----------

display(masterdf.select(F.col("name")).distinct().orderBy("name"))
# COMMAND ----------

masterdf.filter(F.col("name") == "HF").select(F.col("terminology")).distinct().show()
# COMMAND ----------

masterdf.filter(F.col("name") == "diabetes").select(F.col("terminology")).distinct().show()

# COMMAND ----------
display(masterdf.filter(F.col("name") == "diabetes").filter(F.col("code_type") == 0))

# COMMAND ----------
masterdf.select(F.col("code_type")).distinct().show()

# COMMAND ---------- Diabetes Import ICD-10 only codes using copy and paste (open the csv in Visual Studio Code)
# Note: we need tab delimited file. Open the csv file in Excel, select the cells with data only, paste in a new .txt
# file in Visual Studio Code, then copy and paste in triple double-quotes.
text_input = """
name	terminology	code	term	code_type	RecordDate
diabetes	ICD10	E10	Insulin-dependent diabetes mellitus	1	20210127
diabetes	ICD10	E11	Non-insulin-dependent diabetes mellitus	1	20210127
diabetes	ICD10	E12	Malnutrition-related diabetes mellitus	1	20210127
diabetes	ICD10	O242	Diabetes mellitus in pregnancy: Pre-existing malnutrition-related diabetes mellitus	1	20210127
diabetes	ICD10	E13	Other specified diabetes mellitus	1	20210127
diabetes	ICD10	E14	Unspecified diabetes mellitus	1	20210127
diabetes	ICD10	G590	Diabetic mononeuropathy	1	20210127
diabetes	ICD10	G632	Diabetic polyneuropathy	1	20210127
diabetes	ICD10	H280	Diabetic cataract	1	20210127
diabetes	ICD10	H360	Diabetic retinopathy	1	20210127
diabetes	ICD10	M142	Diabetic athropathy	1	20210127
diabetes	ICD10	N083	Glomerular disorders in diabetes mellitus	1	20210127
diabetes	ICD10	O240	Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, insulin-dependent	1	20210127
diabetes	ICD10	O241	Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, non-insulin-dependent	1	20210127
diabetes	ICD10	O243	Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, unspecified	1	20210127
"""
diabetes_icd_codelist, header = cell_csv_import(text_input, drop_header=True, delimiter="\t", format="tre_masterlist")
# print(diabetes_icd_codelist)
# print(header)
diabetes_icd_codelist_df = list_to_pyspark_df(spark_pyspark, diabetes_icd_codelist, header)
display(diabetes_icd_codelist_df)

diabetes_codelist = masterdf.filter(F.col("name") == "diabetes")
diabetes_codelist = diabetes_codelist.union(diabetes_icd_codelist_df)
diabetes_codelist.groupBy(F.col("terminology")).count().show()
diabetes_codelist.select(F.col("name")).distinct().show()

# COMMAND ----------

# Params
param_yaml = """\
phenotype: diabetes
quality_control:
  start_date_qc: "1900-01-01"  
  end_date_qc: "2022-08-31"  # final_production date
  check_birth_death_limits: yes # Todo
gdppr:
  table_tag: gdppr
  pheno_pattern: code_based_diagnosis # Todo
  terminology: SNOMED
  code_type:
    - 0
    - 1
  limit_pheno_window: yes
  pheno_window_start: '2020-06-12'
  pheno_window_end: '2021-06-12'
  production_date_str: '2022-08-31'
  index_col: NHS_NUMBER_DEID
  evdt_col_raw: DATE
  evdt_col_list:
    - DATE
    - RECORD_DATE
  code_col: CODE
  evdt_pheno: gdppr_diabetes_evdt
final_table:
  table_tag: diabetes_phenotype
  index_col: NHS_NUMBER_DEID
  evdt_pheno: diabetes_evdt
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
# start_date_str = pyaml.get("quality_control").get("start_date_qc")
# end_date_str = pyaml.get("quality_control").get("end_date_qc")

# start_date = str_to_date(start_date_str)
# end_date = str_to_date(end_date_str)
# COMMAND ----------
# Add GDPPR
# display(gdppr_df)

# Todo qc of code type hit

# DFS, PS = make_dfset(gdppr_df, param_yaml, table_tag="gdppr")

diabetes_set = make_code_based_pheno(df_raw=gdppr_df,
                                     param_yaml=param_yaml,
                                     table_tag="gdppr",
                                     code_list=diabetes_codelist,
                                     terminology="SNOMED",
                                     code_type=[0, 1],
                                     pheno_window_start=datetime.datetime(2020, 6, 12),
                                     pheno_window_end=datetime.datetime(2022, 8, 31),
                                     limit_pheno_window=False)

display(diabetes_set.df_pheno_alpha)
display(diabetes_set.df_pheno_beta)

display(diabetes_set.first_eventdate_pheno())
display(diabetes_set.last_eventdate_pheno())
display(diabetes_set.last_eventdate_pheno(show_code=False, show_isin_flag=True))
display(diabetes_set.all_eventdates_pheno())
