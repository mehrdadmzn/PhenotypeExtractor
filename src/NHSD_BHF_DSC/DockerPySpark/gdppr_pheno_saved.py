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
from pheno_package.nhsd_docker_pyspark_package.DataFrameSet import PhenoTableSetDateBased, PhenoTableSetGdppr, \
    PhenoTableSetHesApc
from pheno_package.nhsd_docker_pyspark_package.FacadeFunctions import make_code_base_pheno
from pheno_package.pyspark_databricks_interface.DockerPysparkToDatabricks import display

# COMMAND ----------

spark_pyspark = SparkSession.builder.master("local[1]").appName("NHS_TRE_Simulation").getOrCreate()

# Load skinny table
skinny_df = import_csv(spark_session=spark_pyspark, table_name="skinny.csv", path="../../../fake_data/NHSD_BHF_DSC",
                       databricks_import=False)

# Load gdppr
gdppr_df = import_csv(spark_session=spark_pyspark, table_name="GDPPR.csv", path="../../../fake_data/NHSD_BHF_DSC",
                      databricks_import=False)
# Load gdppr
hes_apc_df = import_csv(spark_session=spark_pyspark, table_name="hes_apc.csv", path="../../../fake_data/NHSD_BHF_DSC",
                        databricks_import=False)
# COMMAND ----------
# Check master code list
# this is similar to the master codelist in TRE
# For test, it includes HF and IS (bith with SNOMED and ICD-10 codes) and Diabetes with only SNOMED code.
# We will add icd-10 codes later.

masterdf = import_csv(spark_session=spark_pyspark, table_name="master_codelist.csv", path="../../../codelists/v_01",
                      databricks_import=False)
# display(masterdf)
# masterdf.show(n=30)
# COMMAND ----------

# display(masterdf.select(F.col("name")).distinct().orderBy("name"))
# COMMAND ----------

# masterdf.filter(F.col("name") == "HF").select(F.col("terminology")).distinct().show()
# COMMAND ----------

# masterdf.filter(F.col("name") == "diabetes").select(F.col("terminology")).distinct().show()

# COMMAND ----------
# display(masterdf.filter(F.col("name") == "diabetes").filter(F.col("code_type") == 0))

# COMMAND ----------
# masterdf.select(F.col("code_type")).distinct().show()

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
diabetes	ICD10	O240	Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, insulin-dependent	0	20210127
diabetes	ICD10	O241	Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, non-insulin-dependent	0	20210127
diabetes	ICD10	O243	Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, unspecified	0	20210127
"""
diabetes_icd_codelist, header = cell_csv_import(text_input, drop_header=True, delimiter="\t", format="tre_masterlist")
# print(diabetes_icd_codelist)
# print(header)
diabetes_icd_codelist_df = list_to_pyspark_df(spark_pyspark, diabetes_icd_codelist, header)
# display(diabetes_icd_codelist_df)

diabetes_codelist = masterdf.filter(F.col("name") == "diabetes")
diabetes_codelist = diabetes_codelist.union(diabetes_icd_codelist_df)
# diabetes_codelist.groupBy(F.col("terminology")).count().show()
# diabetes_codelist.select(F.col("name")).distinct().show()

# COMMAND ----------

# Params
gdppr_diabetes_yaml = """\
phenotype_name: diabetes
table_tag: gdppr
codelist_format: bhf_tre
pheno_details:
  evdt_pheno: gdppr_diabetes_evdt
  pheno_pattern: code_based_diagnosis # Todo
  terminology: SNOMED
  check_code_type: no
  code_type: both
  limit_pheno_window: no # if set to yes, the following two optins must be set
  pheno_window_start: '1900-06-12'
  pheno_window_end: '2021-06-12'
table_details:
  table_tag: gdppr
  index_col: NHS_NUMBER_DEID
  evdt_col_raw: DATE
  evdt_col_list:
    - DATE
    - RECORD_DATE
  code_col: CODE
  production_date_str: '2022-08-31'
gdppr_specific:
  swap_date_larger_than_record_date: no # Todo 
quality_control:
  # Time window for event date quality check. Any dates before or after this window
  # must be excluded for quality assurance.
  start_date_qc: "1900-01-01" # time window for event date quality check.
  end_date_qc: "2022-08-31"  # final_production date
  # valid table tag: gdppr, hes_apc, sgss, chess, pillar2
  # valid pheno_pattern: code_based_diagnosis, date_based
  # valid phenotype_codelist_format: bhf_tre, hdruk
  # valid data type: yyyy-mm-dd
optional_settings:
  full_report: yes
  spark_cache_midway: no
  impute_multi_col_null_dates: yes
  impute_multi_col_invalid_dates: yes
  drop_null_ids: yes
  drop_remaining_null_dates: yes
  drop_remaining_invalid_dates: yes
"""
gdppr_diabetes_settings = yaml.load(gdppr_diabetes_yaml, Loader=yaml.SafeLoader)
# COMMAND ----------

# diabetes_set_1 = make_code_base_pheno(df_raw=gdppr_df, table_tag="gdppr",
#                                     param_yaml=gdppr_diabetes_yaml, codelist_df=diabetes_codelist,
#                                     list_extra_cols_to_keep=["details"])

# display(diabetes_set_1.df_sel)

# display(diabetes_set_1.df_final)

# display(diabetes_set_1.df_pheno_alpha)
# display(diabetes_set_1.df_pheno_beta)

# display(diabetes_set_1.first_eventdate_pheno())
# display(diabetes_set_1.last_eventdate_pheno())
# display(diabetes_set_1.last_eventdate_pheno(show_code=False, show_isin_flag=True))
# display(diabetes_set_1.all_eventdates_pheno())
# Test of saving, loadig,and phenotyping
# print("try saving and loading")
# df_pandas = diabetes_set_1.df_final.toPandas()
# df_pandas.to_csv("df_pandas.csv", index=False)
df_pandas_loaded = import_csv(spark_session=spark_pyspark, table_name="df_pandas.csv",
                              path="../../../fake_data/NHSD_BHF_DSC",
                              databricks_import=False)

diabetes_set_1 = make_code_base_pheno(df_in=df_pandas_loaded, table_tag="gdppr",
                                      param_yaml=gdppr_diabetes_yaml, codelist_df=diabetes_codelist,
                                      list_extra_cols_to_keep=["details"], pre_cleaned=True)
# display(diabetes_set_1.df_sel)

display(diabetes_set_1.df_final)

display(diabetes_set_1.df_pheno_alpha)
display(diabetes_set_1.df_pheno_beta)

display(diabetes_set_1.first_eventdate_pheno())
display(diabetes_set_1.last_eventdate_pheno())
display(diabetes_set_1.last_eventdate_pheno(show_code=False, show_isin_flag=True))
display(diabetes_set_1.all_eventdates_pheno())
