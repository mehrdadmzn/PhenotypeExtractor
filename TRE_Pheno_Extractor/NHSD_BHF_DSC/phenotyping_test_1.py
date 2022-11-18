import datetime

import findspark

# findspark.init()

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
import yaml
from NHSD_pheno_package.phenotype_extractor import pheno_functions_alpha
from TRE_Pheno_Extractor.NHSD_BHF_DSC.NHSD_pheno_package.phenotype_extractor.pheno_functions_alpha import \
    event_pheno_extractor

# df = SparkSession.read.format("csv").load("Fake_data/NHS_BHF_DSC/GDPPR.csv")
sc = pyspark.SparkContext()
sq = pyspark.SQLContext(sc)

# Load skinny table
skinny_df = sq.read.format("csv").option("header", "true").load("../../Fake_data/NHS_BHF_DSC/skinny.csv")

# Load gdppr
gdppr_df = sq.read.format("csv").option("header", "true").load("../../Fake_data/NHS_BHF_DSC/GDPPR.csv")

# Load sgss
sgss_df = sq.read.format("csv").option("header", "true").load("../../Fake_data/NHS_BHF_DSC/sgss.csv")

# Set dates
start_date_str = "2019-12-01"  # Start of the pandemic
end_date_str = "2022-08-31"  # final_production date
start_date = datetime.datetime(2019, 12, 1)
end_date = datetime.datetime(2022, 8, 31)

# Basic covid_df
covid_df_1 = skinny_df.select(["NHS_NUMBER_DEID"])
covid_df_1 = covid_df_1.withColumn("start_date", F.to_date(F.lit(start_date_str))).withColumn("end_date", F.to_date(
    F.lit(end_date_str)))
covid_df_1 = covid_df_1.withColumn("isin_skinny", F.lit(1))
covid_df_1.show()
# Params
param_yaml = """\
sgss:
  table_tag: sgss
  production_date_str: '2022-08-31'
  index_col: PERSON_ID_DEID
  evdt_col_raw: Specimen_Date
  evdt_col_list:
    - Specimen_Date
    - Lab_Report_Date
  evdt_pheno: sgss_evdt
  start_date_qc: "2019-12-01"
  end_date_qc: "2022-08-31"
optional_settings:
  full_report: yes
  spark_cache_midway: yes
  impute_multi_col_null_dates: yes
  impute_multi_col_invalid_dates: yes
  drop_null_ids: yes
  drop_remaining_null_dates: yes
  drop_remaining_invalid_dates: yes
"""
print(sgss_df.dtypes)
print(sgss_df.filter(F.col("PERSON_ID_DEID").isNull()).count())



# Add SGSS

sgss_dfset, sgss_ps= event_pheno_extractor(sgss_df, param_yaml, table_tag="sgss")
print('df_raw')
sgss_dfset.df_raw.show()
print('df_linkable')
sgss_dfset.df_linkable.show()
print('df_impute')
sgss_dfset.df_impute.show()
print('df_min_null')
sgss_dfset.df_min_null.show()
print('df_valid')
sgss_dfset.df_valid.show()
print('df_final')
sgss_dfset.df_final.show()

sgss_dfset.extract_basic_pheno_df()
sgss_dfset.pheno_df_basic.show()

sgss_dfset.extract_full_pheno_df()
sgss_dfset.pheno_df_full.show()
print(sgss_dfset.pheno_df_full.collect())
sgss_dfset.explode_array_col(sgss_dfset.pheno_df_full, "list_distinct", sgss_dfset.ps.index_col).show()

print(f'''start_date_qc = {sgss_ps.start_date_qc}''')
#print(f'''end_date_qc = {sgss_ps.end_date_qc}''')
