import datetime

# findspark.init()

import pyspark
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from PhenoPackages.NHSD_pheno_package.phenotype_extractor.local_only.local_simulated_functions import \
    show_all_dfs
from PhenoPackages.NHSD_pheno_package.phenotype_extractor.DateBasedPhenoFunctions import \
    event_pheno_extractor

# df = SparkSession.read.format("csv").load("Fake_data/NHS_BHF_DSC/GDPPR.csv")
#sc = pyspark.SparkContext()
#sq = pyspark.SQLContext(sc)
spark = SparkSession.builder.master("local[1]").appName("NHS_TRE_Simulation").getOrCreate()
# Load skinny table
skinny_df = spark.read.format("csv").option("header", "true").load("../../Fake_data/NHS_BHF_DSC/skinny.csv")

# Load gdppr
gdppr_df = spark.read.format("csv").option("header", "true").load("../../Fake_data/NHS_BHF_DSC/GDPPR.csv")

# Load sgss
sgss_df = spark.read.format("csv").option("header", "true").load("../../Fake_data/NHS_BHF_DSC/sgss.csv")

# Set dates
#start_date_str = "2019-12-01"  # Start of the pandemic
#end_date_str = "2022-08-31"  # final_production date
#start_date = datetime.datetime(2019, 12, 1)
#end_date = datetime.datetime(2022, 8, 31)

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
final_table:
  table_tag: covid
  index_col: NHS_NUMBER_DEID
  evdt_pheno: covid_evdt
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




sgss_dfset.extract_basic_pheno_df()
sgss_dfset.pheno_df_basic.show()

sgss_dfset.extract_full_pheno_df()
sgss_dfset.pheno_df_full.show()
print(sgss_dfset.pheno_df_full.collect())
show_all_dfs(sgss_dfset)
sgss_dfset.explode_array_col(sgss_dfset.pheno_df_full, "list_distinct", sgss_dfset.ps.index_col, sgss_dfset.ps.evdt_pheno).show()
#sgss_dfset.explode_array_col(sgss_dfset.pheno_df_full, "list_all", sgss_dfset.ps.index_col).show()

print(f'''start_date_qc = {sgss_ps.start_date_qc}''')
#print(f'''end_date_qc = {sgss_ps.end_date_qc}''')
