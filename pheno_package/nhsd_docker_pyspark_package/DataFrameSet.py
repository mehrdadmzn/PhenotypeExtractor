# COMMAND ----------

"""This module contains classes and functions for creating DateBased"""
import datetime

# COMMAND ----------
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql import Window

from pheno_package.parametrisation_package.NHSD_pheno_parametrisation import ParameterSet
from pheno_package.report_generator_package.PhenoReportGenerator import ReportGenerator


# COMMAND ----------


# COMMAND ----------
class DataFrameSet:

    def __init__(self, df_raw, parameter_object):
        self.ps = parameter_object
        self.RG = ReportGenerator(self.ps, self)
        self.production_date = self.ps.production_date_str
        self.__df_raw = df_raw

        self.str_df_raw = "df_raw"
        self.df_linkable = None
        self.str_df_linkable = "df_linkable"
        self.df_sel = None
        self.str_df_sel = "df_sel"
        self.__df_impute = None
        self.str_df_impute = "df_impute"
        self.df_min_null = None
        self.str_df_min_null = "df_min_null"
        self.df_valid = None
        self.str_df_valid = "df_valid"
        self.df_final = None
        self.str_df_final = "df_final"

        # Intermediate DFs

        self.temp_df_raw_null_id = None
        self.temp_df_impute_null_id = None
        self.temp_df_datediff = None

        # Intermediate lists
        self.temp_df_datediff_col_list = []

        # null value reports
        self.count_df_raw_null_id = 0
        self.count_df_impute_null_id = 0

    @property
    def df_raw(self):
        return self.__df_raw

    @df_raw.setter
    def df_raw(self, df_raw):
        self.__df_raw = df_raw

    @property
    def df_impute(self):
        return self.__df_impute

    @df_impute.setter
    def df_impute(self, df_impute):
        self.__df_impute = df_impute

    @staticmethod
    def multi_event_date_null_handling(df, evdt_col_list, new_col_name, full_report=True):
        report_list = []
        df_out = df
        for col_name in evdt_col_list[1:]:
            if full_report:
                count_null = df_out.filter((F.col(col_name).isNotNull()) & (F.col(new_col_name).isNull())).count()
                if count_null > 0:
                    report_list.append(
                        f'''{count_null} null {new_col_name} values replaced with non-null values of {col_name}''')
            df_out = df_out.withColumn(new_col_name,
                                       F.when(((F.col(col_name).isNotNull()) & (F.col(new_col_name).isNull())),
                                              F.col(col_name)).otherwise(F.col(new_col_name)))
        return df_out, report_list

    @staticmethod
    def drop_remaining_null_dates(df, index_col, new_evdt_col, full_report=True):
        report_list = []
        df_out = df
        df_invalid = df_out.filter(F.col(new_evdt_col).isNull())
        if full_report:
            invalid_count = df_invalid.count()
            report_list.append(f'''Remaining {invalid_count} records with null dates are dropped.''')
        # df_out = df_out.join(df_invalid, on=[index_col], how="left_anti")
        df_out = df_out.filter(F.col(new_evdt_col).isNotNull())
        return df_out, report_list

    @staticmethod
    def multi_event_date_endpoint_correction(df, evdt_col_list, new_col_name, start_date, end_date,
                                             full_report=True):
        report_list = []
        diff_cols_list = []
        df_datediff = df
        df_out = df
        # To avoid changing the main new_col_nme in the loop
        temp_col_list = []
        for index, item in enumerate(evdt_col_list):
            temp_col_list.append(f'''{item}_{index}''')
        df_out = df_out.withColumn(temp_col_list[0], F.col(new_col_name))

        for col_index, col_name in enumerate(evdt_col_list):
            count_invalid = df_out.filter((
                    ((F.col(temp_col_list[col_index]) < start_date) | (F.col(temp_col_list[col_index]) > end_date)) &
                    ((F.col(col_name) >= start_date) & (F.col(col_name) <= end_date))
            )).count()

            if count_invalid > 0:
                report_list.append(f'''{count_invalid} invalid {new_col_name} values will be replaced with valid 
                dates of {col_name}''')
            df_out = df_out.withColumn(temp_col_list[col_index], F.when((((F.col(
                temp_col_list[col_index]) < start_date) | (F.col(temp_col_list[col_index]) > end_date)) & (
                                                                                 (F.col(col_name) >= start_date) & (
                                                                                 F.col(col_name) <= end_date))),
                                                                        F.col(col_name)).otherwise(
                F.col(temp_col_list[col_index])))

            if col_index + 1 < len(evdt_col_list):
                df_out = df_out.withColumn(temp_col_list[col_index + 1], F.col(temp_col_list[col_index]))
            else:
                df_out = df_out.withColumn(new_col_name, F.col(temp_col_list[col_index]))
        for index, item in enumerate(evdt_col_list):
            df_out = df_out.drop(temp_col_list[index])

        if full_report:
            for col_name in evdt_col_list:
                diff_col = f'''datediff_{col_name}_and_{new_col_name}'''
                df_datediff = df_datediff.withColumn(diff_col, F.datediff(new_col_name, col_name))
                diff_cols_list.append(diff_col)
        return df_out, report_list, df_datediff, diff_cols_list

    @staticmethod
    def drop_remaining_invalid_dates(df, index_col, new_evdt_col, start_date, end_date, full_report):
        report_list = []
        df_out = df
        df_invalid = df_out.filter((F.col(new_evdt_col) < start_date) | (F.col(new_evdt_col) > end_date))
        if full_report:
            invalid_count = df_invalid.count()
            report_list.append(f'''Remaining {invalid_count} records with invalid dates are dropped.''')
        # df_out = df.join(df_invalid, on = [index_col], how = "left_anti")

        df_out = df_out.filter((F.col(new_evdt_col) >= start_date) & (F.col(new_evdt_col) <= end_date))
        return df_out, report_list

    def cleaning_and_report(self):

        self.RG.dataframe_set_initialisation()
        self.RG.initial_report()
        self.RG.report_counts_df_raw()

        print(f'''#### counting null IDs''')
        self.count_df_raw_null_id, self.temp_df_raw_null_id = self.RG.count_null(self.df_raw, self.ps.index_col)

        self.RG.report_null_col(self.df_raw, self.str_df_raw, self.ps.index_col, self.count_df_raw_null_id)

        self.df_linkable = self.df_raw
        if self.ps.drop_null_ids:
            if self.count_df_raw_null_id > 0:
                self.RG.report_drops(self.count_df_raw_null_id, self.str_df_linkable, self.ps.index_col)
                self.df_linkable = self.df_raw.filter(F.col(self.ps.index_col).isNotNull())


            else:
                print(f'''no null {self.ps.index_col} is found''')
        self.RG.report_counts(self.df_linkable, self.str_df_linkable, self.ps.index_col)

        # Todo add more reports

        self.df_sel = self.df_linkable
        code_col_list = []
        if self.ps.code_col is not None:
            code_col_list.append(self.ps.code_col)
        self.df_sel = self.df_sel.select([self.ps.index_col] + self.ps.evdt_col_list + code_col_list)

        # Todo more reports
        print("Making df_impute: Initial imputation of null IDs and null event dates")
        self.df_impute = self.df_sel.withColumn(self.ps.evdt_pheno, F.col(self.ps.evdt_col_raw))
        self.count_df_impute_null_id, self.temp_df_impute_null_id = self.RG.count_null(self.df_impute,
                                                                                       self.ps.index_col)
        if self.ps.impute_multi_col_null_dates:
            self.df_impute, report_list = self.multi_event_date_null_handling(self.df_impute, self.ps.evdt_col_list,
                                                                              self.ps.evdt_pheno,
                                                                              full_report=self.ps.full_report)
            print(*report_list, sep="\n")

        print(f'''### Final null check''')
        self.RG.report_null_col(self.df_impute, self.str_df_impute, self.ps.evdt_pheno, self.count_df_impute_null_id)
        print("Making df_min_null: The dataframe with minimum number of remained null event dates")
        self.df_min_null = self.df_impute
        report_list = []
        if self.ps.drop_remaining_null_dates:
            self.df_min_null, report_list = self.drop_remaining_null_dates(self.df_impute, self.ps.index_col,
                                                                           self.ps.evdt_pheno,
                                                                           self.ps.full_report)
        print(*report_list, sep="\n")
        print(
            f'''\n###### check dates that are less than {self.ps.start_date_qc} and larget than {self.ps.end_date_qc}''')
        self.RG.report_invalid_dates(self.df_min_null, self.str_df_min_null, self.ps.evdt_pheno, self.ps.start_date_qc,
                                     self.ps.end_date_qc)

        # Cache
        if self.ps.spark_cache_midway:
            print(f'''\n Chaching df_min_null''')
            self.df_min_null.cache()

        # df valid
        # Todo more report
        print(
            "Making df_valid: A dataframe with invalid event dates replaced with valid valus from alternative date columns")
        self.df_valid = self.df_min_null
        if self.ps.impute_multi_col_invalid_dates:
            print(f'''#### out-of-range dates are being corrected''')
            self.df_valid, report_list, self.temp_df_datediff, self.temp_df_datediff_col_list = \
                self.multi_event_date_endpoint_correction(
                    self.df_valid, self.ps.evdt_col_list, self.ps.evdt_pheno, self.ps.start_date_qc,
                    self.ps.end_date_qc,
                    full_report=self.ps.full_report)

            print(*report_list, sep="\n")

        # Todo box plot report
        print(f'''final check on invalid dates''')
        self.RG.report_invalid_dates(self.df_valid, self.str_df_valid, self.ps.evdt_pheno, self.ps.start_date_qc,
                                     self.ps.end_date_qc)
        print(f'''Making df_final: The final clean table ''')
        report_list = []
        self.df_final = self.df_valid
        if self.ps.drop_remaining_invalid_dates:
            print(f''''### Invalid dates will be dropped ''')
            self.df_final, report_list = self.drop_remaining_invalid_dates(self.df_final, self.ps.index_col,
                                                                           self.ps.evdt_pheno,
                                                                           self.ps.start_date_qc, self.ps.end_date_qc,
                                                                           full_report=self.ps.full_report)
        print(*report_list, sep="\n")
        print("Initialisation done!")
        return self, self.ps

    def display_tables(self):
        pass


class PhenoTableSet(DataFrameSet):
    def __init__(self, df_raw: DataFrame, parameter_object: ParameterSet):
        super().__init__(df_raw, parameter_object)
        # pheno DFs
        self.df_pheno_mixed = None
        self.df_pheno_flag = None
        self.df_pheno_alpha = None
        self.df_pheno_beta = None
        self.__pheno_df_basic = None
        self.__pheno_df_full = None
        self.__pheno_df_json = None
        self.isin_flag_col = f'''isin_{self.ps.table_tag}'''

    @property
    def pheno_df_basic(self):
        return self.__pheno_df_basic

    @property
    def pheno_df_full(self):
        return self.__pheno_df_full

    @property
    def pheno_df_json(self):
        return self.__pheno_df_json

    @pheno_df_basic.setter
    def pheno_df_basic(self, saved_basic_df):
        self.__pheno_df_basic = saved_basic_df

    @pheno_df_full.setter
    def pheno_df_full(self, saved_full_df):
        self.__pheno_df_full = saved_full_df

    @pheno_df_json.setter
    def pheno_df_json(self, saved_json_df):
        self.__pheno_df_json = saved_json_df

    def code_based_pheno(self, code_list: list, terminology: str, code_type: list,
                         pheno_window_start: datetime.datetime,
                         pheno_window_end: datetime.datetime, limit_pheno_window: bool):
        # Todo generalise to non-BHF code-list structures
        # Todo test: check if the type of the pheno_pattern is code_based_diagnosis. also that code_column
        temp_codelist = code_list.filter(F.lower(F.col("terminology")) == str(terminology).lower()). \
            filter(F.col("code_type").isin(code_type))
        # keep only rows with the codes in the codelist. also make a column indicating this
        codelist_pandas = temp_codelist.select(F.col("code")).toPandas()["code"]
        codelist_list = list(map(lambda x: str(x), codelist_pandas))

        print("Making df_pheno_mixed: df_final plust with code and code_type columns from the codelist ")
        self.df_pheno_mixed = self.df_final.join(
            code_list.select("code", "code_type").withColumnRenamed("code", self.ps.code_col),
            on=[self.ps.code_col],
            how="left")

        print(
            "Making df_pheno_flag: New flags are added: code_hit indicates the row is a candidate phenotype. code_type indicates the rows with the same values in code_type parameter  ")
        self.df_pheno_flag = self.df_pheno_mixed.withColumn("code_hit",
                                                            F.when(F.col(self.ps.code_col).cast("string").isin(
                                                                codelist_list),
                                                                F.lit(1)).otherwise(
                                                                F.lit(0)))
        self.df_pheno_flag = self.df_pheno_flag.withColumn("code_type_hit",
                                                           F.when(F.col("code_type").cast("string").isin(
                                                               list(map(str, code_type))),
                                                               F.lit(1)).otherwise(F.lit(0)))
        print(
            "Making df_pheno_alpha: The dataframe with time window applied. If limit_pheno_window = True, the event dates will be limited to a time window from (and including) pheno_window_start to (and including) pheno_window_end ")

        self.df_pheno_alpha = self.df_pheno_flag.filter(F.col("code_hit") == 1).filter(F.col("code_type_hit") == 1)
        if limit_pheno_window:
            self.df_pheno_alpha = self.df_pheno_alpha.filter(
                (F.col(self.ps.evdt_pheno) >= pheno_window_start) & (F.col(self.ps.evdt_pheno) <= pheno_window_end))
        print(
            "making df_pheno_beta: The dataframe with code_hit and code_type_hit applied and only with  index_col, code, eventdate, and isin_flag")
        self.df_pheno_beta = self.df_pheno_alpha.filter(F.col("code_hit") == 1).filter(F.col("code_type_hit") == 1)
        self.df_pheno_beta = self.df_pheno_beta.select([self.ps.index_col, self.ps.evdt_pheno, self.ps.code_col])
        self.df_pheno_beta = self.df_pheno_beta.withColumn(self.isin_flag_col, F.lit(1))

    @staticmethod
    def handle_code_and_isin_flag(df_in, show_code, show_isin_flag, code_col, isin_flag_col):
        df_out = df_in
        if not show_code:
            df_out = df_out.drop(code_col)
        if not show_isin_flag:
            df_out = df_out.drop(isin_flag_col)
        return df_out

    def first_eventdate_pheno(self, show_code=True, show_isin_flag=True) -> DataFrame:
        df = first_eventdate_extractor(self.df_pheno_beta, self.ps.index_col, self.ps.evdt_pheno)
        df_out = self.handle_code_and_isin_flag(df, show_code, show_isin_flag, self.ps.code_col, self.isin_flag_col)
        return df_out

    def last_eventdate_pheno(self, show_code=True, show_isin_flag=True) -> DataFrame:
        df = last_eventdate_extractor(self.df_pheno_beta, self.ps.index_col, self.ps.evdt_pheno)
        df_out = self.handle_code_and_isin_flag(df, show_code, show_isin_flag, self.ps.code_col, self.isin_flag_col)
        return df_out

    def all_eventdates_pheno(self, show_code=True, show_isin_flag=True) -> DataFrame:
        df = self.df_pheno_beta
        df_out = self.handle_code_and_isin_flag(df, show_code, show_isin_flag, self.ps.code_col, self.isin_flag_col)
        return df_out

    # COMMAND ----------


def make_dfset(df_raw, param_yaml, table_tag):
    pset = ParameterSet(param_yaml, table_tag)
    tset = PhenoTableSet(df_raw, pset)
    tset.cleaning_and_report()

    return tset


def make_code_based_pheno(df_raw, param_yaml, table_tag, code_list: list, terminology: str, code_type: list,
                          pheno_window_start: datetime.datetime,
                          pheno_window_end: datetime.datetime, limit_pheno_window: bool) -> PhenoTableSet:
    tset = make_dfset(df_raw, param_yaml, table_tag)
    tset.code_based_pheno(code_list, terminology, code_type,
                          pheno_window_start,
                          pheno_window_end, limit_pheno_window)

    return tset


# COMMAND ----------
# Previously in PhenoExtractionHelperfunction

def first_eventdate_extractor(df: DataFrame, index_col, date_col):
    window_spec = Window.partitionBy(df[index_col]).orderBy(F.col(date_col).asc_nulls_last())
    df_rank = df.withColumn("rank_col", F.row_number().over(window_spec))
    df_out = df_rank.filter(F.col("rank_col") == 1).drop("rank_col")
    return df_out


def last_eventdate_extractor(df: DataFrame, index_col, date_col):
    window_spec = Window.partitionBy(df[index_col]).orderBy(F.col(date_col).desc_nulls_last())
    df_rank = df.withColumn("rank_col", F.row_number().over(window_spec))
    df_out = df_rank.filter(F.col("rank_col") == 1).drop("rank_col")
    return df_out


def concat_nonnull_eventdate_extractor(df: DataFrame, index_col, date_col):
    window_spec = Window.partitionBy(df[index_col]).orderBy(F.col(date_col).asc_nulls_last())
    df_out = df.withColumn("rank_col", F.row_number().over(window_spec))
    df_out = df_out.withColumn("list_all", F.collect_list(date_col).over(window_spec))
    df_first_infection = df_out.filter(F.col("rank_col") == 1).drop("rank_col")
    df_out = df_out.withColumn("list_all", F.array_sort("list_all"))
    window_rank = Window.partitionBy(df_out[index_col]).orderBy(F.col("rank_col").desc_nulls_last())
    df_out = df_out.withColumn("keep_rank", F.row_number().over(window_rank))
    df_out = df_out.filter(F.col("keep_rank") == 1)
    # df_out = df_out.withColumn("count_null", F.col("rank_col") - F.size(F.col("list_all")))
    df_out = df_out.withColumn("list_distinct", F.array_distinct(F.col("list_all")))
    df_out = df_out.withColumn("count_distinct", F.size(F.col("list_distinct")))
    df_out = df_out.withColumnRenamed("rank_col", "count_all").drop("keep_rank").drop(date_col)

    return df_out


def full_long_wide_eventdate_extractor(df: DataFrame, index_col, date_col):
    window_rank_asc = Window.partitionBy(df[index_col]).orderBy(F.col(date_col).asc_nulls_last())
    df_out = df.withColumn("rank_asc", F.row_number().over(window_rank_asc))
    window_rank_desc = Window.partitionBy(df[index_col]).orderBy(F.col(date_col).desc_nulls_last())
    df_out = df_out.withColumn("rank_desc", F.row_number().over(window_rank_desc))
    return df_out


def extract_basic_pheno_df(ptClass: PhenoTableSet, add_isin_flag=True):
    ptClass.pheno_df_basic = first_eventdate_extractor(
        ptClass.df_final.select([ptClass.ps.index_col, ptClass.ps.evdt_pheno]),
        index_col=ptClass.ps.index_col, date_col=ptClass.ps.evdt_pheno)


def extract_full_pheno_df(ptClass: PhenoTableSet):
    ptClass.pheno_df_full = concat_nonnull_eventdate_extractor(
        ptClass.df_final.select([ptClass.ps.index_col, ptClass.ps.evdt_pheno]), index_col=ptClass.ps.index_col,
        date_col=ptClass.ps.evdt_pheno)


def explode_array_col(pheno_df_full, array_col, index_col, new_col_name):
    if pheno_df_full is not None:
        return pheno_df_full.select(index_col, F.explode(F.col(array_col)).alias(new_col_name))


def return_long_event_df(ptClass: PhenoTableSet, list_events="distinct", add_isin_flag=True, include_nulls=False):
    df_long = None
    if list_events == "all":
        df_long = ptClass.explode_array_col("list_all")
    elif list_events == "distinct":
        df_long = ptClass.explode_array_col("list_distinct")
    else:
        pass

    if df_long is not None:
        df_long = df_long.withColumnRenamed("col", ptClass.ps.evdt_pheno)
        if add_isin_flag:
            df_long = df_long.withColumn(ptClass.isin_flag_col, F.lit(1))
    return df_long


# COMMAND ----------
def make_first_eventdate_pheno(dfset: DataFrameSet, add_isin_flag=True):
    if dfset.pheno_df_full is not None:
        df_out = dfset.pheno_df_full.select(dfset.ps.index_col, F.array_min(F.col("list_distinct")).alias(
            f'''{dfset.ps.evdt_pheno}_first'''))
        if add_isin_flag:
            df_out = df_out.withColumn(dfset.isin_flag_col, F.lit(1))
        return df_out
    else:
        return None


def make_last_eventdate_pheno(dfset, add_isin_flag=True):
    if dfset.pheno_df_full is not None:
        df_out = dfset.pheno_df_full.select(dfset.ps.index_col, F.array_max(F.col("list_distinct")).alias(
            f'''{dfset.ps.evdt_pheno}_last'''))
        if add_isin_flag:
            df_out = df_out.withColumn(dfset.isin_flag_col, F.lit(1))
        return df_out
    else:
        return None


def make_distinct_eventdate_pheno(dfset, add_isin_flag=True):
    if dfset.pheno_df_full is not None:
        df_out = dfset.pheno_df_full.select(dfset.ps.index_col, F.explode(F.col("list_distinct")).alias(
            f'''{dfset.ps.evdt_pheno}_distinct'''))
        if add_isin_flag:
            df_out = df_out.withColumn(dfset.isin_flag_col, F.lit(1))
        return df_out
    else:
        return None


def make_all_qc_eventdate_pheno(dfset, add_isin_flag=True):
    if dfset.pheno_df_full is not None:
        df_out = dfset.pheno_df_full.select(dfset.ps.index_col, F.explode(F.col("list_all")).alias(
            f'''{dfset.ps.evdt_pheno}_all'''))
        if add_isin_flag:
            df_out = df_out.withColumn(dfset.isin_flag_col, F.lit(1))
        return df_out
    else:
        return None

# COMMAND ----------
