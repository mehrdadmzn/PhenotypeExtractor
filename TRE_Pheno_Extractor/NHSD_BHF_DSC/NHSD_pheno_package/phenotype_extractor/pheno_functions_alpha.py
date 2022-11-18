import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql import Row
import datetime
import seaborn as sns
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
        self.start_date_qc = pyaml.get(table_tag).get("start_date_qc")
        self.end_date_qc = pyaml.get(table_tag).get("end_date_qc")
        self.full_report = pyaml.get("optional_settings").get("full_report")
        self.spark_cache_midway = pyaml.get("optional_settings").get("spark_cache_midway")
        self.impute_multi_col_null_dates = pyaml.get("optional_settings").get("impute_multi_col_null_dates")
        self.impute_multi_col_invalid_dates = pyaml.get("optional_settings").get("impute_multi_col_invalid_dates")
        self.drop_null_ids = pyaml.get("optional_settings").get("drop_null_ids")
        self.drop_remaining_null_dates = pyaml.get("optional_settings").get("drop_remaining_null_dates")
        self.drop_remaining_invalid_dates = pyaml.get("optional_settings").get("drop_remaining_invalid_dates")


class EventPhenoTable:
    def __init__(self, parameter_object):
        self.ps = parameter_object
        # pheno DFs
        self.__pheno_df_basic = None
        self.__pheno_df_full = None
        self.__pheno_df_json = None
        self.__isin_flag_col = f'''isin_{self.ps.table_tag}'''

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

    def extract_basic_pheno_df(self, add_isin_flag=True):
        self.pheno_df_basic = first_eventdate_extractor(
            self.df_final.select([self.ps.index_col, self.ps.evdt_pheno]),
            index_col=self.ps.index_col, date_col=self.ps.evdt_pheno)

    def extract_full_pheno_df(self):
        self.pheno_df_full = concat_nonnull_eventdate_extractor(
            self.df_final.select([self.ps.index_col, self.ps.evdt_pheno]), index_col=self.ps.index_col,
            date_col=self.ps.evdt_pheno)

    @staticmethod
    def explode_array_col(pheno_df_full, array_col, index_col):
        if pheno_df_full is not None:
            return pheno_df_full.select(index_col, F.explode(F.col(array_col)))

    def return_long_event_df(self, list_events="distinct", add_isin_flag=True, include_nulls=False):
        df_long = None
        if list_events == "all":
            df_long = self.explode_array_col("list_all")
        elif list_events == "distinct":
            df_long = self.explode_array_col("list_distinct")
        else:
            pass

        if df_long is not None:
            df_long = df_long.withColumnRenamed("col", self.ps.evdt_pheno)
            if add_isin_flag:
                df_long = df_long.withColumn(self.__isin_flag_col, F.lit(1))
        return df_long


def first_eventdate_extractor(df: DataFrame, index_col, date_col):
    window_spec = Window.partitionBy(df[index_col]).orderBy(F.col(date_col).asc_nulls_last())
    df_rank = df.withColumn("rank_col", F.row_number().over(window_spec))
    df_out = df_rank.filter(F.col("rank_col") == 1).drop("rank_col")
    return (df_out)


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


class DataFrameSet(EventPhenoTable):

    def __init__(self, df_raw, production_date_str, parameter_object):
        super().__init__(parameter_object)
        self.RG = ReportGenerator(self.ps, self)
        self.production_date = production_date_str
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


class ReportGenerator:
    def __init__(self, parameter_object, dataframe_set):
        self.__ps = parameter_object
        self.__dfs = dataframe_set

        self.report_dic = {}

    def dataframe_set_initialisation(self):
        print(
            f'''##### Initialising {self.__ps.table_tag} table with production_date = {self.__ps.production_date_str}''')

    def initial_report(self):
        print(f'''###### full report is active? {self.__ps.full_report}''')
        print('****')

        if self.__ps.drop_null_ids:
            print(f'''****Note*** All null iDs WILL BE dropped.''')
        print(f'''Date endpoints are: start_date = {self.__ps.start_date_qc}, end_date = {self.__ps.end_date_qc}''')

        if self.__ps.impute_multi_col_null_dates:
            print(f'''Any null dates WILL BE imputed using alternative date columns''')
        else:
            print(f'''Null dates WILL NOT be imputed''')

        if self.__ps.impute_multi_col_invalid_dates:
            print(f'''Any out of range dates WILL BE imputed using alternative date columns''')
        else:
            print(f'''Our of range dates WILL NOT be imputed''')

        if self.__ps.drop_remaining_null_dates:
            print(f'''Any {"remaining" if self.__ps.impute_multi_col_null_dates else ""} null dates WILL BE dropped''')

        if self.__ps.drop_remaining_invalid_dates:
            print(
                f'''Any {"remaining" if self.__ps.impute_multi_col_invalid_dates else ""} out of range dates WILL BE dropped''')
        print("\n")

    @staticmethod
    def report_counts(df_in, df_str, col_in):
        print(f'''All rows count in {df_str} = {df_in.count()}''')
        print(f'''All distinct {col_in} count in {df_str} = {df_in.select(F.col(col_in)).distinct().count()}''')

    def report_counts_df_raw(self):
        self.report_counts(self.__dfs.df_raw, self.__dfs.str_df_raw, self.__ps.index_col)

    @staticmethod
    def count_null(df, col_in):
        df_in = df
        null_ids_df = df_in.filter(F.col(col_in).isNull())
        null_count = null_ids_df.count()
        return null_count, null_ids_df

    @staticmethod
    def count_null_str(df, col_in):
        df_in = df
        null_ids_df = df_in.filter((F.col(col_in).isNull()) |
                                   (F.col(col_in) == "null")
                                   | (F.col(col_in) == ""))
        null_count = null_ids_df.count()
        return null_count, null_ids_df

    @staticmethod
    def report_null_col(df, df_str, col_in, null_count):
        df_in = df
        print(f'''null {col_in} count of {df_str} = {null_count}''')
        print(f'''null {col_in} percentage of {df_str} = {null_count / df_in.count() * 100}''')

    @staticmethod
    def report_drops(null_count, df_str, col_in):
        print(f'''Dropped null in column {col_in} of {df_str} dataframe = {null_count}''')

    @staticmethod
    def report_invalid_dates(df, df_str, col_str, start_date: str, end_date: str):
        df_in = df
        print(f'''Event dates before {start_date} in {df_str} = {df_in.filter(F.col(col_str) < start_date).count()}''')
        print(f'''Event dattes after {end_date} in {df_str} = {df_in.filter(F.col(col_str) > end_date).count()}''')


def event_pheno_extractor(df_raw, param_yaml, table_tag):
    PS = ParameterSet(param_yaml, table_tag)
    DFS = DataFrameSet(df_raw, PS.production_date_str, PS)
    DFS.RG.dataframe_set_initialisation()

    DFS = DataFrameSet(df_raw, PS.production_date_str, PS)
    DFS.RG.initial_report()
    DFS.RG.report_counts_df_raw()

    print(f'''#### counting null IDs''')
    DFS.count_df_raw_null_id, DFS.temp_df_raw_null_id = DFS.RG.count_null(DFS.df_raw, PS.index_col)

    DFS.RG.report_null_col(DFS.df_raw, DFS.str_df_raw, PS.index_col, DFS.count_df_raw_null_id)

    DFS.df_linkable = DFS.df_raw
    if PS.drop_null_ids:
        if DFS.count_df_raw_null_id > 0:
            DFS.RG.report_drops(DFS.count_df_raw_null_id, DFS.str_df_linkable, PS.index_col)
            DFS.df_linkable = DFS.df_raw.filter(F.col(PS.index_col).isNotNull())


        else:
            print(f'''no null {PS.index_col} is found''')
    DFS.RG.report_counts(DFS.df_linkable, DFS.str_df_linkable, PS.index_col)

    # Todo add more reports

    DFS.df_sel = DFS.df_linkable
    DFS.df_sel = DFS.df_sel.select([PS.index_col] + PS.evdt_col_list)

    # Todo more reports
    print("df_impute")
    DFS.df_impute = DFS.df_sel.withColumn(PS.evdt_pheno, F.col(PS.evdt_col_raw))
    DFS.count_df_impute_null_id, DFS.temp_df_impute_null_id = DFS.RG.count_null(DFS.df_impute,
                                                                                PS.index_col)
    if PS.impute_multi_col_null_dates:
        DFS.df_impute, report_list = DFS.multi_event_date_null_handling(DFS.df_impute, PS.evdt_col_list, PS.evdt_pheno,
                                                                        full_report=PS.full_report)
        print(*report_list, sep="\n")

    print(f'''### Final null check''')
    DFS.RG.report_null_col(DFS.df_impute, DFS.str_df_impute, PS.evdt_pheno, DFS.count_df_impute_null_id)
    print("df_min_null")
    DFS.df_min_null = DFS.df_impute
    report_list = []
    if PS.drop_remaining_null_dates:
        DFS.df_min_null, report_list = DFS.drop_remaining_null_dates(DFS.df_impute, PS.index_col, PS.evdt_pheno,
                                                                     PS.full_report)
    print(*report_list, sep="\n")
    print(f'''\n###### check dates that are less than {PS.start_date_qc} and larget than {PS.end_date_qc}''')
    DFS.RG.report_invalid_dates(DFS.df_min_null, DFS.str_df_min_null, PS.evdt_pheno, PS.start_date_qc, PS.end_date_qc)

    # Cache
    if PS.spark_cache_midway:
        print(f'''\n Chaching df_min_null''')
        DFS.df_min_null.cache()

    # df valid
    # Todo more report
    print("df_valid")
    DFS.df_valid = DFS.df_min_null
    if PS.impute_multi_col_invalid_dates:
        print(f'''#### out-of-range dates are being corrected''')
        DFS.df_valid, report_list, DFS.temp_df_datediff, DFS.temp_df_datediff_col_list = DFS.multi_event_date_endpoint_correction(
            DFS.df_valid, PS.evdt_col_list, PS.evdt_pheno, PS.start_date_qc, PS.end_date_qc, full_report=PS.full_report)

        print(*report_list, sep="\n")

    # Todo box plot report
    print(f'''final check on invalid dates''')
    DFS.RG.report_invalid_dates(DFS.df_valid, DFS.str_df_valid, PS.evdt_pheno, PS.start_date_qc, PS.end_date_qc)
    print(f'''Making df_final''')
    report_list = []
    DFS.df_final = DFS.df_valid
    if PS.drop_remaining_invalid_dates:
        print(f''''### Invalid dates will be dropped ''')
        DFS.df_final, report_list = DFS.drop_remaining_invalid_dates(DFS.df_final, PS.index_col, PS.evdt_pheno,
                                                                     PS.start_date_qc, PS.end_date_qc,
                                                                     full_report=PS.full_report)
    print(*report_list, sep="\n")
    print("Done!")
    return DFS, PS
