# %%
"""This module contains classes and functions for creating DateBased"""
# %%
import pyspark.sql.functions as F
from pheno_package.nhsd_docker_pyspark_package.ParentPhenoClasses import PhenoTable
from pheno_package.parametrisation_package.NHSD_pheno_parametrisation import ParameterSet
from pheno_package.report_generator_package.PhenoReportGenerator import ReportGenerator


# %%
class DataFrameSet(PhenoTable):

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

    def make_first_eventdate_pheno(self, add_isin_flag=True):
        if self.pheno_df_full is not None:
            df_out = self.pheno_df_full.select(self.ps.index_col, F.array_min(F.col("list_distinct")).alias(
                f'''{self.ps.evdt_pheno}_first'''))
            if add_isin_flag:
                df_out = df_out.withColumn(self.isin_flag_col, F.lit(1))
            return df_out
        else:
            return None

    def make_last_eventdate_pheno(self, add_isin_flag=True):
        if self.pheno_df_full is not None:
            df_out = self.pheno_df_full.select(self.ps.index_col, F.array_max(F.col("list_distinct")).alias(
                f'''{self.ps.evdt_pheno}_last'''))
            if add_isin_flag:
                df_out = df_out.withColumn(self.isin_flag_col, F.lit(1))
            return df_out
        else:
            return None

    def make_distinct_eventdate_pheno(self, add_isin_flag=True):
        if self.pheno_df_full is not None:
            df_out = self.pheno_df_full.select(self.ps.index_col, F.explode(F.col("list_distinct")).alias(
                f'''{self.ps.evdt_pheno}_distinct'''))
            if add_isin_flag:
                df_out = df_out.withColumn(self.isin_flag_col, F.lit(1))
            return df_out
        else:
            return None

    def make_all_qc_eventdate_pheno(self, add_isin_flag=True):
        if self.pheno_df_full is not None:
            df_out = self.pheno_df_full.select(self.ps.index_col, F.explode(F.col("list_all")).alias(
                f'''{self.ps.evdt_pheno}_all'''))
            if add_isin_flag:
                df_out = df_out.withColumn(self.isin_flag_col, F.lit(1))
            return df_out
        else:
            return None
# %%
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
        DFS.df_valid, report_list, DFS.temp_df_datediff, DFS.temp_df_datediff_col_list = \
            DFS.multi_event_date_endpoint_correction(
                DFS.df_valid, PS.evdt_col_list, PS.evdt_pheno, PS.start_date_qc, PS.end_date_qc,
                full_report=PS.full_report)

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
