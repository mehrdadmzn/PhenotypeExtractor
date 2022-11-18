import pyspark.sql.functions as F


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
