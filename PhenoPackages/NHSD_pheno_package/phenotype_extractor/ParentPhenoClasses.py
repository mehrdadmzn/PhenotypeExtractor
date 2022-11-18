"""This module contains parent classes for phenotype data collections"""
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

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



class PhenoTable:
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
    def explode_array_col(pheno_df_full, array_col, index_col, new_col_name):
        if pheno_df_full is not None:
            return pheno_df_full.select(index_col, F.explode(F.col(array_col)).alias(new_col_name))

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
