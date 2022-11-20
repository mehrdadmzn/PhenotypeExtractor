# %%
# """This module contains functions and APIs for one-line running of phenotype extraction"""
from pyspark.sql import DataFrame

from pheno_package.nhsd_docker_pyspark_package.DateBasedPhenoFunctions import event_pheno_extractor, DataFrameSet


# %%
# Date-based covid phenotype extractor


def covid_pheno_date_based(df: DataFrame, param_yaml: str, table_tag: str, distinct_dates=True):
    dfset, ps = event_pheno_extractor(df, param_yaml, table_tag=table_tag)
    dfset.extract_full_pheno_df()
    pheno_full_long = dfset.explode_array_col(dfset.pheno_df_full, ("list_distinct" if distinct_dates else "list_all"),
                                              dfset.ps.index_col,
                                              dfset.ps.evdt_pheno)
    #dfset.extract_basic_pheno_df()
    #pheno_basic_long = dfset.pheno_df_basic
    return dfset, pheno_full_long


def show_dfset_dfs(dfset):
    """ Do not run in TRE. This is for test in simulated environment

    Args:
        dfset: DataFrameSet object

    Returns:
        None

    """
    print('df_raw')
    dfset.df_raw.show()
    print('df_linkable')
    dfset.df_linkable.show()
    print('df_impute')
    dfset.df_impute.show()
    print('df_min_null')
    dfset.df_min_null.show()
    print('df_valid')
    dfset.df_valid.show()
    print('df_final')
    dfset.df_final.show()
