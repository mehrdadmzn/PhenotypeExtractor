# %%
# """This module contains functions and APIs for one-line running of phenotype extraction"""
from pyspark.sql import DataFrame

from pheno_package.nhsd_docker_pyspark_package.DataFrameSet import DataFrameSet, PhenoTableSetGdppr, \
    PhenoTableSetHesApc, PhenoTableSet, PhenoTableSetDateBased


# %%
# Date-based covid phenotype extractor


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


def make_code_base_pheno(df_in: DataFrame, table_tag: str, param_yaml: dict, codelist_df: DataFrame,
                         list_extra_cols_to_keep: list = [], pre_cleaned=False
                         ) -> PhenoTableSet:
    if table_tag == "gdppr":
        if pre_cleaned:
            tset = PhenoTableSetGdppr(None, param_yaml=param_yaml)
            tset.df_final = df_in
        else:
            tset = PhenoTableSetGdppr(df_in, param_yaml=param_yaml)
            tset.cleaning_and_report(list_extra_cols_to_keep)
        if codelist_df is not None:
            tset.extract_pheno_tables(codelist_df, list_extra_cols_to_keep)

    elif table_tag == "hes_apc":
        if pre_cleaned:
            tset = PhenoTableSetHesApc(None, param_yaml=param_yaml)
            tset.df_final = df_in
        else:
            tset = PhenoTableSetHesApc(df_in, param_yaml=param_yaml)
            tset.cleaning_and_report(list_extra_cols_to_keep)
        if codelist_df is not None:
            tset.extract_pheno_tables(codelist_df, list_extra_cols_to_keep)
    else:

        tset = None

    return tset


def make_date_base_pheno(df_in: DataFrame, table_tag: str, param_yaml: dict,
                         list_extra_cols_to_keep: list = [], pre_cleaned=False
                         ) -> PhenoTableSetDateBased:
    if pre_cleaned:
        tset = PhenoTableSetDateBased(None, param_yaml=param_yaml)
        tset.df_final = df_in
    else:
        tset = PhenoTableSetDateBased(df_in, param_yaml)
        tset.cleaning_and_report(list_extra_cols_to_keep)

    tset.extract_pheno_tables(list_extra_cols_to_keep)

    # tseract_full_pheno_df()
    # tset.make_distinct_eventdate_pheno(add_isin_flag=True)
    return tset


'''

    if table_tag == "sgss":
        tset = PhenoTableSetGdppr(df_raw, param_yaml)
        tset.cleaning_and_report(list_extra_cols_to_keep)
        tset.extract_code_based_pheno(codelist_df, list_extra_cols_to_keep)

    elif table_tag == "hes_apc":
        tset = PhenoTableSetHesApc(df_raw, param_yaml)
        tset.cleaning_and_report(list_extra_cols_to_keep)
        tset.extract_code_base_pheno(codelist_df, list_extra_cols_to_keep)
    else:

        tset = None
'''


def covid_pheno_date_based(df: DataFrame, param_yaml: str, table_tag: str, distinct_dates=True):
    dfset, ps = make_dfset(df, param_yaml, table_tag=table_tag)
    dfset.extract_full_pheno_df()
    pheno_full_long = dfset.explode_array_col(dfset.pheno_df_full, ("list_distinct" if distinct_dates else "list_all"),
                                              dfset.ps.index_col,
                                              dfset.ps.evdt_pheno)
    # dfset.extract_basic_pheno_df()
    # pheno_basic_long = dfset.pheno_df_basic
    return dfset, pheno_full_long
