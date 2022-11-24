from pheno_package.nhsd_docker_pyspark_package.DataFrameSet import DataFrameSet


def show_all_dfs(dfset: DataFrameSet):
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
