import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import yaml


class ParameterSet:
    def __init__(self, parameter_yaml):
        self.pyaml = yaml.load(parameter_yaml, Loader=yaml.SafeLoader)
        table_details_node = self.pyaml.get("table_details")
        pheno_details_node = self.pyaml.get("pheno_details")
        quality_control_node = self.pyaml.get("quality_control")
        optional_settings_node = self.pyaml.get("optional_settings")
        self.phenotype_name = self.pyaml.get("phenotype_name")
        self.table_tag = self.pyaml.get("table_tag")
        self.codelist_format = self.pyaml.get("codelist_format")
        # Pheno details
        self.evdt_pheno = pheno_details_node.get("evdt_pheno")
        self.pheno_pattern = pheno_details_node.get("pheno_pattern")
        self.terminology = pheno_details_node.get("terminology")
        self.check_code_type = pheno_details_node.get("check_code_type")
        self.code_type = pheno_details_node.get("code_type")
        self.code_type_list = []
        self.set_code_type_list()
        if "hes_apc_specific" in list(self.pyaml.keys()):
            self.primary_diagnosis_only = self.pyaml.get("hes_apc_specific").get("primary_diagnosis_only")
        else:
            self.primary_diagnosis_only = None
        self.limit_pheno_window = pheno_details_node.get("limit_pheno_window")
        self.pheno_window_start = pheno_details_node.get("pheno_window_start")
        self.pheno_window_end = pheno_details_node.get("pheno_window_end")
        # Table details
        # self.table_tag = table_tag
        self.index_col = table_details_node.get("index_col")
        self.evdt_col_raw = table_details_node.get("evdt_col_raw")
        self.evdt_col_list = table_details_node.get("evdt_col_list")
        self.code_col = None
        if "code_col" in list(table_details_node.keys()):
            self.code_col = table_details_node.get("code_col")
        else:
            self.code_col = None
        self.production_date_str = table_details_node.get("production_date_str")
        # Quality control
        self.start_date_qc = quality_control_node.get("start_date_qc")
        self.end_date_qc = quality_control_node.get("end_date_qc")
        # Optional settings
        self.full_report = optional_settings_node.get("full_report")
        self.spark_cache_midway = optional_settings_node.get("spark_cache_midway")
        self.impute_multi_col_null_dates = optional_settings_node.get("impute_multi_col_null_dates")
        self.impute_multi_col_invalid_dates = optional_settings_node.get("impute_multi_col_invalid_dates")
        self.drop_null_ids = optional_settings_node.get("drop_null_ids")
        self.drop_remaining_null_dates = optional_settings_node.get("drop_remaining_null_dates")
        self.drop_remaining_invalid_dates = optional_settings_node.get("drop_remaining_invalid_dates")

    def set_code_type_list(self):
        code_type = str(self.code_type)
        if (code_type == "0") or (code_type == "historical"):
            self.code_type_list = [0]
        elif (code_type == "1") or (code_type == "incident"):
            self.code_type_list = [1]
        elif (code_type == "both"):
            self.code_type_list == [0, 1]
        else:
            self.code_type_list = []


class ParameterSetGdppr(ParameterSet):
    def __init__(self, parameter_yaml):
        super().__init__(parameter_yaml)
        table_specific_node = self.pyaml.get("gdppr_specific")
        self.swap_date_larger_than_record_date = table_specific_node.get("swap_date_larger_than_record_date")
