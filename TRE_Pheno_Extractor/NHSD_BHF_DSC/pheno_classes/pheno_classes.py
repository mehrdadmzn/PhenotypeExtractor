from abc import ABC, abstractmethod

import pyspark.sql
import yaml
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from pyspark.sql import Row
import datetime


def yaml_loader_func(yaml_settings):
    return yaml.load(yaml_settings, Loader=yaml.SafeLoader)


def make_time_stamps(general_yaml_loader):
    start_date_str = general_yaml_loader.get("start_date_str")
    start_date = start_date_str
    end_date_str = general_yaml_loader.get("end_date_str")
    end_date = end_date_str
    if isinstance(start_date_str, str):
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
    if isinstance(end_date_str, str):
        end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()

    return start_date, end_date


class AbstractPhenoExtractor(ABC):

    @abstractmethod
    def df_count_distinct(self, index_col_str: str):
        pass

    @abstractmethod
    def make_df_clean(self):
        pass

    @abstractmethod
    def make_df_sel(self):
        pass

    @abstractmethod
    def event_date_imputation(self):
        pass

    @abstractmethod
    def event_date_range_control(self):
        pass

    @abstractmethod
    def change_date_col_format(self, date_col):
        pass

    @abstractmethod
    def print_table_yaml(self):
        pass

    @abstractmethod
    def print_general_yaml(self):
        pass


class TablePhenoExtractor(AbstractPhenoExtractor):

    def __new__(cls, *args, **kwargs):
        if cls is TablePhenoExtractor:
            raise TypeError(f'''Do not instantiate {cls.__name__} directly. Instantiate its childre.''')
        return super().__new__(cls)

    def __init__(self, df_raw, table_tag, yaml_loader):
        self.table_tag = table_tag
        self.pheno_type = None
        self.table_yaml = yaml_loader.get("table_tag").get(self.table_tag)
        self.general_yaml = yaml_loader.get("general")
        self.start_date, self.end_date = make_time_stamps(self.general_yaml)
        # DFs
        self.df_raw = df_raw
        self.df_clean = None
        self.df_sel = None
        self.df_ready = None
        # col names
        self.index_col = self.get_index_col()
        self.df_raw_evdt_col = self.get_df_event_date_col_name()
        self.df_pheno_evdt_col = f'''{self.table_tag}_evdt'''
        self.count_all_col = "count_all"
        self.count_null_col = "count_null"
        self.list_all_col = "list_all"
        self.list_distinct_col = "list_distinct"

    def df_count_distinct(self, index_col_str: str):
        print(f'''{index_col_str}, len = {len(self.df_raw)}''')

    def print_table_yaml(self):

        print(self.table_yaml)

    def print_general_yaml(self):
        print(self.general_yaml)

    def get_index_col(self):
        return self.table_yaml.get("index_col")

    def get_df_event_date_col_name(self):
        # Todo: Test column exists
        # Todo: Test format
        return self.table_yaml.get("main_event_date_col")

    def get_date_col_list(self):
        # Todo: Test and ensure the first one is the main event date col
        return self.table_yaml.get("event_date_cleaning_order")

    def change_date_col_format(self, date_col):
        # Todo: implement this
        pass

    def is_multiple_event_date_columns(self):
        # Todo: Test the return value, which should be Boolean
        return self.table_yaml.get("impute_date_null_values")

    def make_df_clean(self):
        self.df_clean = self.df_raw.filter(F.col(self.index_col).isNull())

    def make_df_sel(self):
        pass

    def event_date_imputation(self):
        self.df_ready = self.df_ready.withColumn(self.df_pheno_evdt_col, F.col(self.get_date_col_list()[0]))
        if self.is_multiple_event_date_columns():
            for col_name in self.get_date_col_list()[1:]:
                self.df_ready = self.df_sel.withColumn(self.df_pheno_evdt_col, F.when(
                    ((F.col(self.df_pheno_evdt_col).isNull()) & (F.col(col_name).isNotNull())),
                    F.col(col_name)).otherwise(F.col(self.df_pheno_evdt_col)))

    def event_date_range_control(self):
        if self.is_multiple_event_date_columns():
            for col_name in self.get_date_col_list()[1:]:
                self.df_ready = self.df_ready.withColumn(self.df_pheno_evdt_col, F.when(
                    ((not F.col(self.df_pheno_evdt_col) >= self.start_date) & (F.col(str(col_name)) >= self.start_date)),
                    F.col(col_name)).otherwise(F.col(self.df_pheno_evdt_col)))

                self.df_ready = self.df_ready.withColumn(self.df_pheno_evdt_col, F.when(
                    ((not F.col(self.df_pheno_evdt_col) <= self.end_date) & (F.col(str(col_name)) <= self.end_date)),
                    F.col(col_name)).otherwise(F.col(self.df_pheno_evdt_col)))

        # Todo: Handle any remaining later in the pipeline


class TableEventPhenoExtractor(TablePhenoExtractor):

    def __init__(self, df_raw, table_tag, yaml_loader):
        super().__init__(df_raw, table_tag, yaml_loader)
        # Todo: Test pheno_type =event in yaml

        self.pheno_type = "event"
        # Todo: Report counts all, distinct, null

        self.make_df_clean()
        # Todo: report post cleaning

        self.make_df_sel()
        self.df_ready = self.df_sel

        self.event_date_imputation()
        # Todo: report pre and post null value impution
        # Todo: report out of range dates

        self.event_date_range_control()
        # Todo: check and report out of range dates



    def make_df_sel(self):
        col_list = [self.index_col]
        if self.is_multiple_event_date_columns():
            col_list = col_list + self.get_date_col_list()
        else:
            col_list = col_list + [self.df_raw_evdt_col]

        self.df_sel = self.df_raw.select(col_list)

        # def make_pheno_event_date_col(self):

        # Todo: Check and change date format
    # df_sel =

    # if self.table_yaml.get("impute_date_null_values"):


pheno_yaml = """
general:
  start_date_str: 2019-12-01
  end_date_str: 2022-08-31
table_tag:
  sgss:
    pheno_type: event
    index_col: PERSON_ID_DEID
    main_event_date_col: Specimen_Date
    impute_date_null_values: yes
    event_date_cleaning_order:
      - Specimen_Date
      - Lab_Report_Date
  chess:
    test: test
"""
yaml_loader = yaml_loader_func(pheno_yaml)


def extract_sgss_pheno(df_raw, yaml_loader):
    sgss_yaml_loader = yaml_loader.get("table_tag").get("sgss")
    general_yaml_loader = yaml_loader.get("general")

    # print(sgss_yaml_loader)
    # print(general_yaml_loader)

    sgss_pheno = TableEventPhenoExtractor(df_raw, "sgss", yaml_loader)
    return sgss_pheno

report_dict ={
    "summary": "test of this and that",
    "table_tag": "sgss",
    "database": "db_collab",
    "raw_table": "sssss",
    "index_col": "PATIENT_ID_DEID",
    "evdt_col" : "",
    "date_col_list": [],
    "id_report":
        {
            "raw_null_id": 0,
            "dropped_null_id": 0,
            "non_null_id": 0,
            "non_null_distinct_id": 0,
        },
    "event_date_report":
        {

        }


}

#print(report_dict)
report_dict.get("id_report")["raw_null_id"] = 23
print(report_dict.get("id_report"))
'''
a = extract_sgss_pheno([1, 2, 3], yaml_loader)
print(a.table_tag)
print(a.pheno_type)
print(a.df_raw)
print(a.start_date)
print(a.end_date)
'''


# df = [1, 2, 3]
# c = (df, "sgss", covid_yaml)
# print(c.df_raw)
# print(c.df_count_distinct("test"))
