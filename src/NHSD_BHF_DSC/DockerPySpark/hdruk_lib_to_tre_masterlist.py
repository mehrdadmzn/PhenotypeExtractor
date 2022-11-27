import csv
import sys
import os
import pandas as pd

input_file = f'''/Users/mehrdadmizani/PycharmProjects/PE/codelists/hdr_uk_pheno_lib/ischaemic_stroke/phenotype_PH56_ver_112_concepts_20221127T004004.csv'''

hdruk_pd = pd.read_csv(input_file)


# print(hdruk_pd.head(10))
# print(hdruk_pd.columns)
# print(hdruk_pd[["coding_system"]])

def hdruk_lib_to_tre(hdruk_pd, hdruk_pheno_date, tre_pheno_name, terminology_list=["SNOMED", "ICD10"]):
    tre_pd = hdruk_pd.copy()

    tre_pd['name'] = tre_pheno_name
    tre_pd['terminology'] = tre_pd['coding_system']

    replace_terminology = {
        "ICD10 codes": "ICD10",
        "SNOMED CT codes": "SNOMED",
        "Read codes_v2": "READ"
    }
    tre_pd["terminology"] = tre_pd["terminology"].map(replace_terminology)
    tre_pd = tre_pd[tre_pd['terminology'].isin(terminology_list)]
    out_pd = pd.DataFrame()
    out_pd['name'] = tre_pd['name']
    out_pd['terminology'] = tre_pd['terminology']
    out_pd['code'] = tre_pd['code']
    out_pd['description'] = tre_pd['description']
    out_pd['code_type'] = 1
    out_pd['RecordDate'] = hdruk_pheno_date

    return out_pd


tre_pheno_name = 'stroke_IS'
stroke_is = hdruk_lib_to_tre(hdruk_pd, "20190520", tre_pheno_name, terminology_list=["SNOMED", "ICD10"])
stroke_is.to_csv(
    f'''/Users/mehrdadmizani/PycharmProjects/PE/codelists/hdr_uk_pheno_lib/ischaemic_stroke/tre_format.csv''',
    sep='\t', index=False)