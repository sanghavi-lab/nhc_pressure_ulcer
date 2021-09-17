# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will merge 27 mbsf chronic conditions with mds-hospital_claims denominators.
# ----------------------------------------------------------------------------------------------------------------------#

import dask
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
import numpy as np

def add_chronic_condition_code(row):
    ## this function will determine the chronic condition for each beneficiary:
    ## if the beneficiary has ever been diagnosed with the chronic condition prior to hospital admission
    ## the indicator for the chronic condition (**_final) equals 1 and 0 otherwise

    ## define chronic condition columns
    cc_code = ['AMI', 'ALZH', 'ALZH_DEMEN', 'ATRIAL_FIB', 'CATARACT',
               'CHRONICKIDNEY', 'COPD', 'CHF', 'DIABETES', 'GLAUCOMA',
               'HIP_FRACTURE', 'ISCHEMICHEART', 'DEPRESSION',
               'OSTEOPOROSIS', 'RA_OA', 'STROKE_TIA', 'CANCER_BREAST',
               'CANCER_COLORECTAL', 'CANCER_PROSTATE', 'CANCER_LUNG', 'CANCER_ENDOMETRIAL', 'ANEMIA',
               'ASTHMA', 'HYPERL', 'HYPERP', 'HYPERT', 'HYPOTH']

    for cc in cc_code:
        colname = cc + '_final'
        cc_ever = cc + '_EVER'
        if (row[cc_ever] <= row['ADMSN_DT']):
            row[colname] = 1
        else:
            row[colname] = 0
    return row

def merge_mbsfcc(mbsfcc_path, merge_path, write_path):
    n_merge_all = []
    years = range(2011, 2018)

    ## read in mds-hospital_claims data
    df = dd.read_csv(merge_path,
                     dtype={'A2000_DSCHRG_DT_lag': 'object',
                            'PRVDR_NUM': 'object'})

    df = df.astype({'ADMSN_DT': 'datetime64[ns]'})
    ## drop Unnamed: 0 column if it is in the data
    if sum('Unnamed: 0'==col for col in df.columns)>0:
        df = df.drop(columns=['Unnamed: 0'])
    ## define chronic condition columns
    cc_code = ['AMI', 'ALZH', 'ALZH_DEMEN', 'ATRIAL_FIB', 'CATARACT',
               'CHRONICKIDNEY', 'COPD', 'CHF', 'DIABETES', 'GLAUCOMA',
               'HIP_FRACTURE', 'ISCHEMICHEART', 'DEPRESSION',
               'OSTEOPOROSIS', 'RA_OA', 'STROKE_TIA', 'CANCER_BREAST',
               'CANCER_COLORECTAL', 'CANCER_PROSTATE', 'CANCER_LUNG', 'CANCER_ENDOMETRIAL', 'ANEMIA',
               'ASTHMA', 'HYPERL', 'HYPERP', 'HYPERT', 'HYPOTH']
    ## read in mbsf chronic condition data
    for i in range(len(years)):
        cc = dd.read_csv(mbsfcc_path[i], low_memory=False, assume_missing=True)
        cc = cc.astype(dict(zip([c + "_EVER" for c in cc_code],
                                ['datetime64[ns]'] * 27)))

        ## select mds-hospital_claims data for each year
        df_year = df[df.MEDPAR_YR_NUM==years[i]]

        ## merge mbsfcc chronic conditions with hospital analytical sample data for each year
        merge = df_year.merge(cc[['BENE_ID'] + [c + "_EVER" for c in cc_code]],
                              on='BENE_ID', how='inner')

        merge = merge.apply(add_chronic_condition_code, axis=1)
        # write to parquet
        merge.to_parquet(write_path + '{}/'.format(years[i]))
        n_merge_all.append(merge.shape[0].compute())
    print(sum(n_merge_all))


if __name__ == '__main__':
    import yaml
    from dask.distributed import Client
    client = Client("10.50.86.250:42654")

    yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

    years = list(range(2011, 2018))

    #121058
    merge_mbsfcc(path['7_merge_mbsfcc']['input_mbsfcc'],
                 path['7_merge_mbsfcc']['input_main'],
                 path['7_merge_mbsfcc']['output_main'])

    # 363500
    merge_mbsfcc(path['7_merge_mbsfcc']['input_mbsfcc'],
                 path['7_merge_mbsfcc']['input_spu'],
                 path['7_merge_mbsfcc']['output_spu'])
