# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will select pressure ulcer patients coming from nursing home.
# ----------------------------------------------------------------------------------------------------------------------#

####################################################################
## This code is to
## select hospital claims from patients who come from nursing home and then admitted to hospital
## within 1 day of being discharged from the nursing home with a MDS discharge assessments;
## these patients are defined has having pressure ulcers during nursing home residency
####################################################################

import dask
import os
import dask.dataframe as dd
import pandas as pd
from fastparquet import ParquetFile


def select_pu_in(row):
    ## this function looks at each hospital claim to determine
    ## if patients were nursing home residents when they had pressure ulcers (they had pressure ulcers during nursing home residency);
    ## only claims with from_nh=1 were used in later analysis

    min_day = 1 ## not used
    max_day = 180 ## not used

    ## from_nh=1 means the patient had pressure ulcer during NH residency
    if row.rank==0: # if hospital claim is the first record for the beneficiary, he/she is not from nh
        row['from_nh'] = 0
    else:
        ## use A0310F_ENTRY_DSCHRG_CD_lag code to see if the last record from hospital claim is a mds discharge assessment
        if (row.A0310F_ENTRY_DSCHRG_CD_lag==10) | (row.A0310F_ENTRY_DSCHRG_CD_lag==11):
            ## use A2100_DSCHRG_STUS_CD_lag to see if the discharge destination of the discharge assessment is an acute hospital or long term care hospital
            if (row.A2100_DSCHRG_STUS_CD_lag!=3) & (row.A2100_DSCHRG_STUS_CD_lag!=9):
                row['from_nh'] = 0 ## if the patient is not discharged to a hospital from nursing home, from_nh=0
            ## if the discharge destination is acute hospital or long term care hospital
            elif (row.A2100_DSCHRG_STUS_CD_lag==3) | (row.A2100_DSCHRG_STUS_CD_lag==9):
                ## calculate the days between nursing home discharge date and hospital admission date
                d = row['rd'] - row['A2000_DSCHRG_DT_lag']
                ## if hospital admission date is within plus/minus 1 days of mds discharge date then this patient comes from NH;
                if (abs(d.days) < min_day) | (abs(d.days) == min_day):
                    row['from_nh'] = 1
                elif (abs(d.days)>min_day) and ((abs(d.days)<max_day) | (abs(d.days)==max_day)):
                    row['from_nh'] = -1
                ## if days elapsed greater than 180 then patients did not come from NH;
                elif abs(d.days)>max_day:
                    row['from_nh'] = 0
        else: ## if preivous record of hospital claim is not a discharge assessment, from_nh != 1
            d = row['rd'] - row['TRGT_DT_lag']
            if abs(d.days) > max_day:
                row['from_nh'] = 0 ## if more than 180 days has passed since NH discharge to hospital admission then patients did not come from NH;
            else:
                row['from_nh'] = -1
    return row

def create_pu_in(concat_rank_path, write_path):
    ## this function selects medpar claims for patients who have pressure ulcers during
    ## nursing home residency (from_nh=1) and writes these claims to parquet

    ## read concat_rank data
    df = dd.read_csv(concat_rank_path,
                     dtype={'PRVDR_NUM': 'object',
                            'DGNS_2_CD': 'object',
                            'DGNS_24_CD': 'object',
                            'DGNS_25_CD': 'object',
                            'POA_DGNS_20_IND_CD': 'object',
                            'POA_DGNS_21_IND_CD': 'object',
                            'POA_DGNS_22_IND_CD': 'object',
                            'POA_DGNS_23_IND_CD': 'object',
                            'POA_DGNS_24_IND_CD': 'object',
                            'POA_DGNS_25_IND_CD': 'object'
                            },
                     low_memory=False)
    ## drop HMO columns from MBSF in analytical sample data
    hmo  = ['HMO_IND_{:02d}'.format(i) for i in range(1, 13)]
    df = df.drop(columns=hmo)
    ## change data type for columns
    df = df.astype({'rd': 'datetime64[ns]',
                    'A2000_DSCHRG_DT_lag': 'datetime64[ns]',
                    'TRGT_DT_lag': 'datetime64[ns]'})
    ## select medpar hospital claims
    df = df[df.r == 'h']

    ## exclude the first 100 days in 2011 and the last 3 months in 2015
    ## exclude the first 100 days for the later determination of short-stay vs. long-stay patients using 5-day MDS PPS assessment within 100 days
    df = df[~((df.rd < '2011-04-11') |
              ((df.rd >= '2015-10-01') &
               (df.rd < '2016-01-01')))]

    ## apply function to identify patients who have pu during nh stay
    df = df.apply(select_pu_in, axis=1)
    ## select patients who have pu during nh residentcy
    df_pu_in = df[df['from_nh'] == 1]
    ## write selected patients' inpatient hospital claims to parquet
    print(df_pu_in.shape[0].compute())
    # df_pu_in.to_parquet(write_path)



if __name__ == '__main__':
    import yaml
    from dask.distributed import Client
    client = Client("10.50.86.250:50521")
    ## define file paths
    yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

    ## run functions to select primary and secondary pressure ulcer hospital claims
    ## for patients who had pressure ulcer during nh stay
    #168246
    create_pu_in(path['3_select_pu_in']['input_main'],
                path['3_select_pu_in']['output_main'])
    #572331
    create_pu_in(path['3_select_pu_in']['input_spu'],
                path['3_select_pu_in']['output_spu'])

