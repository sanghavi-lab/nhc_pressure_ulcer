# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will merge nh facility data from CASPER with mds-hospital_claims and mds-snf_claims denominators
#              nursing home-level characteristics.
# ----------------------------------------------------------------------------------------------------------------------#

import dask
import os
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
import numpy as np
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)


def merge_fac(merge_path, casper_path, facility_path, write_path, claims='medpar'):
    if claims=='medpar':
        df = pd.read_parquet(merge_path)
    elif claims=='snf':
        snf_path = os.listdir(merge_path)
        print(snf_path)
        df = [pd.read_csv(merge_path+p, low_memory=False) for p in snf_path]
        df = pd.concat(df)
    print(df.MEDPAR_ID.unique().size)

    ## merge casper data in
    ### 1) use facility.csv from LTCFocus to crosswalk provider number
    casper = pd.read_csv(casper_path, low_memory=False) ## read in capser data
    facility = pd.read_csv(facility_path) ## read in facility data

    ## create new column -fac_state_id- that concatenates FACILITY_INTERNAL_ID and STATE_ID
    facility = facility.astype({'FACILITY_INTERNAL_ID': 'str',
                                'STATE_ID': 'str'})
    ## fill FACILITY_INTERNAL_ID with leading zeros
    facility['FACILITY_INTERNAL_ID'] = facility['FACILITY_INTERNAL_ID'].str.zfill(10)
    ## combine FACILITY_INTERNAL_ID and STATE_ID to create fac_state_id
    facility['fac_state_id'] = facility['FACILITY_INTERNAL_ID'] + facility['STATE_ID']

    df['FAC_PRVDR_INTRNL_ID'] = df['FAC_PRVDR_INTRNL_ID'].astype('Int64')

    df = df.astype({'FAC_PRVDR_INTRNL_ID': 'str',
                    'TRGT_DT': 'datetime64[ns]'})
    df['FAC_PRVDR_INTRNL_ID'] = df['FAC_PRVDR_INTRNL_ID'].str.zfill(10)
    df['fac_state_id'] = df['FAC_PRVDR_INTRNL_ID'] + df['STATE_CD']

    ## merge facility data and mds-medpar data on the fac_state_id column
    df = df.merge(facility[['fac_state_id', 'MCARE_ID', 'NAME', 'ADDRESS', 'FAC_CITY', 'STATE_ID','FAC_ZIP', 'CATEGORY', 'CLOSEDDATE' ]],
                  on='fac_state_id',
                  how='left')
    print('what is the percentage of mds-medpar records didn not match to a MCARE_ID?')
    print(df.MCARE_ID.isna().mean())

    ## clean CASPER data
    casper = casper.rename(columns={'PRVDR_NUM': 'MCARE_ID'})
    casper['CRTFCTN_DT'] = pd.to_datetime(casper['CRTFCTN_DT'].astype(str), infer_datetime_format=True)
    ## create casper_year to indicate the year of the survey results
    casper['casper_year'] = casper['CRTFCTN_DT'].dt.year

    ## keep the latest snap shot of NH characteristics for each year
    casper_latest = casper.groupby(['MCARE_ID', 'casper_year'])['CRTFCTN_DT'].max().reset_index()
    casper = casper.merge(casper_latest, on=['MCARE_ID', 'casper_year', 'CRTFCTN_DT'], how='inner')

    ## 2) merge the latest NH characteristics within a year from CASPER with mds-medpar
    df_casper = df.merge(casper[['STATE_CD', 'MCARE_ID', 'CRTFCTN_DT', 'casper_year',
                                 'GNRL_CNTL_TYPE_CD', 'CNSUS_RSDNT_CNT', 'CNSUS_MDCD_CNT',
                                 'CNSUS_MDCR_CNT', 'CNSUS_OTHR_MDCD_MDCR_CNT']],
                         on='MCARE_ID',
                         how='left',
                         suffixes=['', '_casper'])
    # check
    print('how many claims are not matched with casper records?')
    print(df_casper[df_casper.CRTFCTN_DT.isna()].MEDPAR_ID.unique().size)
    ## drop the Unnamed: 0 column if it is in the data (usually casued by reading in the csv data with unnamed index)
    if sum(['Unnamed: 0'==col for col in df_casper.columns]) >0:
        df_casper = df_casper.drop(columns=['Unnamed: 0'])

    ## select mds-medpar data matched with same year casper and write to csv
    df_casper_matched = df_casper[(df_casper.MEDPAR_YR_NUM == df_casper.casper_year)]
    df_casper_matched.to_csv(write_path + 'merged_with_sameyear_casper.csv', index=False)
    # print(df_casper_matched.shape[0].compute()==df_casper_matched.MEDPAR_ID.unique().size.compute())


    ## calculate the number of years between casper and claim
    df_casper['days_casper'] = (df_casper['TRGT_DT'] - df_casper['CRTFCTN_DT']).dt.days
    df_casper['abs_days_casper'] = abs(df_casper['days_casper'])
    df_casper['years_casper'] = df_casper['days_casper']/365

    ## select casper within two years of the hospital/SNF claim
    df_casper_nonmatched = df_casper[~df_casper.MEDPAR_ID.isin(df_casper_matched.MEDPAR_ID)]
    df_casper_nonmatched = df_casper_nonmatched[((df_casper_nonmatched['years_casper']>=0) &
                                                 (df_casper_nonmatched['years_casper']<=2))]


    ## select the closest casper linked to mds-medpar denominator
    df_casper_closest_casper = \
        df_casper_nonmatched. \
            groupby('MEDPAR_ID')['abs_days_casper']. \
            min(). \
            rename('closest_casper').reset_index()
    ## get all other columns for the closest casper
    df_casper_nonmatched = df_casper_nonmatched.merge(df_casper_closest_casper[['MEDPAR_ID', 'closest_casper']],
                                                      on='MEDPAR_ID',
                                                      how='inner')
    ## for claims not matched with casper at the year of hospitalization, select the closest previous casper within 2 years
    df_casper_nonmatched = df_casper_nonmatched[df_casper_nonmatched.abs_days_casper == df_casper_nonmatched.closest_casper]
    df_casper_nonmatched = df_casper_nonmatched.drop(columns=['days_casper', 'abs_days_casper', 'years_casper', 'closest_casper'])
    ## write to csv
    df_casper_nonmatched.to_csv(write_path + 'merged_with_nonsameyear_casper.csv', index=False)

    df_casper = pd.concat([df_casper_matched, df_casper_nonmatched])
    print('final sameple size is')
    print(df_casper.shape[0])
    print(df_casper.MEDPAR_ID.unique().size)

    return df_casper

if __name__ == '__main__':
    import yaml
    from dask.distributed import Client
    client = Client("10.50.86.250:37700")

    yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

    years = list(range(2011, 2018))

    # 121197 obs after merging with MBSF
    # final sample is 121058
    main = merge_fac(path['6_merge_mbsf_and_fac']['input_ip']['main'],
                       path['6_merge_mbsf_and_fac']['input_casper_path'],
                       path['6_merge_mbsf_and_fac']['input_facility_path'],
                       path['6_merge_mbsf_and_fac']['output_ip']['main'],
                       claims='medpar')

    ## 363861 obs after merging with MBSF
    ## final sample size is 363500
    spu = merge_fac(path['6_merge_mbsf_and_fac']['input_ip']['spu'],
                      path['6_merge_mbsf_and_fac']['input_casper_path'],
                      path['6_merge_mbsf_and_fac']['input_facility_path'],
                      path['6_merge_mbsf_and_fac']['output_ip']['spu'],
                      claims='medpar')

    # 110134 obs after merging with MBSF
    # 34 mds not matched with capser
    # final sample size is 110070
    main_snf = merge_fac(path['6_merge_mbsf_and_fac']['input_snf'],
                          path['6_merge_mbsf_and_fac']['input_casper_path'],
                          path['6_merge_mbsf_and_fac']['input_facility_path'],
                          path['6_merge_mbsf_and_fac']['output_snf'],
                         claims='snf')

