# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will select MDS assessments within the beneficiaries' NH stay .
# ----------------------------------------------------------------------------------------------------------------------#


import dask
import os
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
import numpy as np


def assign_end_date(row):
    ## assign either the discharge date or the care through date as the end date of a snf stay
    row['end_date'] = row['DSCHRG_DT']

    if pd.isnull(row['DSCHRG_DT']):
        row['end_date'] = row['CVRD_LVL_CARE_THRU_DT']

    return row


def select_mds_within_stay(merge_path, write_path, mds_all_dash_id):
    ## this is for calculation in flowchart
    nmedpar = [] ## the number of medpar SNF PU claims merged with mds
    no_end_date_medpar = [] ## the number of merged SNF PU claims without an assigned end date (both dschrg_dt and CVRD_LVL_CARE_THRU_DT is missing)
    n_within_stay_no_entry = [] ## the number of claims merged with at leaset one mds (except entry/death record) within SNF stay
    multiple_nh = [] ## the number of claims merged with mds from several NH within stay
    n_final = [] ## the final number of SNF claims selected
    ########################################

    for i in range(len(merge_path)):
        ## read in merged MDS and SNF claims
        merge = pd.read_parquet(merge_path[i])
        merge = merge.replace('', np.nan)
        ## calculate the number of medpar SNF PU claims merged with mds
        nmedpar.append(merge.MEDPAR_ID.unique().size)

        ## create a date column represents the end date of SNF stay covered by Medicare
        merge = merge.apply(assign_end_date, axis=1)
        ## calculate the number of merged SNF PU claims without an assigned end date
        no_end_date_medpar.append(merge[merge.end_date.isna()].MEDPAR_ID.unique().size)

        ## SELECT MDS WITHIN THE START AND END DATE OF SNF STAY
        merge = merge.astype({'DSCHRG_DT': 'datetime64[ns]',
                              'end_date': 'datetime64[ns]',
                              'TRGT_DT': 'datetime64[ns]',
                              'ADMSN_DT': 'datetime64[ns]',
                              'A0310F_ENTRY_DSCHRG_CD': 'int64'})
        merge['within_stay'] = (merge['TRGT_DT'] <= merge['end_date']) & (merge['TRGT_DT'] >= merge['ADMSN_DT'])

        ## select mds within SNF stay
        merge = merge[merge.within_stay==1]
        ## remove entry/death record (has no pressure ulcer item in mds)
        wstay_no_entry = merge[~merge.A0310F_ENTRY_DSCHRG_CD.isin([1, 12])]

        ## remove mds if it is a discharge assessment and its target date is on the day of SNF admission date
        wstay_no_entry = wstay_no_entry[~((wstay_no_entry.A0310F_ENTRY_DSCHRG_CD.isin([10, 11])) & (wstay_no_entry.TRGT_DT == wstay_no_entry.ADMSN_DT))]

        ## calculate the number of SNF claims that has mds from multiple NHs within the stay
        wstay_nh = wstay_no_entry.groupby('MEDPAR_ID')['FAC_PRVDR_INTRNL_ID'].nunique().reset_index()
        wstay_nh_multiple = wstay_nh[wstay_nh['FAC_PRVDR_INTRNL_ID'] > 1]
        multiple_nh.append(wstay_nh_multiple.shape[0])

        ## remoeve SNF claims matched with mds from multiple NHs
        ## ensure that all SNF claims are merged with only one nursing home with reporting responsibility,
        ## i.e. the same nursing home that submits the MedPAR claim
        wstay_no_entry = wstay_no_entry[~wstay_no_entry.MEDPAR_ID.isin(wstay_nh_multiple.MEDPAR_ID)]
        del wstay_nh_multiple
        del wstay_nh

        ## calculate the number of SNF claims matched with mds within stay
        m_wstay_no_entry = wstay_no_entry.groupby('MEDPAR_ID')['within_stay'].sum()
        n_within_stay_no_entry.append((m_wstay_no_entry > 0).sum())
        del m_wstay_no_entry

        ## remove mds where all pu items are coded as "-"
        wstay_no_entry = wstay_no_entry[~wstay_no_entry.MDS_ASMT_ID.isin(mds_all_dash_id)]

        ## for each SNF claims, calculate whether any matched mds reported pressure ulcer
        m_pu_col = \
            ['m_pu',
             'm_pu_stage2_and_above',
             'M0300A_STG_1_ULCR_NUM',
             'M0300B1_STG_2_ULCR_NUM',
             'M0300C1_STG_3_ULCR_NUM',
             'M0300D1_STG_4_ULCR_NUM',
             'M0300E1_UNSTGBL_ULCR_DRSNG_NUM',
             'M0300F1_UNSTGBL_ULCR_ESC_NUM',
             'M0300G1_UNSTGBL_ULCR_DEEP_NUM']

        report = wstay_no_entry.groupby('MEDPAR_ID')[m_pu_col].max().reset_index()
        wstay_no_entry = wstay_no_entry.drop(columns=m_pu_col)

        wstay_no_entry = wstay_no_entry.sort_values(by=['MEDPAR_ID', 'TRGT_DT'])
        wstay_no_entry = wstay_no_entry.drop_duplicates(subset='MEDPAR_ID')
        wstay_no_entry = wstay_no_entry.merge(report, on='MEDPAR_ID', how='inner')

        ## calculate final the number of MedPAR claims selected
        n_final.append(wstay_no_entry.MEDPAR_ID.unique().size)

        wstay_no_entry.to_csv(write_path + '{}.csv'.format(i))
    print(sum(nmedpar)) #134793
    print(sum(no_end_date_medpar)) #17277
    print(sum(multiple_nh)) #98
    print(sum(n_within_stay_no_entry)) #110209
    print(sum(n_final)) #110134


if __name__ == '__main__':
    import yaml
    from dask.distributed import Client
    client = Client('10.50.86.250:37700')
    years = list(range(2011, 2018))

    ## define file paths
    yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))
    main_merge_snf_mds_path = \
        [path['3_select_mds_within_snf_stay']['input'] + '{}/'.format(i)
         for i in range(21)]

    ## select MDS where all pu items are coded as "-"
    pucode_num = ['M0210_STG_1_HGHR_ULCR_CD', 'M0300A_STG_1_ULCR_NUM', 'M0300B1_STG_2_ULCR_NUM',
                  'M0300C1_STG_3_ULCR_NUM', 'M0300D1_STG_4_ULCR_NUM', 'M0300E1_UNSTGBL_ULCR_DRSNG_NUM',
                  'M0300F1_UNSTGBL_ULCR_ESC_NUM', 'M0300G1_UNSTGBL_ULCR_DEEP_NUM']

    mds_all_dash = []
    for i in range(len(years)):
        mds = dd.read_parquet(path['processMDS']['input'][i])
        cols = [col.upper() for col in mds.columns]
        mds.columns = cols
        mds['all_dash'] = mds.apply(lambda row: (row[pucode_num] == "-").all(), axis=1)
        mds_id_all_dash = mds['MDS_ASMT_ID'][mds['all_dash'] == True].compute()
        mds_all_dash.append(mds_id_all_dash)
    mds_all_dash = pd.concat(mds_all_dash)

    select_mds_within_stay(main_merge_snf_mds_path, path['3_select_mds_within_snf_stay']['output'], mds_all_dash)
    # # nmedpar                134793
    # # no_end_date_medpar     17277 (13.6%)
    # # n_within_stay_no_entry 110209
    # # multiple_nh            98
    # # n_final                110134

