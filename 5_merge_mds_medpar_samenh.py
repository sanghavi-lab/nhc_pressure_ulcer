# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will merge mds discharge assessments and medpar primary and secondary hospital claims for
#              patients who had pu during NH residency and returned to the same NH within 1 day of hospital discharge
# ----------------------------------------------------------------------------------------------------------------------#

import dask
import os
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
from fastparquet import ParquetFile

def merge_mds_medpar(pu_in_claims_path, pu_claims_samenh_path, concat_rank_path, write_path):
    ## define data type for columns
    dtypes =  {'PRVDR_NUM': 'object',
               'DGNS_2_CD': 'object',
               'DGNS_24_CD': 'object',
               'DGNS_25_CD': 'object',
               'POA_DGNS_20_IND_CD': 'object',
               'POA_DGNS_21_IND_CD': 'object',
               'POA_DGNS_22_IND_CD': 'object',
               'POA_DGNS_23_IND_CD': 'object',
               'POA_DGNS_24_IND_CD': 'object',
               'POA_DGNS_25_IND_CD': 'object'
               }
    ## read in claims data for patients who had PU during NH residency
    df_pu_in   = \
        dd.read_parquet(pu_in_claims_path,
                        dtype=dtypes)
    ## read in concat rank of MDS and Claims data
    concat_rank = \
        dd.read_csv(concat_rank_path,
                    dtype=dtypes,
                    low_memory=False)
    ## drop HMO columns from MBSF in concat-rank data
    hmo = ['HMO_IND_{:02d}'.format(i) for i in range(1, 13)]
    concat_rank = concat_rank.drop(columns=hmo)

    ## read in claims data for patients returned to the same NH within 1 day
    samenh = \
        dd.read_csv(pu_claims_samenh_path,
                    dtype=dtypes,
                    low_memory=False)

    ## These claims are medpar claims for patients who had pressure ulcer during nursing home stay.
    ## Look back from these claims to identify discharge assessment prior to hospitalization,
    ## and merge them with mds discharge assessments

    ## select hospital claims id for patients who had pressure ulcer during nursing home stay
    unique_claim_id       = df_pu_in[['BENE_ID', 'MEDPAR_ID', 'rank']]
    ## create a copy of hospital claims id
    unique_mds_id         = unique_claim_id.copy()
    ## rename the medpar_id column to indicate this would be the MDS discharge assessment to merge with the hospital claim
    unique_mds_id         = unique_mds_id.rename(columns={'MEDPAR_ID': 'm_MEDPAR_ID'})
    ## minus 1 from the rank variable to calculate the rank of the MDS prior to hospital claim
    unique_mds_id['rank'] = unique_claim_id['rank'] - 1

    ## select the mds discharge assessments prior to hospital claims using BENE_ID and rank
    mds = concat_rank.merge(unique_mds_id, on=['BENE_ID', 'rank'], how='right')

    ## drop medpar columns in mds
    dcode   = ['DGNS_{}_CD'.format(i) for i in list(range(1,26))]
    decode  = ['DGNS_E_{}_CD'.format(i) for i in list(range(1,13))]
    dvcode = ['DGNS_VRSN_CD_{}'.format(i) for i in list(range(1, 26))]
    dvecode = ['DGNS_E_VRSN_CD_{}'.format(i) for i in list(range(1, 13))]
    poacode = ['POA_DGNS_{}_IND_CD'.format(i) for i in list(range(1,26))]
    dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]
    mbsf_col = ['death_dt', 'ESRD_IND', 'age',
               'race_RTI', 'BENE_ENROLLMT_REF_YR', 'sex',
               'ENTLMT_RSN_ORIG', 'ENTLMT_RSN_CURR']

    h_col = ['MEDPAR_ID', 'MEDPAR_YR_NUM', 'PRVDR_NUM', 'ADMSN_DT', 'DSCHRG_DT',
             'DSCHRG_DSTNTN_CD', 'SS_LS_SNF_IND_CD', 'BENE_DSCHRG_STUS_CD', 'DRG_CD',
             'ADMTG_DGNS_CD'] + dcode + decode + poacode + dvcode + dvecode + dual + mbsf_col

    mds = mds.drop(columns=h_col)
    ## rename the MEDPAR_ID variable in mds to merge with hospital claim
    mds = mds.rename(columns={'m_MEDPAR_ID': 'MEDPAR_ID'})

    ## merge mds discharge assessments with medpar claims based on MEDPAR_ID
    merge = mds.merge(df_pu_in, on='MEDPAR_ID', how='inner', suffixes=['', '_h'])

    ## drop duplicated columns in merged data
    merge = merge.drop(columns=[x for x in merge.columns if x.endswith('_h')])

    ## select merged mds-hospital_claims for patients who return to the same nursing home within 1 day
    ## and write to parquet
    merge = merge[merge.MEDPAR_ID.isin(list(samenh.MEDPAR_ID))]
    merge.to_parquet(write_path, write_index=False)

    print('merged mds and medpar is written to ', write_path)


if __name__ == '__main__':
    import yaml
    from dask.distributed import Client
    client = Client("10.50.86.250:42654")
    ## define intput and output path
    yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))
    ## run the function to merge mds discharge assessments with primary and secondary hospital claims
    merge_mds_medpar(path['3_select_pu_in']['output_main'],
                     path['4_select_samenh_medpar']['output_main'][0] + '*.csv',
                     path['2_concat_mds_medpar']['output_main'][1] + '*.csv',
                     path['5_merge_mds_medpar_samenh']['output_main'])
    merge_mds_medpar(path['3_select_pu_in']['output_spu'],
                     path['4_select_samenh_medpar']['output_spu'][0] + '*.csv',
                     path['2_concat_mds_medpar']['output_spu'][1] + '*.csv',
                     path['5_merge_mds_medpar_samenh']['output_spu'])

