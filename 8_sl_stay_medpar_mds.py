# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will classify residents in mds-hospital_claims smaple into long-stay and short-stay categories
#              by looking for a 5-day PPS MDS assessment within 100 days prior to NH discharge.
#              If there is one, the resident is classified as short-stay, and long-stay otherwise.
# ----------------------------------------------------------------------------------------------------------------------#

import dask
import os
from datetime import datetime
import dask.dataframe as dd
import pandas as pd

def separate_sl_stay(concat_rank_path, merge_samenh_path, wpath, sensitivity=False):
    ## read in concated mds and claims data
    concat_rank  = \
        dd.read_csv(concat_rank_path,
                    dtype={'PRVDR_NUM': 'object',
                           'DGNS_2_CD': 'object'},
                    low_memory=False)
    ## select mds assessments
    concat_rank_m = \
        concat_rank[concat_rank.r=='m']
    ## read in mds-hospital_claims data
    if sensitivity:
        merge_samenh = dd.read_parquet(merge_samenh_path)
    else:
        merge_samenh = \
            [dd.read_parquet(path) for path in merge_samenh_path]
        merge_samenh = dd.concat(merge_samenh)

    ## for each discharge assessments,
    ## look back to see if there is a 5-day PPS within 100 days
    ## If there is one, the resident is short-stay

    ## 1) for each discharge assessment, merge with all assessments on bene_id
    merge_mds = \
        merge_samenh[['BENE_ID', 'MEDPAR_ID', 'TRGT_DT']].\
            merge(concat_rank_m[['BENE_ID', 'TRGT_DT', 'A0310B_PPS_CD', 'A1700_ENTRY_TYPE_CD', 'A1600_ENTRY_DT']],
                  on='BENE_ID',
                  how='inner',
                  suffixes=['', '_s'])

    ## 2) keep only mds assessments no later than the discharge assessment
    merge_mds = \
        merge_mds.astype({'TRGT_DT': 'datetime64[ns]',
                          'TRGT_DT_s': 'datetime64[ns]',
                          'A1600_ENTRY_DT': 'datetime64[ns]'})

    merge_mds = merge_mds[merge_mds.TRGT_DT >= merge_mds.TRGT_DT_s]

    ## 3) select mds assessments within 100 days from the discharge assessment
    merge_mds_sub1 = \
        merge_mds[
            ((merge_mds.TRGT_DT - merge_mds.TRGT_DT_s).dt.days <= 101) &
            ((merge_mds.TRGT_DT - merge_mds.TRGT_DT_s).dt.days >= 0)
        ]
    ## 4) create short_stay indicator which equals 1 if there is a 5-day PPS
    merge_mds_sub1['short'] = \
        merge_mds_sub1.A0310B_PPS_CD == 1
    ## group by each claim to see if there is any 5-day PPS MDS linked to the claim
    short_sum = \
        merge_mds_sub1.\
            groupby('MEDPAR_ID')['short'].\
            sum().reset_index().rename(columns={'short':'short_sum'})
    ## if short_sum is larger than 0, it means there is at least one 5-day PPS MDS linked to the claim
    ## within 100 days, so the bene is a short-stay resident in nursing homes
    short_sum['short_stay'] = short_sum.short_sum > 0

    ## merge the short_stay indicator with mds-hospital_claims data
    merge_samenh_sl = \
        merge_samenh.merge(short_sum[['MEDPAR_ID', 'short_stay']],
                           on='MEDPAR_ID',
                           how='left')
    ## write to csv
    merge_samenh_sl.compute().to_csv(wpath, index=False)

    return merge_samenh_sl


if __name__ == '__main__':
    import yaml
    from dask.distributed import Client
    client = Client("10.50.86.250:52851")

    yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

    years = range(2011, 2018)
    
    ## primary hospital admission sample
    df = separate_sl_stay(path['8_sl_stay_medpar_mds']['input_concat_rank']['main'],
                          path['8_sl_stay_medpar_mds']['input_main'],
                          path['8_sl_stay_medpar_mds']['output_main'])
    ## secondary hopistal admission sample
    df2 = separate_sl_stay(path['8_sl_stay_medpar_mds']['input_concat_rank']['spu'],
                            path['8_sl_stay_medpar_mds']['input_spu'],
                            path['8_sl_stay_medpar_mds']['output_spu'])
                            
    ## sensitivity analysis sample without the readmission restriction
    sensitivity1 = separate_sl_stay(path['2_concat_mds_medpar']['output_main'][1] + '*.csv',
                                   path['sensitivity_noreturn']['output'][0],
                                   path['8_sl_stay_medpar_mds']['output_sensitivity'][0],
                                   sensitivity=True)
    sensitivity2 = separate_sl_stay(path['2_concat_mds_medpar']['output_spu'][1] + '*.csv',
                                    path['sensitivity_noreturn']['output'][1],
                                    path['8_sl_stay_medpar_mds']['output_sensitivity'][1],
                                    sensitivity=True)



