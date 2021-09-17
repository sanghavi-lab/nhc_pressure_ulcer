# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will select patients who return to the same nursing home after hospitalization within 1 day.
# ----------------------------------------------------------------------------------------------------------------------#


import dask
import os
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
import re
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)

def create_pu_in_same_nh(concat_rank_path, medpar_pu_in_path, write_path_medpar, write_path_mdspre, write_path_mdspost ):
    ## create variables for flowchart calculation
    n_nh_back = []
    n_nh_back_one_day = []
    n_nh_not_back = []
    n_nh_not_back_one_day = []
    n_nh_notsamenh = []
    n_samenh_oneday = []

    ## read in medpar primary and secondary hospital claims for patients who had PU during NH residency
    medpar_pu_in = dd.read_parquet(medpar_pu_in_path)

    ## extract medpar id as unique identification
    medpar_pu_in_id = list(medpar_pu_in['MEDPAR_ID'])

    ## for each partition of concat rank data
    ## 1) combine contiguous hospital stays for each beneficiary
    ## 2) from each medpar claims, scan the next record to determine if the patient returns to the same nh
    cr_path = os.listdir(concat_rank_path)
    for i in range(len(cr_path)):

        ## read in concat_rank data
        concat_rank = pd.read_csv(concat_rank_path + cr_path[i], low_memory=False)
        ## extract the number in the name of the data file (used for creating output data file)
        x = re.findall(r'\d+', cr_path[i])
        ## drop HMO columns from MBSF in concat_rank data
        hmo = ['HMO_IND_{:02d}'.format(i) for i in range(1, 13)]
        concat_rank = concat_rank.drop(columns=hmo)

        ## assign datetime data type to date columns
        concat_rank = concat_rank.astype({'TRGT_DT': 'datetime64[ns]',
                                          'ADMSN_DT': 'datetime64[ns]',
                                          'DSCHRG_DT': 'datetime64[ns]',
                                          'rd': 'datetime64[ns]'})
        ## separate mds records and hospital claims
        mmr = concat_rank[concat_rank['r']=='m'] ## MDS
        hmr = concat_rank[concat_rank.r == 'h'] ## hospital claims

        # <editor-fold desc="COMBINE CONTIGUOUS HOSPITALIZATIONS">
        ## for each medpar claims, look for its next record
        ## to see if it is a contiguous hospital stay, and
        ## combine all contiguous hospital stays

        ## sort hospital claims for each bene by rank in descending order
        hmr = hmr.\
            groupby('BENE_ID').\
            apply(lambda g: g.sort_values('rank', ascending=False)).\
            reset_index(drop=True)
        ## record values from the next hospital claim
        hmr_shift = \
            hmr[['MEDPAR_ID', 'BENE_ID', 'ADMSN_DT', 'DSCHRG_DT', 'rank']].\
                groupby('BENE_ID').\
                shift(1)
        ## merge the next hospital claim with the current claim
        hmr = hmr.join(hmr_shift.rename(columns=lambda x: x+"_next"))

        ## select claims with contiguous hostpializations
        ## if hospital discharge date is equal to the next hospital admission date,
        ## there are two claims for the contiguous hospital stay
        sub_bene = hmr[hmr['DSCHRG_DT'] == hmr['ADMSN_DT_next']]

        ## remove the next hospital claims in the contiguous hospitalization
        hmr_sub = hmr[~hmr.MEDPAR_ID.isin(sub_bene.MEDPAR_ID_next)]

        def change_dischrg_date(row):
            ## if the medpar claim is the earlier claim of the contiguous hospitalizations
            ## change its discharge date to the discharge date on the next hospital claims
            ## to combine two hospital claims into one
            if (sub_bene.MEDPAR_ID == row['MEDPAR_ID']).any():
                row['DSCHRG_DT'] = row['DSCHRG_DT_next']
            return row
        ## apply the function to each claim
        hmr_new = hmr_sub.apply(change_dischrg_date, axis=1)
        # </editor-fold>

        ## concat new hospital claims with combined contiguous hospitalizations and mds
        df = pd.concat([mmr, hmr_new]).sort_values(['BENE_ID', 'rank'])
        df = df.drop('ADMSN_DT_next', axis=1)

        ## from each hospital claim, look for the next record
        ## to determine if the patient returns to nursing home

        ## sort hospital claims and MDS by rank in descending order
        df = df.groupby('BENE_ID').apply(lambda g: g.sort_values('rank', ascending=False)).reset_index(drop=True)

        ## record values from the next record (maybe a hospital claim or a mds)
        df_shift = df[
            ['BENE_ID', 'ADMSN_DT', 'FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'rd', 'r', 'A0310F_ENTRY_DSCHRG_CD', 'TRGT_DT']].groupby(
            'BENE_ID').shift(1)
        ## merge the next record with the current record
        df = df.join(df_shift.rename(columns=lambda x: x + "_next"))

        ## assign a new "ranking" value because some medpar claims are deleted for contiguous hospitalizations
        df = df.groupby('BENE_ID').apply(lambda g: g.sort_values('rank')).reset_index(drop=True)
        df['rank_new'] = df.groupby('BENE_ID').cumcount().reset_index(drop=True)

        ## select medpar claims
        df_h = df.copy(deep=True)
        df_h = df_h.loc[df_h.r == 'h']

        # # <editor-fold desc="USEFUL FOR FLOWCHART CALCULATION">
        # select hospital claims for residents who had pressure ulcer during nursing home residency
        df_h_pu = df_h[df_h.MEDPAR_ID.isin(medpar_pu_in_id)]
        # divide patients to those who returned to a nursing home after hospital discharge and those who didn't
        # by looking at the record type of the next record of a hospital claim (is it a MDS?)
        df_h_not_back = df_h_pu[df_h_pu.r_next!='m']
        df_h_back     = df_h_pu[df_h_pu.r_next=='m']
        # for patients who returned to MDS after hospital discharge, select those who returned within 1 day
        df_h_back.loc[:, 'dischrg_nh_elapse'] = (df_h_back['TRGT_DT_next'] - df_h_back['DSCHRG_DT']).dt.days
        df_h_back_one_day = df_h_back[(df_h_back['dischrg_nh_elapse']<2) &
                                      (df_h_back['dischrg_nh_elapse']>-1)]

        ## calculate the number of patients who were nh residents during pressure ulcer and went back to the nursing home
        df_h_back_pu_one_day = df_h_back_one_day[df_h_back_one_day.MEDPAR_ID.isin(medpar_pu_in_id)]
        n_nh_back_one_day.append(df_h_back_pu_one_day.MEDPAR_ID.unique().size)

        if i==199:
            print('the number of claims for residents went back to nursing home within 1 day')
            print(sum(n_nh_back_one_day))
        # ## </editor-fold>


        ############################################
        ## check if there are three contiguous hospital stays
        ## there are at least one three contiguous hospital stays in spu, check it later
        # three = df[df['DSCHRG_DT']==df['ADMSN_DT_next']].shape[0]
        # if three > 0:
        #     print('\nthere are three contiguous hospital stays in ', i, '\n')
        #     print(df[df['DSCHRG_DT']==df['ADMSN_DT_next']].BENE_ID)
        ############################################

        # <editor-fold desc="USEFUL FOR FLOW CHART CALCULATION">
        ## from patients who returned to nursing home within 1 day of hospital discharge
        ## select claims for patients who didn't to the same nursing home
        medpar_not_samenh = \
            df_h_back_one_day[
                (df_h_back_one_day['FAC_PRVDR_INTRNL_ID_lag'] != df_h_back_one_day['FAC_PRVDR_INTRNL_ID_next']) |
                (df_h_back_one_day['STATE_CD_lag'] != df_h_back_one_day['STATE_CD_next'])
            ]
        ## further select claims for patients who had pressure ulcer during their nh stay
        medpar_not_samenh_pu = medpar_not_samenh[medpar_not_samenh.MEDPAR_ID.isin(medpar_pu_in_id)]
        ## calculate the number of claims for atients who had pressure ulcer during their nh stay but didn't return
        ## to the same nursing home
        n_nh_notsamenh.append(medpar_not_samenh_pu.MEDPAR_ID.unique().size)
        if i==199:
            print('the number of claims for residents returned to a different NH after hospitalization')
            print(sum(n_nh_notsamenh))
        # # </editor-fold>

        ## from each hospital claim, look for nursing home facility number (FAC_PRVDR_INTRNL_ID_lag) and state code (STATE_CD_lag)
        ## of the next record to determine if the patient returns to the same nursing home as the previous one,
        ## and select medpar claims for patients returning to the same nh within 1 day after hospitalization

        ## calculate the number of days between hospital discharge and the next nursing home admission date
        df_h.loc[:, 'dischrg_nh_elapse'] = (df_h['TRGT_DT_next'] - df_h['DSCHRG_DT']).dt.days
        ## select hospital claims for patients who returned to the same nh within 1 day
        medpar_same_nh_1_day = \
            df_h[(df_h['FAC_PRVDR_INTRNL_ID_lag'] == df_h['FAC_PRVDR_INTRNL_ID_next']) &\
                 (df_h['STATE_CD_lag'] == df_h['STATE_CD_next'])&\
                 (df_h['dischrg_nh_elapse'] < 2) &\
                 (df_h['dischrg_nh_elapse'] > -1)]

        ## further select claims for patients who had pressure ulcers during NH stay
        medpar_same_nh_1_day = medpar_same_nh_1_day[medpar_same_nh_1_day.MEDPAR_ID.isin(medpar_pu_in_id)]

        ## write medpar claims for patients returning to the same nh to csv
        ### drop mds columns in data
        medpar_same_nh_1_day = medpar_same_nh_1_day.dropna(axis=1, how='all')

        # # <editor-fold desc="FLOW CHART CALCULATION">
        n_samenh_oneday.append(medpar_same_nh_1_day.shape[0])
        if i==199:
            print('the number of claims for NH residents returning to the same NH within 1 day after hospitalization is: ')
            print(sum(n_samenh_oneday))
        # # </editor-fold>

        ## write hospital claims to csv
        # medpar_same_nh_1_day.to_csv(write_path_medpar + 'medpar_same_nh{}.csv'.format(x[0]), index=False)

        ## bene_id and rank_new would be the unique identification for each mds assessment
        bene_same_nh = medpar_same_nh_1_day[['BENE_ID', 'rank_new']]

        # ## write mds pre and post medpar claims for patients return to the same nh to csv
        # ## check if there are same number of pu records in pre-hospitalization mds assessments
        # ## as that of pu records in post-hospitalization mds
        mdspre_same_nh = bene_same_nh.copy(deep=True)
        mdspost_same_nh = bene_same_nh.copy(deep=True)
        mdspre_same_nh.loc[:,'rank_new'] = mdspre_same_nh['rank_new'] - 1
        mdspost_same_nh.loc[:,'rank_new'] = mdspost_same_nh['rank_new'] + 1

        mdspre_same_nh = df.merge(mdspre_same_nh, on=['BENE_ID', 'rank_new'], how='inner')
        mdspre_same_nh = mdspre_same_nh.dropna(axis=1, how='all')
        # mdspre_same_nh.to_csv(write_path_mdspre + '{}.csv'.format(x[0]), index=False)

        mdspost_same_nh = df.merge(mdspost_same_nh, on=['BENE_ID', 'rank_new'], how='inner')
        mdspost_same_nh = mdspost_same_nh.dropna(axis=1, how='all')
        # mdspost_same_nh.to_csv(write_path_mdspost + '{}.csv'.format(x[0]), index=False)
        print(i)


if __name__ == '__main__':
    import yaml
    from dask.distributed import Client
    client = Client("10.50.86.250:50521")
    ## define input and output path
    yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

    #458904 final sample size
    # the number of claims for residents went back to nursing home within 1 day
    # 135840
    # the number of claims for residents returned to a different NH after hospitalization
    # 14643
    # the number of claims for NH residents returning to the same NH within 1 day after hospitalization is:
    # 121197
    create_pu_in_same_nh(path['4_select_samenh_medpar']['input_main_concat_rank'],
                         path['4_select_samenh_medpar']['input_main_pu_in'],
                         path['4_select_samenh_medpar']['output_main'][0],
                         path['4_select_samenh_medpar']['output_main'][1],
                         path['4_select_samenh_medpar']['output_main'][2])

    # 406784
    # 42923
    # 363861
    create_pu_in_same_nh(path['4_select_samenh_medpar']['input_spu_concat_rank'],
                         path['4_select_samenh_medpar']['input_spu_pu_in'],
                         path['4_select_samenh_medpar']['output_spu'][0],
                         path['4_select_samenh_medpar']['output_spu'][1],
                         path['4_select_samenh_medpar']['output_spu'][2])
