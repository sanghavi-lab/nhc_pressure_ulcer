# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will create final mds-hospital_claims denominator data for statistical analysis
# ----------------------------------------------------------------------------------------------------------------------#

####################################################################
# 1) merge comorbidity
# 2) construct resident-level and nursing home-level variables
# 3) delete other variables in data
# 4) remove missing values
####################################################################

import dask
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
import numpy as np
import time
import yaml
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 100)

years = range(2011, 2018)

## define paths
yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

## read in FIPS state code (a crosswalk file that maps each state name abbreviations with state code)
statecode = pd.read_csv(path['10_construct_model_data']['input_statecode'])

for n in range(len(path['10_construct_model_data']['input_denominator'])):
    ## read in denominators
    df = pd.read_csv(path['10_construct_model_data']['input_denominator'][n],
                     low_memory=False)
    df = df.astype({'ADMSN_DT': 'datetime64[ns]',
                    'DSCHRG_DT': 'datetime64[ns]',
                    'MCARE_ID': 'str',
                    'FAC_PRVDR_INTRNL_ID': 'str'})
    # print(df.shape[0]) #121058 363500

    ## read in comorbidity score
    comorbidity = pd.read_csv(path['10_construct_model_data']['input_comorbidity'][n])

    ## merge denominator with comorbidity score
    df = df.merge(comorbidity, left_on='MEDPAR_ID', right_on='patid')

    ## merges denominator with state code
    df = df.merge(statecode, left_on='STATE_CD', right_on='state', how='left')
    df['state_code'] = df['state_code'].apply(lambda x: '{:02d}'.format(x))

    ## 1) create individual-level variables
    # create dual indicator;
    # a patient is considered a dual if he/she is a full dual in any month of hospital admission year for pressure ulcer;
    dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]
    df = df.astype(dict(zip(dual, ['float']*12)))
    ## dual is True if the patient is a full dual in any month of the hospitalization year
    df['dual'] = df[dual].isin([2, 4, 8]).any(axis=1)

    ## create female and disability indicators
    df['female'] = df['sex'] == 2
    df['disability'] = df['ENTLMT_RSN_CURR'] == 1

    ## remove missing values
    cc_code = ['AMI', 'ALZH', 'ALZH_DEMEN', 'ATRIAL_FIB', 'CATARACT',
               'CHRONICKIDNEY', 'COPD', 'CHF', 'DIABETES', 'GLAUCOMA',
               'HIP_FRACTURE', 'ISCHEMICHEART', 'DEPRESSION',
               'OSTEOPOROSIS', 'RA_OA', 'STROKE_TIA', 'CANCER_BREAST',
               'CANCER_COLORECTAL', 'CANCER_PROSTATE', 'CANCER_LUNG', 'CANCER_ENDOMETRIAL', 'ANEMIA',
               'ASTHMA', 'HYPERL', 'HYPERP', 'HYPERT', 'HYPOTH']
    cc_code_final = [cc + '_final' for cc in cc_code]

    ## drop missing values before calculating nh-level characteristics
    df = df.dropna(
        subset=['BENE_ID', 'MEDPAR_ID', 'MEDPAR_YR_NUM', 'MCARE_ID', 'race_RTI', 'female', 'dual', 'age',
                'disability', 'combinedscore', 'short_stay', 'state_code', 'GNRL_CNTL_TYPE_CD',
                'pu_rate_medicare', 'highest stage', 'm_pu', 'm_pu_stage2_and_above'] + cc_code_final)
    # print(df.shape[0]) #119888 347507
    df = df[~(df['highest stage']==0)] #drop unspecified pressure ulcer stage

    # print(df.shape[0]) #115416 295290

    ## create race variables
    conditions_race = [(df['race_RTI']==0),
                        (df['race_RTI']==1),
                        (df['race_RTI']==2),
                        (df['race_RTI']==3),
                        (df['race_RTI']==4),
                        (df['race_RTI']==5),
                        (df['race_RTI']==6)]
    values_race     = ['missing', 'white', 'black', 'other', 'asian', 'hispanic', 'american indian']
    df['race_name'] = np.select(conditions_race, values_race)

    ## remove records where race is missing
    print((df['race_name']=='missing').mean()) #0.003
    df = df[df['race_name']!='missing']

    ## create 3 equal age_bins out of the continuous age variable according to age distribution
    # print(df.age.quantile([0.25, 0.5, 0.75]))
    # 0.25 68.0
    # 0.50 77.0
    # 0.75 86.0
    conditions_age = [(df['age']<68),
                      ((df['age']>=68) & (df['age']<77)),
                      ((df['age']>=77) & (df['age']<86)),
                      (df['age']>=86)]
    values_age     = ['<68', '68_76', '77_85', '>=86']
    df['age_bin'] = np.select(conditions_age, values_age)

    # ## 2.1) create facility-level variables for race and dual,
    # ##    e.g. the percentage of dual residents in a nursing home
    race_dummy = pd.get_dummies(df['race_name'])
    race_dummy_cols = race_dummy.columns
    race_mix_cols = ['white_percent', 'black_percent', 'other_percent', 'asian_percent', 'hispanic_percent', 'american_indian_percent']
    race_dev_cols = ['{}_dev'.format(i) for i in race_dummy_cols]
    df = pd.concat([df, race_dummy], axis=1)

    # ## drop observations if all race mix is zero
    # print('number of claims where all race mix zero:')
    # print(df[(df[race_mix_cols]==0).all(axis=1)].shape[0])

    # df = df[~(df[race_mix_cols]==0).all(axis=1)]
    # construct race_deviation and dual_deviation variable, which are resident-level variables for race and dual status
    df['dual_dev'] = df['dual'] - df['dual_percent']
    df['white_dev'] = df['white'] - df['white_percent']
    df['black_dev'] = df['black'] - df['black_percent']
    df['asian_dev'] = df['asian'] - df['asian_percent']
    df['hispanic_dev'] = df['hispanic'] - df['hispanic_percent']
    df['other_dev'] = df['other'] - df['other_percent']
    df['american indian_dev'] = df['american indian'] - df['american_indian_percent']


    ## 2.2) create facility-level variables for highest presusre ulcer stage
    ##      e.g. the percentage of residents with Stage 4 pressure ulcer as their highest stage pressure ulcer in a nursing home
    highest_pu_stage_dummy = pd.get_dummies(df['highest stage'], prefix='hpu_stage_w', dummy_na=True)
    highest_pu_stage_dummy_cols = highest_pu_stage_dummy.columns
    hpus_perc_wo_cols = ['{}_percentage_wo'.format(i) for i in highest_pu_stage_dummy_cols]
    hpus_perc_stage2_cols = ['{}_percentage_stage2'.format(i) for i in highest_pu_stage_dummy_cols]
    hpus_dev_cols = ['{}_dev'.format(i) for i in highest_pu_stage_dummy_cols]
    hpus_dev_cols2 = ['{}_dev2'.format(i) for i in highest_pu_stage_dummy_cols]

    df = pd.concat([df, highest_pu_stage_dummy], axis=1)

    ## create nh level variables of pressure ulcer mix by year
    ## 2.2a) percentage of highest pu stage in nursing home
    facility_pu_wo = df.groupby('MCARE_ID')[highest_pu_stage_dummy_cols].mean().reset_index()

    ## 2.2b) percentage of highest pu stage in nursing home including only stage 2 and above pressure ulcer (this is not used in later analysis)
    df_stage2 = df[df['highest stage']>=2]
    facility_pu_stage2 = df_stage2.groupby('MCARE_ID')[highest_pu_stage_dummy_cols].mean().reset_index()

    df = df.merge(facility_pu_wo, on='MCARE_ID', how='left', suffixes=['', '_percentage_wo'])
    df = df.merge(facility_pu_stage2, on='MCARE_ID', how='left', suffixes=['', '_percentage_stage2'])

    ## calculate the individual-level variable for highest PU stage
    for col in highest_pu_stage_dummy_cols:
        df['{}_dev'.format(col)] = df[col] - df['{}_percentage_wo'.format(col)]
        df['{}_dev2'.format(col)] = df[col] - df['{}_percentage_stage2'.format(col)]

    ## categorize nursing homes into small, medium and large
    ## according to their total number of residents

    # print(df['CNSUS_RSDNT_CNT'].quantile([0.33, 0.66]))
    # # # 0.33     92.0
    # # 0.66    132.0

    conditions_size = [(df['CNSUS_RSDNT_CNT'] < 92),
                       ((df['CNSUS_RSDNT_CNT'] >= 92) & (df['CNSUS_RSDNT_CNT'] < 132)),
                       ((df['CNSUS_RSDNT_CNT'] >= 132))]
    values_size = ['s', 'm', 'l']
    df['size'] = np.select(conditions_size, values_size)

    ## categorize nursing homes into northeast, midwest, south or west nursign homes by their location
    conditions_region = [(df['state_code'].isin(["09", "23", "25", "33", "44" ,"50", "34" ,"36", "42"])),
                         (df['state_code'].isin(["18", "17", "26", "39", "55", "19", "20", "27", "29", "31", "38", "46"])),
                         (df['state_code'].isin(["10", "11", "12", "13", "24", "37", "45", "51", "54", "01", "21", "28", "47", "05", "22", "40", "48", '78'])),
                         (df['state_code'].isin(["04", "08", "16", "35", "30", "49", "32", "56", "02", "06", "15", "41","53"]))]
    values_region = ['northeast', 'midwest', 'south', 'west']
    df['region'] = np.select(conditions_region, values_region)

    ## categorize nursing homes into for-profit, non-profit and government based on their ownership
    conditions_ownership = [(df['GNRL_CNTL_TYPE_CD'].isin([1, 2, 3])),
                            (df['GNRL_CNTL_TYPE_CD'].isin([4, 5, 6])),
                            (df['GNRL_CNTL_TYPE_CD'].isin([7,8,9,10,11,12]))]
    values_ownership = ['for-profit', 'non-profit', 'government']
    df['ownership'] = np.select(conditions_ownership, values_ownership, default='other')
    # print(df['ownership'].value_counts())

    ## m_pu change from T/F to 1/0
    df['m_pu'] = df['m_pu'].map({True: 1,
                                 False: 0})
    df['m_pu_stage2_and_above'] = df['m_pu_stage2_and_above'].map({True: 1, False: 0})
    # ## keep variables needed for the model
    m_pu_col = \
        ['M0100A_RISK_VSBL_CD',
         'M0300A_STG_1_ULCR_NUM',
         'M0300B1_STG_2_ULCR_NUM',
         'M0300C1_STG_3_ULCR_NUM',
         'M0300D1_STG_4_ULCR_NUM',
         'M0300E1_UNSTGBL_ULCR_DRSNG_NUM',
         'M0300F1_UNSTGBL_ULCR_ESC_NUM',
         'M0300G1_UNSTGBL_ULCR_DEEP_NUM']
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))] + ['ADMTG_DGNS_CD']
    quality_cols = ['long_stay_pu_score', 'short_stay_pu_score', 'overall_rating', 'quality_rating', 'long_stay_pu_score_avg', 'short_stay_pu_score_avg', 'overall_rating_avg', 'quality_rating_avg']

    use_cols = ['BENE_ID', 'MEDPAR_ID', 'MDS_ASMT_ID', 'MEDPAR_YR_NUM', 'MCARE_ID', 'race_name', 'female', 'age',
                'dual', 'dual_dev', 'age_bin', 'disability', 'combinedscore', 'short_stay', 'CNSUS_MDCR_CNT', 'CNSUS_RSDNT_CNT', 'medicare_count',
                'dual_percent', 'size', 'region', 'ownership', 'pu_rate_medicare',
                'm_pu', 'm_pu_stage2_and_above', 'highest stage'] +\
               cc_code_final + race_mix_cols + race_dev_cols +\
               hpus_perc_wo_cols + hpus_dev_cols + hpus_perc_stage2_cols + hpus_dev_cols2
    df_final = df[use_cols + m_pu_col + dcode + quality_cols]
    df_model_final = df[use_cols]

    print(df_final.shape[0]) #115032 294373
    ## data in output_denominator2 is used only for modelling
    df_final.to_csv(path['10_construct_model_data']['output_denominator1'][n], index=False)
    df_model_final.to_csv(path['10_construct_model_data']['output_denominator2'][n], index=False)
    print(df_model_final.head())

## remove obs where all mds M0300 items are coded as "-"
from dask.distributed import Client
client = Client("10.50.86.250:33380")

years = list(range(2011, 2018))
## define M0300 items
pucode_num = ['M0210_STG_1_HGHR_ULCR_CD', 'M0300A_STG_1_ULCR_NUM', 'M0300B1_STG_2_ULCR_NUM',
              'M0300C1_STG_3_ULCR_NUM', 'M0300D1_STG_4_ULCR_NUM', 'M0300E1_UNSTGBL_ULCR_DRSNG_NUM',
              'M0300F1_UNSTGBL_ULCR_ESC_NUM', 'M0300G1_UNSTGBL_ULCR_DEEP_NUM']
mds_all_dash = []
for i in range(len(years)):
    ## read in MDS raw data
    mds = dd.read_parquet(path['processMDS']['input'][i])
    cols = [col.upper() for col in mds.columns]
    mds.columns = cols
    ## create a column "all_dash", which is True if all M0300 items are coded as dash
    mds['all_dash'] = mds.apply(lambda row: (row[pucode_num]=="-").all(), axis=1)
    ## get the MDS id for MDS with all M0300 items coded as dash
    mds_id_all_dash = mds['MDS_ASMT_ID'][mds['all_dash']==True].compute()
    mds_all_dash.append(mds_id_all_dash)
mds_all_dash = pd.concat(mds_all_dash)

for n in range(len(path['10_construct_model_data']['output_denominator1'])):
    ## read in mds-hospital_claims final data
    if n==1:
        df = pd.read_csv(path['10_construct_model_data']['output_denominator1'][n], low_memory=False)
        print(df.shape[0]) #115032 294373
        ## read in mds-hospital_claims final model data
        df_model = pd.read_csv(path['10_construct_model_data']['output_denominator2'][n], low_memory=False)
        df = df[~df['MDS_ASMT_ID'].isin(mds_all_dash)]
        print(df.shape[0]) #114729 293617
        ## remove MDS with all M0300 items coded as dash
        df_model = df_model[~df_model['MDS_ASMT_ID'].isin(mds_all_dash)]
        ## write to csv
        df.to_csv(path['10_construct_model_data']['output_denominator1'][n], index=False)
        df_model.to_csv(path['10_construct_model_data']['output_denominator2'][n], index=False)
