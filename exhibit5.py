# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will create Table 5 Correlations between claims-based PU rate and NHC ratings and QMs
# ----------------------------------------------------------------------------------------------------------------------#

## THIS CODE IS TO GENERATE THE FIFTH EXHIBIT IN THE PAPER:
## CLAIMS-BASED PRESSURE ULCER RATE WAS CALCULATED FOR EACH NURSING HOME EACH YEAR IN 9_merge_star_ratings.py;
## DIVIDE CLAIMS-BASED PRESSURE ULCER RATE INTO 5 QUINTILES AND SUMMARIZE NHC MDS-BASED MEASURES WITHIN EACH QUINTILE;
## CALCULATE THE CORRELATION BETWEEN CLAIMS-BASED PRESSURE ULCER RATE AND MDS STAR RATING AND PRESSURE ULCER QUALITY MEASURES;


## NOTE IN YEAR 2011 AND 2012
# 199.0: small denominator suppression:
#   <30 for the long stay measures (401-419) and
#   <20 for the short stay measures (424-434)
# 201.0: score is missing
# (either no score generated for a particular measure or
#  facility is not matched to QM data, in which case all measures for a facility would be set to 201.0)


import pandas as pd
import numpy as np
import yaml

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)

# <editor-fold desc="FUNCTIONS">
## define MDS PU ITEMS
m_pu_col = \
        ['M0100A_RISK_VSBL_CD',
         'M0300A_STG_1_ULCR_NUM',
         'M0300B1_STG_2_ULCR_NUM',
         'M0300C1_STG_3_ULCR_NUM',
         'M0300D1_STG_4_ULCR_NUM',
         'M0300E1_UNSTGBL_ULCR_DRSNG_NUM',
         'M0300F1_UNSTGBL_ULCR_ESC_NUM',
         'M0300G1_UNSTGBL_ULCR_DEEP_NUM']

def identify_pu_stage(code):
    ## this function maps ICD codes to pressure ulcer stages
    ## 1 - 4 corresponds to stage 1-4
    ## 0 is unspecified stage
    ## 5 is unstageable
    ## 6 is deep dissue damage which only included in ICD-10 after 2017
    ## there are a few cases with eroneous pressure ulcer stage coding, which is captured
    ## by the try/except statements
    code = str(code) ## type(np.nan) = float
    if code != '':
        stage = np.nan
        if code.startswith('7072') or code.startswith('L89'):
            if code == '70721' or (code.startswith('L89') & code.endswith('1')):
                stage = 1
            elif code == '70722' or (code.startswith('L89') & code.endswith('2')):
                stage = 2
            elif code == '70723' or (code.startswith('L89') & code.endswith('3')):
                stage = 3
            elif code == '70724' or (code.startswith('L89') & code.endswith('4')):
                stage = 4
            elif code == '70720':
                stage = 0 ## unspecified
            elif code == '70725':
                stage = 5 ## unstageable
            elif code.startswith('L894') or code.startswith('L899'):
                if code.endswith('0'):
                    stage = 0 ## unspecified
                elif code.endswith('5'):
                    stage = 5 ## unstageable
                elif code.endswith('6'):
                    stage = 6 ## deep tissue damage
            elif code.startswith('L89') & code.endswith('0'):
                stage = 5 ## unstageable
            elif code.startswith('L89') & code.endswith('6'):
                stage = 6 ## deep tissue damage
            elif code.startswith('L89') & code.endswith('9'):
                stage = 0 ## unspecified
        else:
            stage = np.nan
    else:
        stage = np.nan
    return stage

def count_pu_stage(row):
    # This function counts the number of pressure ulcers at each stage
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]
    stage = [identify_pu_stage(i) for i in list(row[dcode])]

    stage0 = sum([j == 0 for j in stage])
    stage1 = sum([j == 1 for j in stage])
    stage2 = sum([j == 2 for j in stage])
    stage3 = sum([j == 3 for j in stage])
    stage4 = sum([j == 4 for j in stage])
    stage5 = sum([j == 5 for j in stage])
    stage6 = sum([j == 6 for j in stage])

    row['stage1'] = (stage1 > 0)
    row['stage2'] = (stage2 > 0)
    row['stage3'] = (stage3 > 0)
    row['stage4'] = (stage4 > 0)
    row['unspecified'] = (stage0 > 0)
    row['unstageable'] = (stage5 > 0)
    row['deep tissue damage'] = (stage6 > 0)

    return row

def assign_highest_stage(row):
    ## this function assigns the highest pressure ulcer stage in diagnosis codes to each claim
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))] + ['ADMTG_DGNS_CD']
    stage_list = [identify_pu_stage(i) for i in list(row[dcode])]
    # if len([i for i in stage_list if i==None])!=0: ## it means some pressure ulcer stage code is wrong, and captured by the try/except statement in identify_pu_stage function; since it prints out the eroneous code, the returned value is None
    #     row['highest stage'] = row['MEDPAR_ID']
    if np.all(np.isnan([i for i in stage_list if i!=None])):
        row['highest stage'] = np.NaN
    else:
        row['highest stage'] = np.nanmax(stage_list)
    return row

# </editor-fold>

years = range(2011, 2018)
## define paths
yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

## read in mds-hospital_claims(primary) data
main = pd.read_csv(path['exhibits']['input'] + 'main_data_final.csv')
main = main.astype({'MCARE_ID': 'str'})
# print(main.shape[0]) #114729

# # <editor-fold desc="CALCULATE CORRELATION BETWEEN CLAIMS-BASED PU RATES AND NHC COMPARE MEASURES">
## define QM and rating related columns
qmcols = ['pu_rate_medicare', 'long_stay_pu_score', 'short_stay_pu_score', 'overall_rating', 'quality_rating', 'long_stay_pu_score_avg', 'short_stay_pu_score_avg', 'overall_rating_avg', 'quality_rating_avg']


## remove missing values
df = main.dropna(subset=['pu_rate_medicare', 'long_stay_pu_score', 'short_stay_pu_score', 'overall_rating', 'quality_rating'], how='any')
# print(df.shape[0]) #96950
## keep unique nursing home-year data
df = df.drop_duplicates(subset=['MCARE_ID', 'MEDPAR_YR_NUM'])

## divide claims-based pu rate to quintiles;
## calculate mean and 10th/90th percentile within each quintile
## compute percent of NHs with 4/5 star Overall/Quality ratings within each quintile
## compute average Overall and Quality star-rating, and MDS-based pressure ulcer scores within each quintile
for n in range(len(years)):
    ## select data for each year
    rating = df[df.MEDPAR_YR_NUM==years[n]][qmcols + ['MCARE_ID']]
    ## divide claims-based pressure ulcer rate into 5 quantiles
    rating_quintile = rating['pu_rate_medicare'].quantile([0.2, 0.4, 0.6, 0.8]).to_list()

    conditions_quintile = [(rating['pu_rate_medicare'] < rating_quintile[0]),
                            ((rating['pu_rate_medicare'] >= rating_quintile[0]) & (rating['pu_rate_medicare'] < rating_quintile[1])),
                            ((rating['pu_rate_medicare'] >= rating_quintile[1]) & (rating['pu_rate_medicare'] < rating_quintile[2])),
                            ((rating['pu_rate_medicare'] >= rating_quintile[2]) & (rating['pu_rate_medicare'] < rating_quintile[3])),
                            (rating['pu_rate_medicare'] >= rating_quintile[3])
                            ]
    ## assign quintile names
    values_quintile = ['quintile1', 'quintile2', 'quintile3', 'quintile4', 'quintile5']
    rating['quintile'] = np.select(conditions_quintile, values_quintile)
    ## calculate the number of nursing homes within each quintile for 2014 and 2017
    if (n==3) | (n==6):
        rating.groupby(['quintile', 'size'])['MCARE_ID'].count().to_csv(path['exhibits']['input'] + 'analysis/correlation/nh_size{}.csv'.format(years[n]))
        print(rating.groupby('quintile')['pu_rate_medicare'].count())
    ## calculate the mean claims-based pu rate and NHC measures within each quintile
    rating_mean = rating.groupby('quintile').mean().reset_index()
    rating_mean.columns = ['quintile', 'pu_rate_medicare_mean', 'long_stay_pu_score_mean',
                            'short_stay_pu_score_mean', 'overall_rating_mean', 'quality_rating_mean',
                            'long_stay_pu_score_avg_mean', 'short_stay_pu_score_avg_mean',
                            'overall_rating_avg_mean', 'quality_rating_avg_mean'
                            ]
    ## calculate the 10th and 90th percentile of claims-based rate within each quintile
    rating_10_90 = rating.groupby('quintile')['pu_rate_medicare'].quantile([0.1, 0.9]).reset_index()
    rating_10_90 = rating_10_90.pivot(index='quintile', columns='level_1').reset_index()
    rating_10_90.columns = rating_10_90.columns.to_flat_index()
    col = rating_10_90.columns
    col = pd.Index([str(e[0]) + str(e[1]) for e in col.tolist()])
    rating_10_90.columns = col

    ## calculate the percentage of nursing homes with 4- or 5-star overall rating/quality rating within each quintile
    rating_percent_highstar = \
        rating.groupby('quintile') \
            [['overall_rating', 'quality_rating', 'overall_rating_avg', 'quality_rating_avg']].apply(lambda x: (x>=4).sum()/x.count()).reset_index()
    rating_percent_highstar.columns = \
        ['quintile', 'overall_rating_percent_4_or_5_star', 'quality_rating_percent_4_or_5_star', 'overall_rating_avg_percent_4_or_5_star', 'quality_rating_avg_percent_4_or_5_star']
    ## merge all results data
    rating_table_final = pd.merge(pd.merge(rating_mean, rating_10_90, on='quintile'), rating_percent_highstar, on='quintile')
    rating_table_final.to_csv(path['exhibits']['input'] + 'analysis/correlation/sensitivity analysis 2/main_pu_rate_medicare{}_quintile_QM.csv'.format(years[n]))
# </editor-fold>

## select nursing homes with no pressure ulcer claims and calculate their mean NHC measures

## read in nursing home population data
nh_population = pd.read_csv(path['medicare_count']['output'] + 'nh_variables_nhc_measures.csv',
                            low_memory=False)

## drop nurisng homes with missing MCARE_ID
nh_population = nh_population.dropna(subset=['MCARE_ID'])


## drop nursing homes with missing NHC measures
nh_population = nh_population.dropna(subset=['long_stay_pu_score', 'short_stay_pu_score', 'overall_rating', 'quality_rating'])
# print(nh_population.shape[0])#77737
conditions_size = [(nh_population['CNSUS_RSDNT_CNT'] < 92),
                   ((nh_population['CNSUS_RSDNT_CNT'] >= 92) & (nh_population['CNSUS_RSDNT_CNT'] < 132)),
                   ((nh_population['CNSUS_RSDNT_CNT'] >= 132))]
values_size = ['s', 'm', 'l']
nh_population['size'] = np.select(conditions_size, values_size)


## merge with primary pressure ulcer hospital claims analytical sample data
nh_population_merge = \
    nh_population[['MCARE_ID', 'MEDPAR_YR_NUM', 'overall_rating_avg', 'quality_rating_avg',
                   'long_stay_pu_score_avg', 'short_stay_pu_score_avg', 'size']]. \
    merge(df[['MCARE_ID', 'MEDPAR_YR_NUM', 'MEDPAR_ID','pu_rate_medicare']], on=['MCARE_ID', 'MEDPAR_YR_NUM'],
          how='left')
## fill missing pu_rate_medicare with 0
nh_population_merge['pu_rate_medicare'] = nh_population_merge['pu_rate_medicare'].fillna(value=0)
## calculate the correlation between pu_rate_medicare and NHC measures for all nursing homes
nh_population_merge.\
    groupby('MEDPAR_YR_NUM')\
    [['pu_rate_medicare', 'overall_rating_avg', 'quality_rating_avg',
      'long_stay_pu_score_avg', 'short_stay_pu_score_avg']].\
    corr(method='pearson').to_csv(
path['exhibits']['input'] + 'analysis/correlation/sensitivity analysis 2/main_corr_pu_rate_medicare_and_qm_score_byyear_all_nh.csv'
)
## select nursing homes with no primary pressure ulcer claims
nh_population_nopu = nh_population_merge[nh_population_merge['MEDPAR_ID'].isna()]
nh
## calculcate the number of nursing homes with no pu claims
print(nh_population_nopu.groupby('MEDPAR_YR_NUM')['MCARE_ID'].count())

## calculate mean NHC measures
print(nh_population_nopu.groupby('MEDPAR_YR_NUM')\
          [['overall_rating_avg', 'quality_rating_avg',
            'long_stay_pu_score_avg', 'short_stay_pu_score_avg']].\
      mean())
## calculate percentage 4- or 5-star
nh_population_nopu['high_star_overall'] = nh_population_nopu['overall_rating_avg']>=4
nh_population_nopu['high_star_quality'] = nh_population_nopu['quality_rating_avg']>=4
print(nh_population_nopu.groupby('MEDPAR_YR_NUM')[['high_star_overall', 'high_star_quality']].mean())

