# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will
#               1) merge NHC MDS-based quality measure data and five-star rating from 2011 - 2017 with mds-hospital_claims, and
#               2) construct nursing-home claims-based pressure ulcer rate
# ----------------------------------------------------------------------------------------------------------------------#

## NOTE IN YEAR 2011 AND 2012
# 199.0: small denominator suppression:
#   <30 for the long stay measures (401-419) and
#   <20 for the short stay measures (424-434)
# 201.0: score is missing
# (either no score generated for a particular measure or
#  facility is not matched to QM data, in which case all measures for a facility would be set to 201.0)

import pandas as pd
import numpy as np
import csv
import os
import re
from medpar_functions import assign_highest_stage
import yaml
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 500)

years = range(2011, 2018)

## define paths
yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

## read in self-constructed data for the number of Medicare residents in each nursing home-year
nh_variables = pd.read_csv(path['9_merge_star_ratings']['input_medicare_count'],
                             low_memory=False)
# nh_variables = nh_variables.rename(columns={'year': 'MEDPAR_YR_NUM'})
nh_variables = nh_variables.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
nh_variables = nh_variables.replace({np.nan: 0})

## read in five-star ratings file
star_09_15 = pd.read_sas(path['9_merge_star_ratings']['input_star'][0], format='sas7bdat', encoding='latin-1')
star16_17 = [pd.read_csv(path) for path in path['9_merge_star_ratings']['input_star'][1:]]

## read in Quality Measures (QMs) data
qm_11_17 = pd.read_csv(path['9_merge_star_ratings']['input_qm'])
qm_11_17 = qm_11_17.drop(columns=['Unnamed: 0'])

## pre-process QMs data
## reshape quality measure from long to wide format
qm_11_17_wide = qm_11_17.pivot(index=['provnum', 'year', 'quarter'], columns='msr_cd', values='MEASURE_SCORE')
qm_11_17_wide = qm_11_17_wide.reset_index()
qm_11_17_wide = qm_11_17_wide.rename(columns={403: 'long_stay_pu_score',
                                              425: 'short_stay_pu_score'})

## Since these are quarterly QMs, we keep the
## last QM each year and the average QM each year for each NH
## (the average ratings and QMs are used in the end)

## calculate average QMs each year for each nh
qm_11_17_wide_avg = \
    qm_11_17_wide.groupby(['provnum', 'year']) \
        [['long_stay_pu_score', 'short_stay_pu_score']].mean().reset_index()
## merge average QMs
qm_11_17_wide = qm_11_17_wide.merge(qm_11_17_wide_avg, on=['provnum', 'year'], how='inner', suffixes=['', '_avg'])
## select the latest QMs each year
qm_11_17_wide_latest_quarter = qm_11_17_wide.groupby(['provnum', 'year'])['quarter'].max().reset_index()
qm_11_17_wide = qm_11_17_wide.merge(qm_11_17_wide_latest_quarter,
                                    on=['provnum', 'year', 'quarter'],
                                    how='inner')
## keep unique nursing home-year QMs
qm_11_17_wide = qm_11_17_wide.drop_duplicates(subset=['provnum', 'year'])

# ## pre-process Five-star ratings data

## define columns for analysis
star_cols = ['provnum', 'year', 'overall_rating', 'quality_rating', 'survey_rating', 'staffing_rating', 'rn_staffing_rating',
             'overall_rating_avg', 'quality_rating_avg', 'survey_rating_avg', 'staffing_rating_avg', 'rn_staffing_rating_avg']
## separate time column into year and quarter columns
star_09_15[['year', 'quarter']] = star_09_15.time.str.split('_', expand=True)
## remove the character "Q" from quarter column
star_09_15['quarter'] = star_09_15['quarter'].str.replace('Q', '')

star_09_15['year'] = pd.to_numeric(star_09_15['year'], errors='coerce')
star_09_15['quarter'] = pd.to_numeric(star_09_15['quarter'], errors='coerce')
star_09_15 = star_09_15.astype({'provnum': 'str'})

## select ratings form 2011 - 2015
star_11_15 = star_09_15[star_09_15['year']>=2011]

## calculate the average rating within each year for providers
## also record the latest rating within each year (normally the 4th quarter) for providers

## calculate average ratings each year
star_11_15_avg = \
    star_11_15.groupby(['provnum', 'year']) \
        [['overall_rating', 'quality_rating', 'survey_rating', 'staffing_rating', 'rn_staffing_rating']].\
        mean().reset_index()
## merge average ratings with original ratings
star_11_15 = star_11_15.merge(star_11_15_avg, on=['provnum', 'year'], how='inner', suffixes=['', '_avg'])
## select the latest ratings each year
star_11_15_latest_quarter = star_11_15.groupby(['provnum', 'year'])['quarter'].max().reset_index()
star_11_15 = star_11_15.merge(star_11_15_latest_quarter,
                              on=['provnum', 'year', 'quarter'],
                              how='inner')
## keep unique nursing home-year ratings
star_11_15 = star_11_15.drop_duplicates(subset=['provnum', 'year'])

## pre-process ratings in 2016-2017
star16_17 = pd.concat(star16_17)
star16_17.columns = [i.lower() for i in star16_17.columns]
## separate quarter column into year and quarter columns
star16_17[['year', 'quarter']] = star16_17.quarter.str.split('Q', expand=True)

star16_17['year'] = pd.to_numeric(star16_17['year'], errors='coerce')
star16_17['quarter'] = pd.to_numeric(star16_17['quarter'], errors='coerce')
star16_17 = star16_17.astype({'provnum': 'str'})

## calcualte average rating each year
star16_17_avg = \
    star16_17.groupby(['provnum', 'year'])\
        [['overall_rating', 'quality_rating', 'survey_rating', 'staffing_rating', 'rn_staffing_rating']].\
        mean().reset_index()
star16_17 = star16_17.merge(star16_17_avg, on=['provnum', 'year'], how='inner', suffixes=['', '_avg'])
## keep the latest rating within each year for providers
star16_17_latest_quarter = star16_17.groupby(['provnum', 'year'])['quarter'].max().reset_index()
star16_17 = star16_17.merge(star16_17_latest_quarter,
                              on=['provnum', 'year', 'quarter'],
                              how='inner')
## keep unique nursing home-year ratings
star16_17 = star16_17.drop_duplicates(subset=['provnum', 'year'])

## combine ratings from 2011 - 2017
star = pd.concat([star_11_15[star_cols], star16_17[star_cols]])

## merge all nursing homes NHC measures and medicare population count
nh_variables_new = nh_variables.merge(qm_11_17_wide,
                                      left_on=['MCARE_ID', 'MEDPAR_YR_NUM'],
                                      right_on=['provnum', 'year'],
                                      how='left')
nh_variables_new = nh_variables_new.drop(columns=['provnum', 'year'])
nh_variables_new = nh_variables_new.merge(star,
                                          left_on=['MCARE_ID', 'MEDPAR_YR_NUM'],
                                          right_on=['provnum', 'year'],
                                          how='left')
nh_variables_new = nh_variables_new.drop(columns=['provnum', 'year'])

nh_variables_new.to_csv(path['medicare_count']['output'] + 'nh_variables_nhc_measures.csv', index=False)
print(nh_variables_new.head())
## read in denominator files and merge with star-ratings and QM
for n in range(len(path['9_merge_star_ratings']['input_denominator'])):
    df = pd.read_csv(path['9_merge_star_ratings']['input_denominator'][n],
                     low_memory=False,
                     dtype={'A2000_DSCHRG_DT_lag': 'object',
                            'PRVDR_NUM': 'object',
                            'DGNS_2_CD': 'object',
                            'DGNS_3_CD': 'object',
                            'ADMTG_DGNS_CD': 'object',
                            'CANCER_ENDOMETRIAL_EVER': 'object',
                            'DGNS_1_CD': 'object',
                            'MCARE_ID': 'str',
                            'FAC_PRVDR_INTRNL_ID': 'str'
                            })
    df = df.astype({'ADMSN_DT': 'datetime64[ns]'})
    df['quarter'] = df['ADMSN_DT'].dt.quarter

    ## merge ratings and denominator files
    df = df.merge(star,
                  left_on=['MCARE_ID', 'MEDPAR_YR_NUM'], right_on=['provnum', 'year'],
                  how='left')

    ## merge PU quality measure with denominator files
    df = df.merge(qm_11_17_wide, left_on=['MCARE_ID', 'MEDPAR_YR_NUM'], right_on=['provnum', 'year'], how='left')

    ## construct claims-based pressure ulcer rate for each year
    ## pu_rate_medicare is used in statistical analysis and it is calculated
    ## as the number of claims for a nursing home within a year divided by the number of Medicare residents in the nursing home

    ##1) merge denominator files with self-constructed Medicare FFS resident count data
    df = df.merge(nh_variables, on=['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'MEDPAR_YR_NUM'])

    nh_pu = df.groupby(['MCARE_ID', 'MEDPAR_YR_NUM'])['MEDPAR_ID'].count().rename('nclaims').reset_index()
    df = df.merge(nh_pu, on=['MCARE_ID', 'MEDPAR_YR_NUM'], how='left')
    df['pu_rate_medicare'] = df['nclaims']/df['medicare_count']

    ## exclude claims with a pu_rate larger than 1
    print(df[df['pu_rate_medicare']>1].shape[0]) #0 claims
    df = df[df['pu_rate_medicare'] <= 1]
    ## assign the highest pressure ulcer stage to each claim
    df = df.apply(assign_highest_stage, axis=1)

    df.to_csv(path['9_merge_star_ratings']['output_denominator'][n], index=False)
    print(df.shape[0])
