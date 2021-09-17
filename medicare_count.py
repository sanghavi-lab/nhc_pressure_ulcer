# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# ----------------------------------------------------------------------------------------------------------------------#

####################################################################
## THIS CODE IS TO COUNT THE NUMBER OF MEDICARE FEE-FOR-SERVICE RESIDENTS,
## RACE MIX, AND THE PERCENTAGE OF DUALLY ELLIGIBLE RESIDENTS IN EACH
## NURSING HOME-YEAR FROM 2011 TO 2017
## USING MDS ASSESSMENTS AND MBSF BENEFICIARIES DATA
####################################################################

import pandas as pd
import dask.dataframe as dd
import yaml
import numpy as np
from dask.distributed import Client
# client = Client("10.50.86.250:48730")
pd.set_option('display.max_columns', 100)

years = list(range(2011, 2018))
## define input and output paths
yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

## set BENE_ID as index and write MBSF to parquet to facilitate merging with MDS
for i in range(len(years)):
    print(years[i])
    mbsf = dd.read_csv(path['6_merge_mbsf_and_fac']['input_mbsf_path'][i],
                       low_memory=False,
                       assume_missing=True)
    print(mbsf.shape[0].compute())
    print(len(mbsf.columns))
    print(list(mbsf.columns))
    mbsf = mbsf.set_index('BENE_ID')
    col = mbsf.columns
    mbsf = mbsf.astype(dict(zip(col, ['str']*len(col))))
    mbsf.to_parquet(path['medicare_count']['input_mbsf'][i],
                    engine='fastparquet', compression='gzip')
    mbsf_parquet = dd.read_parquet(path['medicare_count']['input_mbsf'][i])
    print(mbsf_parquet.shape[0].compute())
    print(len(mbsf_parquet.columns))
## calculate nursing home level variables
for i in range(len(years)):

    # read in cross-walked mds with BENE_ID as index
    mds = dd.read_parquet(path['medicare_count']['input_mds'][i])
    if i >= 5:
        cols = mds.columns
        mds.columns = [col.upper() for col in cols]
    mds = mds.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
    mds = mds[['FAC_PRVDR_INTRNL_ID', 'STATE_CD']]
    ## create a proxy column for BENE_ID
    mds['BENE_ID_col'] = mds.index
    ## keep unique beneficiaries in each nursing home
    mds = mds.drop_duplicates(subset=['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'BENE_ID_col'])

    ## read in MBSF
    mbsf = dd.read_parquet(path['medicare_count']['input_mbsf'][i])

    mbsf = mbsf.astype({'RTI_RACE_CD': 'float64'})
    # pd.DataFrame(list(mbsf.divisions)).to_csv('/gpfs/data/sanghavi-lab/Zoey/gardner/data/merge_output/pu/medpar_mds_final/test/mbsf_division2011_test.csv')
    hmo = ['HMO_IND_{:02d}'.format(i) for i in range(1, 13)]
    ## include only FFS beneficiaries for the full year
    mbsf = mbsf[mbsf[hmo].isin(['0', '4']).all(axis=1)]
    ## select columns
    dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]
    mbsf = mbsf[['RTI_RACE_CD'] + dual]
    mbsf = mbsf.astype(dict(zip(dual, ['float']*12)))

    ## merge mbsf FFS beneficiaries with mds on BENE_ID (index)
    merge = mds.merge(mbsf, left_index=True, right_index=True)

    ## within each nursing home, count the number of unique MEDICARE FFS residents for each year
    medicare_count = \
        merge.groupby(['FAC_PRVDR_INTRNL_ID', 'STATE_CD'])['BENE_ID_col'].\
        count().rename('medicare_count').reset_index()
    medicare_count = medicare_count.compute()
    medicare_count.to_csv(path['medicare_count']['output'] + 'medicare_res_count{}.csv'.format(years[i]),
                          index=False)

    ## calculate nursing home race mix
    race_mix = \
        merge.groupby(['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'RTI_RACE_CD'])['BENE_ID_col']. \
            count().rename('nbene').reset_index().compute()

    race_mix.columns = ['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'race_rti', 'nbene']

    race_mix.to_csv(path['medicare_count']['output'] + 'nh_racemix_{}.csv'.format(years[i]),
                    index=False)

    ## count the percentage of dual eligibles in each nh
    merge['dual'] = merge[dual].isin([2, 4, 8]).any(axis=1)
    dual_percent = \
        merge.groupby(['FAC_PRVDR_INTRNL_ID', 'STATE_CD'])['dual'].\
            mean().rename('dual_percent').reset_index().compute()
    dual_percent.columns = ['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'dual_percent']

    dual_percent.to_csv(path['medicare_count']['output'] + 'nh_dualpercent_{}.csv'.format(years[i]),
                        index=False)

## concat nh medicare count, dual percentage, and race mix from 2011 - 2017
## read in and concat percent of duals for each nursing home
dual_percent = [pd.read_csv(path['medicare_count']['output'] + 'nh_dualpercent_{}.csv'.format(y),
                              low_memory=False)
                  for y in years]
for i in range(len(dual_percent)):
    dual_percent[i]['MEDPAR_YR_NUM'] = years[i]
dual_percent = pd.concat(dual_percent)
dual_percent = dual_percent.astype({'FAC_PRVDR_INTRNL_ID': 'str'})

## concat nh medicare count
medicare_count = [pd.read_csv(path['medicare_count']['output'] + 'medicare_res_count{}.csv'.format(y),
                              low_memory=False)
                  for y in years]
for i in range(len(medicare_count)):
    medicare_count[i]['MEDPAR_YR_NUM'] = years[i]
medicare_count = pd.concat(medicare_count)
medicare_count = medicare_count.astype({'FAC_PRVDR_INTRNL_ID': 'str'})

## concat nh race mix
race_mix = [pd.read_csv(path['medicare_count']['output'] + 'nh_racemix_{}.csv'.format(y),
                        low_memory=False)
            for y in years]
for i in range(len(race_mix)):
    race_mix[i]['MEDPAR_YR_NUM'] = years[i]
race_mix = pd.concat(race_mix)
race_mix = race_mix.astype({'FAC_PRVDR_INTRNL_ID': 'str',
                            'race_rti': 'float'})
## remove race with missing values
race_mix = race_mix[race_mix['race_rti']!=0]

## replace values for race_rti
vals_to_replace = {1: 'white', 2: 'black', 3:'other', 4: 'asian', 5:'hispanic', 6:'american_indian'}
race_mix['race_rti'] = race_mix['race_rti'].map(vals_to_replace)

## reshape race mix from long to wide
race_mix_wide = race_mix.pivot(index=['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'MEDPAR_YR_NUM'],
                               columns='race_rti',
                               values='nbene').reset_index()
## calculate the percentage of each race within each nh
race = list(vals_to_replace.values())

race_mix_wide['all_race'] = race_mix_wide[race].sum(axis=1)
for r in race:
    race_mix_wide[r + '_percent'] = race_mix_wide[r] / race_mix_wide['all_race']
race_mix_wide = race_mix_wide.drop(columns=race)

## merge race mix with medicare count by nh-year
nh_population = medicare_count.merge(race_mix_wide,
                                     on=['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'MEDPAR_YR_NUM'])
nh_population = nh_population.merge(dual_percent,
                                    on=['FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'MEDPAR_YR_NUM'])

nh_population.to_csv(path['medicare_count']['output'] + 'nh_variables.csv', index=False)
# print(nh_population.head())

nh_population = pd.read_csv(path['medicare_count']['output'] + 'nh_variables.csv')
print(nh_population.shape[0])
# print(nh_population.groupby('MEDPAR_YR_NUM')['FAC_PRVDR_INTRNL_ID'].count())


## merge MCARE_ID from LTCFocus facility data with nurisng home medicare population data
facility = pd.read_csv(path['6_merge_mbsf_and_fac']['input_facility_path'])  ## read in facility data

## create new column -fac_state_id- that concatenates FACILITY_INTERNAL_ID and STATE_ID
facility = facility.astype({'FACILITY_INTERNAL_ID': 'str',
                            'STATE_ID': 'str'})
## fill FACILITY_INTERNAL_ID with leading zeros
facility['FACILITY_INTERNAL_ID'] = facility['FACILITY_INTERNAL_ID'].str.zfill(10)
## combine FACILITY_INTERNAL_ID and STATE_ID to create fac_state_id
facility['fac_state_id'] = facility['FACILITY_INTERNAL_ID'] + facility['STATE_ID']

nh_population['FAC_PRVDR_INTRNL_ID'] = nh_population['FAC_PRVDR_INTRNL_ID'].astype('Int64')

nh_population = nh_population.astype({'FAC_PRVDR_INTRNL_ID': 'str'})
nh_population['FAC_PRVDR_INTRNL_ID'] = nh_population['FAC_PRVDR_INTRNL_ID'].str.zfill(10)
nh_population['fac_state_id'] = nh_population['FAC_PRVDR_INTRNL_ID'] + nh_population['STATE_CD']
## merge nurisng home variables with facility data to get MCARE_ID
nh_population = nh_population.merge(facility[['fac_state_id', 'MCARE_ID', 'NAME', 'ADDRESS', 'FAC_CITY', 'STATE_ID', 'FAC_ZIP', 'CATEGORY',
                                    'CLOSEDDATE']],
                              on='fac_state_id',
                              how='left')
nh_population.to_csv(path['medicare_count']['output'] + 'nh_variables.csv', index=False)
print(nh_population['MCARE_ID'].isna().mean()) #0.00016352784061486467

nh_population = pd.read_csv(path['medicare_count']['output'] + 'nh_variables.csv')
nh_population = nh_population.drop(columns='Unnamed: 0')


## merge with CASPER
casper = pd.read_csv(path['6_merge_mbsf_and_fac']['input_casper_path'], low_memory=False)
## clean CASPER data
casper = casper.rename(columns={'PRVDR_NUM': 'MCARE_ID'})
casper['CRTFCTN_DT'] = pd.to_datetime(casper['CRTFCTN_DT'].astype(str), infer_datetime_format=True)
## create casper_year to indicate the year of the survey results
casper['casper_year'] = casper['CRTFCTN_DT'].dt.year

## keep the latest snap shot of NH characteristics for each year
casper_latest = casper.groupby(['MCARE_ID', 'casper_year'])['CRTFCTN_DT'].max().reset_index()
casper = casper.merge(casper_latest, on=['MCARE_ID', 'casper_year', 'CRTFCTN_DT'], how='inner')

years = range(2011, 2018)
nh_population_casper_final = []
for n in range(len(years)):
    print(years[n])
    nh_population_year = nh_population[nh_population['MEDPAR_YR_NUM']==years[n]]
    ## 2) merge the latest NH characteristics within a year from CASPER with other nursing home-level variables
    nh_population_casper = nh_population_year.merge(casper[['STATE_CD', 'MCARE_ID', 'CRTFCTN_DT', 'casper_year',
                                                       'GNRL_CNTL_TYPE_CD', 'CNSUS_RSDNT_CNT', 'CNSUS_MDCD_CNT',
                                                       'CNSUS_MDCR_CNT', 'CNSUS_OTHR_MDCD_MDCR_CNT']],
                                                    on='MCARE_ID',
                                                    how='left',
                                                     suffixes=['', '_casper'])
    ## select data matched with same year casper
    nh_population_casper_matched = nh_population_casper[(nh_population_casper.MEDPAR_YR_NUM == nh_population_casper.casper_year)]
    # print(nh_population_casper_matched.shape[0])
    ## for nursing homes not matched with same year casper, match them with the closest casper within 2 years
    nh_population_casper_notmacthed = \
        nh_population_casper[~nh_population_casper['MCARE_ID'].isin(nh_population_casper_matched['MCARE_ID'])]

    nh_population_casper_notmacthed['years_diff'] = nh_population_casper_notmacthed['MEDPAR_YR_NUM'] - nh_population_casper_notmacthed['casper_year']

    nh_population_casper_notmacthed_within2 = nh_population_casper_notmacthed[(nh_population_casper_notmacthed['years_diff']<=0) |
                                                                              (nh_population_casper_notmacthed['years_diff']>=2)]

    nh_population_casper_notmacthed_within2_closest = \
        nh_population_casper_notmacthed_within2.groupby(['MCARE_ID', 'MEDPAR_YR_NUM'])['CRTFCTN_DT'].max().reset_index()
    nh_population_casper_notmacthed_within2 = \
        nh_population_casper_notmacthed_within2.merge(nh_population_casper_notmacthed_within2_closest,
                                                      on=['MCARE_ID', 'MEDPAR_YR_NUM', 'CRTFCTN_DT'])
    # print(nh_population_casper_notmacthed_within2.shape[0])

    nh_population_casper_notmacthed_atall = \
        nh_population_casper_notmacthed[~nh_population_casper_notmacthed['MCARE_ID'].isin(nh_population_casper_notmacthed_within2['MCARE_ID'])]

    nh_population_casper_notmacthed_atall = nh_population_casper_notmacthed_atall.drop_duplicates(subset=['MCARE_ID', 'MEDPAR_YR_NUM'])
    nh_population_casper_notmacthed_atall = \
        nh_population_casper_notmacthed_atall.drop(columns=[
            'GNRL_CNTL_TYPE_CD', 'CNSUS_RSDNT_CNT', 'CNSUS_MDCD_CNT',
            'CNSUS_MDCR_CNT', 'CNSUS_OTHR_MDCD_MDCR_CNT'])
    # print(nh_population_casper_notmacthed_atall.shape[0])
    nh_population_casper_year = pd.concat([nh_population_casper_matched,
                                            nh_population_casper_notmacthed_within2,
                                            nh_population_casper_notmacthed_atall])

    nh_population_casper_final.append(nh_population_casper_year)
nh_population_casper_final = pd.concat(nh_population_casper_final)
nh_population_casper_final.to_csv(path['medicare_count']['output'] + 'nh_variables_capser.csv',
                                  index=False)





