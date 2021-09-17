# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will preprocess MEDPAR hospital raw data to merge with MDS.
# ----------------------------------------------------------------------------------------------------------------------#

####################################################################
# Pre-process medpar to merge with MDS
# 1) select columns
# 2) create main hospital claims: select pressure ulcer related claims by the
#    admission, primary or the second diagnosis codes and poa indicators
# 3) create secondary hospital claims: select pressure ulcer claims only by
#    secondary diagnosis codes (except the second diagnosis) and
#    poa indicators, excluding any overlap with claims in main hospital claims
# 4) merge hospital claims with MBSF FFS data for beneficiary characteristics
####################################################################

import dask.dataframe as dd
import pandas as pd
import numpy as np
import yaml

def identify_pu_stage(code):
    ## this code use ICD codes to determine pressure ulcer stages
    ## 1 - 4 corresponds to stage 1-4
    ## 0 is unspecified stage
    ## 5 is unstageable
    ## 6 is deep dissue damage which only included in ICD-10 after 2017
    ## there are a few cases with eroneous pressure ulcer stage coding, which is captured
    ## by the initial assignment of stage to missing value
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

def assign_highest_stage(row):
    ## this function assigns the highest pressure ulcer stage in diagnosis codes to each claim

    ## define diagnosis code columns
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))] + ['ADMTG_DGNS_CD']
    ## use diagnosis code to determine pu stage
    stage_list = [identify_pu_stage(i) for i in list(row[dcode])]
    ## assign the highest pu stage to each claim
    if np.all(np.isnan(stage_list)): ## assign missing values to "highest stage" if no diagnosis code has pressure ulcer stage
        row['highest stage'] = np.NaN
    else:
        row['highest stage'] = np.nanmax(stage_list)## assign the "highest stage"
    return row

def identify_pu_poa(row):
    ## map claims to main, spu and not_pu categories based on diagnosis codes and poa indicators
    ## main - primary hospital claims: if admitting, primary or the second diagnosis code is related to pressure ulcer and "present on admission"
    ## spu - secondary hospital claims: if any other diagnosis code is related to pressure ulcer and "present on admission"

    ## define diagnosis code columns
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]
    ## define poa indicator columns
    poacode = ['POA_DGNS_{}_IND_CD'.format(i) for i in list(range(1, 26))]

    ## returns a list of True and False to see if each diagnosis code is related to PU
    ## or is POA
    pu1 = row[dcode[1:]].str.startswith('7070', na=False) ##determines if diagnosis code is related to pu place code in ICD9
    pu2 = row[dcode[1:]].str.startswith('7072', na=False) ##determines if diagnosis code is related to pu place stage in ICD9
    pu3 = row[dcode[1:]].str.startswith('L89', na=False)  ##determines if diagnosis code is related to pu code in ICD10
    poa = row[poacode[1:]].str.startswith('Y', na=False)  ##determines if the diagnosis is present-on-admission to hospital

    ## determine the main pressure ulcer claims
    if (((row.DGNS_1_CD.startswith('7070') |  ## determine if the primary diagnosis code is pu and poa
           row.DGNS_1_CD.startswith('7072') |
           row.DGNS_1_CD.startswith('L89')) &
          row.POA_DGNS_1_IND_CD.startswith('Y')) |
         ((row.DGNS_2_CD.startswith('7070') |
           row.DGNS_2_CD.startswith('7072') |  ## determine if the second diagnosis code is pu and poa
           row.DGNS_2_CD.startswith('L89')) &
          row.POA_DGNS_2_IND_CD.startswith('Y')) |
         (row.ADMTG_DGNS_CD.startswith('7070') |
          row.ADMTG_DGNS_CD.startswith('7072') |  ## determine if the admission diagnosis code is pu
          row.ADMTG_DGNS_CD.startswith('L89'))):
        row['pu_poa_ind'] = 'main'
    ## determine the secondary pressure ulcer claims
    elif (any([True for i,j in zip(pu1, poa) if i&j]) | \
        ## True if any of the secondary diagnosis code is related to pressure ulcer and its corresponding poa indicator is Y
          any([True for i,j in zip(pu2, poa) if i&j]) | \
          any([True for i,j in zip(pu3, poa) if i&j])):
        row['pu_poa_ind'] = 'spu'
    else:
        row['pu_poa_ind'] = 'not_pu'
    return row


def processmedpar(medparpath, year, outpath):
    ## this function select PU related claims and write to parquet
    ## define columns
    dcode   = ['DGNS_{}_CD'.format(i) for i in list(range(1,26))]
    decode  = ['DGNS_E_{}_CD'.format(i) for i in list(range(1,13))]
    poacode = ['POA_DGNS_{}_IND_CD'.format(i) for i in list(range(1,26))]
    dvcode  = ['DGNS_VRSN_CD_{}'.format(i) for i in list(range(1,26))]
    dvecode  = ['DGNS_E_VRSN_CD_{}'.format(i) for i in list(range(1,13))]

    ## define columns used for analysis
    col_use = ['BENE_ID', 'MEDPAR_ID', 'MEDPAR_YR_NUM', 'PRVDR_NUM', 'ADMSN_DT', 'DSCHRG_DT',
               'DSCHRG_DSTNTN_CD', 'SS_LS_SNF_IND_CD', 'BENE_DSCHRG_STUS_CD', 'DRG_CD',
               'ADMTG_DGNS_CD'] + dcode + decode + poacode + dvcode + dvecode

    ## read in MedPAR raw data
    medpar = dd.read_parquet(medparpath, engine='fastparquet')

    medpar = medpar.reset_index()

    ## select columns and rows
    ## 1) select inpatient claims
    ## 2) select columns
    ## 3) select pu related claims

    ## 1) select hospital claims
    dfip = medpar[medpar.SS_LS_SNF_IND_CD.isin(['S', "L"])]
    ## 2) select columns
    dfip = dfip[col_use]

    ## create an indicator denotes the claim is a medpar claim
    dfip['r'] = 'h'

    ## 3) select pu related claims
    ### a) main analysis: if admission, primary or the secondary diagnosis codes
    ###    are related to pu and is poa
    ### b) secondary only: if the admission, primary and the second diagnoses is unrelated to pu and
    ###    any other secondary diagnoses code is related to pu and is poa
    dfip_pu_poa = dfip.apply(identify_pu_poa, axis=1)
    dfip_pu_poa_main = dfip_pu_poa[dfip_pu_poa.pu_poa_ind == 'main']
    dfip_pu_poa_spu = dfip_pu_poa[dfip_pu_poa.pu_poa_ind == 'spu']

    dfip_pu_poa_main = dfip_pu_poa_main.drop(columns='pu_poa_ind')
    dfip_pu_poa_spu = dfip_pu_poa_spu.drop(columns='pu_poa_ind')

    ## write the data to parquet
    dfip_pu_poa_main.to_parquet(outpath + 'main_pu_claims_medpar/{}/'.format(year))
    dfip_pu_poa_spu.to_parquet(outpath + 'secondary_only_pu_claims_medpar/{}/'.format(year))

def merge_mbsf(medpar_path, mbsf_path, output_path):
    ## read in pressure ulcer claims
    pu_claims = dd.read_parquet(medpar_path)
    ## read in MBSF base data
    mbsf = dd.read_csv(mbsf_path,
                       dtype={'ESRD_IND': 'object'},
                       low_memory=False,
                       assume_missing=True
                       )
    ## define MBSF columns to use
    dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]
    hmo  = ['HMO_IND_{:02d}'.format(i) for i in range(1, 13)]
    col_use = ['BENE_ID', 'BENE_DEATH_DT', 'ESRD_IND', 'AGE_AT_END_REF_YR',
               'RTI_RACE_CD', 'BENE_ENROLLMT_REF_YR', 'SEX_IDENT_CD',
               'ENTLMT_RSN_ORIG', 'ENTLMT_RSN_CURR'] + dual + hmo
    mbsf = mbsf[col_use]
    ## rename columns
    mbsf = mbsf.rename(columns={'BENE_DEATH_DT': 'death_dt',
                              'AGE_AT_END_REF_YR': 'age',
                              'RTI_RACE_CD': 'race_RTI',
                              'SEX_IDENT_CD': 'sex'})

    ## include only FFS beneficiaries for the full year
    mbsf = mbsf[mbsf[hmo].isin(['0', '4']).all(axis=1)]

    ## merge pu claims and mbsf
    pu_claims = pu_claims.merge(mbsf, on='BENE_ID')
    pu_claims.to_parquet(output_path)


if __name__ == '__main__':
    from dask.distributed import Client
    client = Client("10.50.86.250:59024")

    # input and output paths
    years = list(range(2011, 2018))
    yaml_path ='/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

    ## RUN THE FUNCTIONs TO PROCESS MEDPAR hospital claims DATA
    for y in range(len(years)):
        processmedpar(path['processMEDPAR']['input'][y], years[y], outpath=path['processMEDPAR']['output'])

    ## RUN THE FUNCTION TO MERGE MEDPAR PRESSURE ULCER CLAIMS WITH MBSF FFS beneficiaries data
    for y in range(len(years)):
        merge_mbsf(medpar_path=path['processMEDPAR']['output'] + 'main_pu_claims_medpar/{}/'.format(years[y]),
                   mbsf_path=path['processMEDPAR']['input_mbsf_path'][y],
                   output_path=path['processMEDPAR']['output'] + 'main_pu_claims_medpar_ffs/{}/'.format(years[y]))
    # ## flowchart calculation
    # main = [dd.read_parquet(path['processMEDPAR']['output'] + 'main_pu_claims_medpar_ffs/{}/'.format(y)) for y in years]
    # n_puclaims = []
    # for y in range(len(main)):
    #     df = main[y]
    #     df['ADMSN_DT'] = df['ADMSN_DT'].astype('datetime64[ns]')
    #     df = df[~((df.ADMSN_DT < '2011-04-11') |
    #               ((df.ADMSN_DT >= '2015-10-01') &
    #                (df.ADMSN_DT < '2016-01-01')))]
    #     n_puclaims.append(df.shape[0].compute())
    # print(sum(n_puclaims))#596,891

    ## merge MBSF with secondary pressure ulcer hospital claims
    for y in range(len(years)):
        merge_mbsf(medpar_path=path['processMEDPAR']['output'] + 'secondary_only_pu_claims_medpar/{}/'.format(years[y]),
                   mbsf_path=path['processMEDPAR']['input_mbsf_path'][y],
                   output_path=path['processMEDPAR']['output'] + 'secondary_only_pu_claims_medpar_ffs/{}/'.format(years[y]))
    # ## flowchart calculation
    # spu = [dd.read_parquet(path['processMEDPAR']['output'] + 'secondary_only_pu_claims_medpar_ffs/{}/'.format(y)) for y in years]
    # n_puclaims = []
    # for y in range(len(spu)):
    #     df = spu[y]
    #     df['ADMSN_DT'] = df['ADMSN_DT'].astype('datetime64[ns]')
    #     df = df[~((df.ADMSN_DT < '2011-04-11') |
    #               ((df.ADMSN_DT >= '2015-10-01') &
    #                (df.ADMSN_DT < '2016-01-01')))]
    #     n_puclaims.append(df.shape[0].compute())
    # print(sum(n_puclaims))#1,744,803

