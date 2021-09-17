# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will preprocess MEDPAR SNF claims to merge with MDS.
# ----------------------------------------------------------------------------------------------------------------------#

import pandas as pd
import dask.dataframe as dd
import os
import numpy as np
import sys

def identify_pu(row):
    ## this function map claims to main, secondary and not_pu categories based on diagnosis codes
    ## NOTE: only main pressure ulcer claims is used in this study
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]

    pu1 = row[dcode[1:]].str.startswith('7070', na=False)
    pu2 = row[dcode[1:]].str.startswith('7072', na=False)
    pu3 = row[dcode[1:]].str.startswith('L89', na=False)

    ## identify main pu claims: if the admitting, the primary or the second diagnosis code is pu
    if ((row.DGNS_1_CD.startswith('7070') |  ## determine if the primary diagnosis code is pu
         row.DGNS_1_CD.startswith('7072') |
         row.DGNS_1_CD.startswith('L89')) |
            (row.DGNS_2_CD.startswith('7070') |
             row.DGNS_2_CD.startswith('7072') |  ## determine if the second diagnosis code is pu
             row.DGNS_2_CD.startswith('L89')) |
            (row.ADMTG_DGNS_CD.startswith('7070') |
             row.ADMTG_DGNS_CD.startswith('7072') |  ## determine if the admission diagnosis code is pu
             row.ADMTG_DGNS_CD.startswith('L89'))):
        row['pu_ind'] = 'main'
    elif (any(pu1) | any(pu2) | any(pu3)):
        ## if any of the secondary diagnosis codes is PU related (except those categorized as "main"),
        ## the claim is a 'spu' claims - secondary only pressure ulcer claims
        row['pu_ind'] = 'spu'
    else:
        row['pu_ind'] = 'not_pu'
    return row


def processsnf(medparpath, year, outpath):
    ## this function select PU related SNF claims and write to parquet
    ## define diagnosis columns
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))]
    ## define diagnosis e code columns
    decode = ['DGNS_E_{}_CD'.format(i) for i in list(range(1, 13))]
    ## define poa indicator columns (not used)
    poacode = ['POA_DGNS_{}_IND_CD'.format(i) for i in list(range(1, 26))]
    ## define diagnosis version columns
    dvcode = ['DGNS_VRSN_CD_{}'.format(i) for i in list(range(1, 26))]
    dvecode = ['DGNS_E_VRSN_CD_{}'.format(i) for i in list(range(1, 13))]
    ## define columns selected for the analysis
    col_use = ['BENE_ID', 'MEDPAR_ID', 'MEDPAR_YR_NUM', 'PRVDR_NUM', 'ADMSN_DT', 'DSCHRG_DT',
               'DSCHRG_DSTNTN_CD', 'SS_LS_SNF_IND_CD', 'BENE_DSCHRG_STUS_CD', 'DRG_CD',
               'ADMTG_DGNS_CD', 'CVRD_LVL_CARE_THRU_DT'] + dcode + decode + poacode + dvcode + dvecode

    ## read in raw MedPAR data
    medpar = dd.read_parquet(medparpath, engine='fastparquet')

    medpar = medpar.reset_index()

    ## select columns and rows
    ## 1) select inpatient data
    ## 2) select columns
    ## 3) select pu related claims

    ## 1) select SNF claims
    snf = medpar[medpar.SS_LS_SNF_IND_CD == 'N']
    ## 2) select columns
    snf = snf[col_use]

    ## create an indicator denotes the claim is a medpar claim
    snf['r'] = 'h'
    ## identify main and secondary pressure ulcer claims
    snf = snf.apply(identify_pu, axis=1)
    snf_main = snf[snf['pu_ind'] == 'main']
    snf_spu = snf[snf['pu_ind'] == 'spu']

    snf_main = snf_main.drop(columns='pu_ind')
    snf_spu = snf_spu.drop(columns='pu_ind')
    ## write pu SNF claims to parquet
    snf_main.to_parquet(outpath + 'main_pu_claims_snf/{}/'.format(year))
    snf_spu.to_parquet(outpath + 'secondary_only_pu_claims_snf/{}/'.format(year))

    return snf_main

def merge_mbsf(input_snf, input_mbsf, output_path):
    ## read in SNF pu claims
    snf = dd.read_parquet(input_snf)
    ## read in MBSF base data
    mbsf = dd.read_csv(input_mbsf,
                       dtype={'ESRD_IND': 'object'},
                       low_memory=False,
                       assume_missing=True
                       )
    ## define dual and hmo columns
    dual = ['DUAL_STUS_CD_{:02d}'.format(i) for i in range(1, 13)]
    hmo  = ['HMO_IND_{:02d}'.format(i) for i in range(1, 13)]
    col_use = ['BENE_ID', 'BENE_DEATH_DT', 'ESRD_IND', 'AGE_AT_END_REF_YR',
               'RTI_RACE_CD', 'BENE_ENROLLMT_REF_YR', 'SEX_IDENT_CD',
               'ENTLMT_RSN_ORIG', 'ENTLMT_RSN_CURR'] + dual + hmo
    ## select columns
    mbsf = mbsf[col_use]
    mbsf = mbsf.rename(columns={'BENE_DEATH_DT': 'death_dt',
                              'AGE_AT_END_REF_YR': 'age',
                              'RTI_RACE_CD': 'race_RTI',
                              'SEX_IDENT_CD': 'sex'})

    ## include only FFS for the full year
    mbsf = mbsf[mbsf[hmo].isin(['0', '4']).all(axis=1)]
    ## merge mbsf with snf pu claims
    merge = snf.merge(mbsf, on='BENE_ID')
    merge.to_parquet(output_path)
    return merge



if __name__ == '__main__':
    import yaml
    from dask.distributed import Client
    client = Client("10.50.86.250:55317")

    # input and output paths
    years = list(range(2011, 2018))
    yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

    ## RUN THE FUNCTIONs TO PROCESS MEDPAR SNF Claims
    for y in range(len(years)):
        snf_main = processsnf(path['processSNF']['input'][y], years[y], outpath=path['processSNF']['output'])
    ## RUN THE FUNCTION TO MERGE MBSF WITH SNF PU CLAIMS
    for y in range(len(years)):
        snf_main_merge = merge_mbsf(input_snf=path['processSNF']['output'] + 'main_pu_claims_snf/{}/'.format(years[y]),
                                    input_mbsf=path['processSNF']['input_mbsf'][y],
                                    output_path=path['processSNF']['output'] + 'main_pu_claims_snf_ffs/{}/'.format(years[y]))


    # ## flowchart calculation
    # ## total SNF claims
    # n_snf = []
    # for p in path['processSNF']['input']:
    #     df = dd.read_parquet(p)
    #     df = df[df.SS_LS_SNF_IND_CD=="N"]
    #     n_snf.append(df.shape[0].compute())
    # print(sum(n_snf))  # 125,019,398
    #
    ## total SNF FFS pressure ulcer claims
    ## main: 138780
    n_pu_snf = []
    for y in years:
        df = dd.read_parquet(path['processSNF']['output'] + 'main_pu_claims_snf_ffs/' + str(y))
        n_pu_snf.append(df.shape[0].compute())
    print(sum(n_pu_snf))





