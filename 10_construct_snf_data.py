# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will create final mds-snf_claims denominator data for statistical analysis
# ----------------------------------------------------------------------------------------------------------------------#


import pandas as pd
import numpy as np
import yaml

from dask.distributed import Client
import yaml

def identify_pu_stage(code):
    ## this code map ICD codes to pressure ulcer stages
    ## 1 - 4 corresponds to stage 1-4
    ## 0 is unspecified stae
    ## 5 is unstageable
    ## 6 is deep dissue damage which only included in ICD-10
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
    # iter each medpar claim to count the number of diagnosis associated with each stage
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
    dcode = ['DGNS_{}_CD'.format(i) for i in list(range(1, 26))] + ['ADMTG_DGNS_CD']
    stage_list = [identify_pu_stage(i) for i in list(row[dcode])]
    if len([i for i in stage_list if i==None])!=0: ## it means some pressure ulcer stage code is wrong, and captured by the try/except statement in identify_pu_stage function; since it prints out the eroneous code, the returned value is None
        row['highest stage'] = row['MEDPAR_ID']
    elif np.all(np.isnan([i for i in stage_list if i!=None])):
        row['highest stage'] = np.NaN
    else:
        row['highest stage'] = np.nanmax(stage_list)
    return row

## define file paths
yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))
## read in snf
snf = [pd.read_csv(p, low_memory=False) for p in path['10_construct_snf_data']['input']]
snf = pd.concat(snf)

## all snf claims only have short-stay residents because Medicare only covers post-acute care within 100 days
snf['short_stay'] = True

## assign the highest pressure ulcer stage in claims
snf = snf.apply(assign_highest_stage, axis=1)
snf = snf[~((snf['highest stage']==0) | (snf['highest stage'].isna()))]
print(snf.shape[0]) #63076
snf.to_csv(path['10_construct_snf_data']['output'])
