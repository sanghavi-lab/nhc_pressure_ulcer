# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will Pre-process mds assessments to merge with MedPAR claims.
# ----------------------------------------------------------------------------------------------------------------------#

####################################################################
# Pre-process mds assessments to merge with MEDPAR
# 1) select columns
# 2) clean special characters
# 3) create m_pu indicator for whether the assessment recorded a pressure ulcer
####################################################################

import pandas as pd
import dask.dataframe as dd
from dask.delayed import delayed
import dask
from sas7bdat import SAS7BDAT
import numpy as np
import os
import time
pd.set_option('display.max_columns', 500)


def processmds_parquet(rpath, write_path):
    ## MDS_ASMT_ID is the unique id for mds
    ## define pu-related MDS items (these columns are not used in analysis)
    pucode = ['M0100A_RISK_VSBL_CD', 'M0100B_RISK_FRML_ASMT_CD', 'M0100C_RISK_CLNCL_JDGMNT_CD',
              'M0100Z_NO_RISK_DTMNTN_CD', 'M0150_PRSR_ULCR_RISK_CD', 'M0300B3_STG_2_ULCR_OLD_DT']
    ## define MDS items for present-on-admission pu
    pupoacode = ['M0300B2_STG_2_ULCR_ADMSN_NUM', 'M0300C2_STG_3_ULCR_ADMSN_NUM', 'M0300D2_STG_4_ULCR_ADMSN_NUM',
                 'M0300E2_U_ULCR_DRSNG_ADMSN_NUM', 'M0300F2_U_ULCR_ESC_ADMSN_NUM', 'M0300G2_U_ULCR_DEEP_ADMSN_NUM']
    ## define MDS items for "Current Number of Unhealed Pressure Ulcers at Each Stage"
    ## except for the first itme, pucode_num lists all items in Table 1 in the paper
    ## and will determine our primary/secondary outcome
    pucode_num = ['M0210_STG_1_HGHR_ULCR_CD', 'M0300A_STG_1_ULCR_NUM', 'M0300B1_STG_2_ULCR_NUM',
                  'M0300C1_STG_3_ULCR_NUM', 'M0300D1_STG_4_ULCR_NUM', 'M0300E1_UNSTGBL_ULCR_DRSNG_NUM',
                  'M0300F1_UNSTGBL_ULCR_ESC_NUM', 'M0300G1_UNSTGBL_ULCR_DEEP_NUM']
    ## define other MDS items
    other_cols = ['MDS_ASMT_ID', 'BENE_ID', 'TRGT_DT', 'STATE_CD', 'FAC_PRVDR_INTRNL_ID', 'A0310A_FED_OBRA_CD',
	              'A0310B_PPS_CD', 'A0310C_PPS_OMRA_CD', 'A0310D_SB_CLNCL_CHG_CD', 'A0310E_FIRST_SINCE_ADMSN_CD',
	              'A0310F_ENTRY_DSCHRG_CD', 'A1600_ENTRY_DT', 'A1700_ENTRY_TYPE_CD', 'A1800_ENTRD_FROM_TXT',
                  'A1900_ADMSN_DT', 'A2000_DSCHRG_DT', 'A2100_DSCHRG_STUS_CD', 'A2300_ASMT_RFRNC_DT']

    ## read in raw mds
    mds = dd.read_parquet(rpath)
    ## exclude mds with missing BENE_ID
    mds = mds[~mds.BENE_ID.isna()]

    ## turn all columns to upper case
    cols = [col.upper() for col in mds.columns]
    mds.columns = cols

    ## select columns defined above
    cols_to_use = other_cols + pucode + pupoacode + pucode_num
    mds = mds[cols_to_use]
    ## replace special characters
    mds = mds.replace({'^': np.NaN, '-': np.NaN, '': np.NaN})

    ## change data types for certain columns
    dtype = {col: 'float64' for col in pucode_num + pupoacode}
    mds = mds.astype(dtype)
    mds = mds.astype({'A2000_DSCHRG_DT': 'datetime64[ns]',
                      'A1600_ENTRY_DT': 'string',
                      'TRGT_DT': 'string'})
    ## change date columns to datetime format
    mds['A1600_ENTRY_DT'] = dd.to_datetime(mds['A1600_ENTRY_DT'], infer_datetime_format=True)
    mds['TRGT_DT'] = dd.to_datetime(mds['TRGT_DT'], infer_datetime_format=True)

    ## create new columns which indicate
    ## 1) whether all pressure ulcer fields are missing; True if all fields are missing (not used)
    ## 2) whether the resident has recorded pressure ulcer; True if any of the fields is larger than 0
    ## 3) whether the resident has recorded pressure ulcer at stage 2 and above; True if any of the fields for pressure ulcer at stage 2 and above is larger than 0 (not used)
    mds['m_pu_missing'] = (mds[pucode_num].isna()).all(axis=1)
    mds['m_pu'] = (mds[pucode_num[1:]] > 0).any(axis=1)
    mds['m_pu_stage2_and_above'] = (mds[pucode_num[2:]] > 0).any(axis=1)

    ## create an indicator column to show the record is from mds
    mds['r'] = 'm'
    ## write data to parquet
    mds.to_parquet(write_path)
    return mds

if __name__ == '__main__':
    import yaml
    from dask.distributed import Client
    client = Client("10.50.86.250:57089")

    years = list(range(2011, 2018))
    ## import input and output path
    yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))
    ## run the function to preprocess MDS and write to parquet
    for i in range(len(years)):
        mds = processmds_parquet(path['processMDS']['input'][i], path['processMDS']['output'][i])
        print(mds.shape[0].compute())


