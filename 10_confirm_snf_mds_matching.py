## THIS CODE IS TO MAKE SURE MDS ASSESSMENTS AND LINKED SNF CLAIMS COME FROM THE SAME FACILITY

# ORG_NPI_NUM
# On an institutional claim, the National Provider Identifier (NPI) number
# assigned to uniquely identify the institutional provider certified by Medicare to provide services to the beneficiary.

#A0100A_NPI_NUM
# This column contains the facility or provider's National Provider Identifier number.

## the original processed claims don't have the above providers' id, so have to re-merge raw data back in


import pandas as pd
import numpy as np
import yaml
import dask.dataframe as dd
from dask.distributed import Client
client = Client("10.50.86.250:37700")
pd.set_option('display.max_columns', 200)

## define input and output path
years = list(range(2011, 2018))
yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

## read in merged mds-snf_claims
snf = pd.read_csv(path['10_construct_snf_data']['output'], low_memory=False)
print(snf.shape[0])
snf = snf.astype({'MDS_ASMT_ID': 'int'})
snf = snf.astype({'MDS_ASMT_ID': 'str'})
snf = snf.astype({'MDS_ASMT_ID': 'str',
                  'TRGT_DT': 'datetime64[ns]'})
## create the column year using TRGT_DT
snf['year'] = snf['TRGT_DT'].dt.year
## select columns
df = snf[['MEDPAR_ID', 'MDS_ASMT_ID', 'MEDPAR_YR_NUM', 'year']]

claims_all = []
mds_all = []

for i in range(len(years)):
    ## read in raw medpar for each year
    claims = dd.read_parquet(path['processSNF']['input'][i])
    ## select raw snf claims with ORG_NPI_NUM for snf claims that are
    ## in the final mds_snf analytical sample data for each medpar year
    claims = claims[['MEDPAR_ID', 'ORG_NPI_NUM']][
    claims.MEDPAR_ID.isin(df[df['MEDPAR_YR_NUM'] == years[i]]['MEDPAR_ID'])].compute()
    ## append selected medpar from 2011 - 2017
    claims_all.append(claims)

    ## read in raw mds
    mds = dd.read_parquet(path['processMDS']['input'][i])
    if i >= 5:
        cols = mds.columns
        mds.columns = [col.upper() for col in cols]
        mds = mds.astype({'MDS_ASMT_ID': 'float'})
        mds = mds.astype({'MDS_ASMT_ID': 'int'})
    mds = mds.astype({'MDS_ASMT_ID': 'str'})
    ## select raw mds with column A0100A_NPI_NUM for the MDS linked to SNF claims
    mds = \
        mds[['MDS_ASMT_ID', 'A0100A_NPI_NUM']][
            mds['MDS_ASMT_ID'].
                isin(df[df['year']==years[i]]['MDS_ASMT_ID'])
        ].compute()

    mds_all.append(mds)

## merge ORG_NPI_NUM from claims and A0100A_NPI_NUM from mds to denominator-file
df = pd.concat(claims_all).merge(df, on='MEDPAR_ID')
df = df.merge(pd.concat(mds_all), on='MDS_ASMT_ID')
df = df.astype({'ORG_NPI_NUM': 'str',
                'A0100A_NPI_NUM': 'str'})

## keep only snf claims matched with the mds from the same facility
df_new = df[df['ORG_NPI_NUM']==df['A0100A_NPI_NUM']]

snf_new = snf[snf['MEDPAR_ID'].isin(df_new['MEDPAR_ID'])]
## write to csv
snf_new.to_csv(path['10_construct_snf_data']['output'], index=False)
print(snf_new.shape[0]) #60203



