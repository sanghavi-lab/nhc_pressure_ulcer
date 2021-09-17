# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will merge MedPAR SNF PU claims and MDS Assessments on BENE_ID.
# ----------------------------------------------------------------------------------------------------------------------#


import dask
import os
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
import numpy as np


# <editor-fold desc="PART 1: SELECT COMMON BENE_ID BETWEEN MEDPAR AND MDS; WRITE COMMON BENE_ID TO CSV">

def select_common_id(medpar_path, mds_path, writepath):
    ## This function selects common beneficiary id between pressre ulcer hospital claims and MDS assessments

    ## read in pressure ulcer claims
    medpar = [dd.read_parquet(path, engine='fastparquet') for path in medpar_path]
    medpar = dd.concat(medpar)

    ## read in mds raw data and pre-process
    mds = [dd.read_parquet(path, engine='fastparquet') for path in mds_path]
    mds = dd.concat(mds)
    mds = mds[~mds.BENE_ID.isna()]

    ## select common BENE_ID (medpar pu claims and mds)
    id_medpar = medpar['BENE_ID'].compute()
    id_mds = mds['BENE_ID'].compute()

    common_id = pd.merge(id_medpar, id_mds, how='inner')

    ## write unique common BENE_ID to csv
    pd.DataFrame(common_id['BENE_ID'].unique()).to_csv(writepath, index=False)
    print(common_id['BENE_ID'].unique().size)#100532


##########################################################################################################
# </editor-fold>

def merge_snf_mds(common_id_path, medpar_path, mds_path, write_path):
    ## this function is to merge all mds with the snf claims on BENE_ID

    ## read in common_bene_id
    cid = pd.read_csv(common_id_path)
    cid.columns = ['BENE_ID']
    ## calculate the size of common bene_id
    len_cid = cid.shape[0]

    ## read in cleaned mds
    mds = [dd.read_parquet(path, engine='fastparquet') for path in mds_path]
    mds = dd.concat(mds)
    ## read in SNF PU claims
    medpar = [dd.read_parquet(path, engine='fastparquet') for path in medpar_path]
    medpar = dd.concat(medpar)

    ## chunck bene_id to smaller size, for each chunck, merge mds with snf
    for n in range(len_cid // 5000 + 1): ## divide bene_id into n_bene/5000 partitions
        if n != len_cid // 5000:
            id = cid.loc[n * 5000:((n + 1) * 5000 -1), 'BENE_ID'].tolist()
        else:
            id = cid.loc[n * 5000:len_cid, 'BENE_ID'].tolist()
        ## select MDS using bene_id
        mds_id = mds[mds.BENE_ID.isin(id)]
        ## select SNF claims using bene_id
        medpar_id = medpar[medpar.BENE_ID.isin(id)]
        ## merge mds and snf claims
        merge_id = dd.merge(medpar_id, mds_id, on='BENE_ID', how='inner')
        ## write to parquet
        merge_id.to_parquet(write_path + '{}/'.format(n))


if __name__ == '__main__':
    import yaml
    from dask.distributed import Client
    client = Client('10.50.86.250:55317')

    years = list(range(2011, 2018))
    ## define file paths
    yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))
    ## run the function to select common bene_id for main (primary) and secondary hospital claims
    select_common_id(path['2_merge_snf_mds']['input_main'],
                     path['2_merge_snf_mds']['input_mds'],
                     writepath=path['2_merge_snf_mds']['output_common_id_main'])

    ## run functions to merge mds with SNF claims
    merge_snf_mds(path['2_merge_snf_mds']['output_common_id_main'],
                  path['2_merge_snf_mds']['input_main'],
                  path['2_merge_snf_mds']['input_mds'],
                  path['2_merge_snf_mds']['output_main'])


    ## flowchart calculation
    files = os.listdir(path['2_merge_snf_mds']['output_main'])
    n = []
    for i in range(len(files)):
        main = dd.read_parquet(path['2_merge_snf_mds']['output_main'] + str(i))
        n.append(main.MEDPAR_ID.unique().size.compute())
    print(sum(n))#134793


