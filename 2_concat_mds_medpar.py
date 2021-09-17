# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# Description: This script will concat and rank MedPAR hospital PU claims and MDS Assessments for each beneficiary.
# ----------------------------------------------------------------------------------------------------------------------#

####################################################################
# Concat and rank MedPAR PU claims and MDS Assessments
# 1) concat medpar hospital claims and mds for each beneficiary
# 2) for each beneficiary, rank mds and medpar claims according to the time of claims or assessments (hospital admission date or MDS target date)
####################################################################

import dask
import os
from datetime import datetime
import dask.dataframe as dd
import pandas as pd
from fastparquet import ParquetFile
import numpy as np
from processMDS import processmds_parquet as procmds

def assign_ranking_date(row):
    ## create columns to sort MDS and hospital claims for each beneficiary
    ## the goal is to re-create (for each beneficiary) a flow of nursing home admission, discharge and readmission,
    ## and hospital admission and discharge to identify the population for our analytical sample

    ## 1) create a column 'rd' to rank claims and mds for each beneficiary
    ## "rd" equals target date (TRGT_DT) if the record is a MDS assessment
    if pd.isnull(row['PRVDR_NUM']): ## if hospital PRVDR_NUM is missing, it is a MDS
        row['rd'] = row['TRGT_DT']
    # "rd" equals hospital admission date ("ADMSN_DT") if it is a hospital claim
    elif pd.isnull(row['FAC_PRVDR_INTRNL_ID']): ## if nursing home FAC_PRVDR_INTRNL_ID is missing, it is a hospital claim
        row['rd'] = row['ADMSN_DT']
    else:
        row['rd'] = np.NaN

    ## 2) create sort_m_a0310e and other variables to sort MDS and hospital claims if rd is a tie

    ## scenario: some assessments has the same target date
    ## if it is the first assessment since the nursing home stay, it ranks earlier than any other assessment for the resident
    ## we use sort_m_a0310e to sort other assessments with the same target date
    if row['A0310E_FIRST_SINCE_ADMSN_CD']=='1':
        row['sort_m_a0310e']=-1
    else:
        row['sort_m_a0310e']=0

    ## scenario: many discharge mds has a mds with A0310F_ENTRY_DSCHRG_CD=99 at the same date
    ## we use A0310F_ENTRY_DSCHRG_CD to rank these assessments with A0310F_ENTRY_DSCHRG_CD coded as 99 earlier than discharge assessment
    if row['A0310F_ENTRY_DSCHRG_CD']=='99':
        row['sort_m_a0310f'] = -1
    else:
        row['sort_m_a0310f'] = 0

    ### scenario: a patient is discharged from nursing home and admitted to the hospital on the same day
    ### in this case, discharge assessment should rank earlier than hospital claims;
    ### we would use sort_m_dschrg to rank discharge assessment and hospital claim with the same "rd"
    if row['A0310F_ENTRY_DSCHRG_CD']=='10': ## if this is a discharge assessment return anticipated
        row['sort_m_dschrg'] = -1 ## -1 would rank earlier than 0
    elif row['A0310F_ENTRY_DSCHRG_CD']=='11': ## if this is a discharge assessment return not anticipated
        row['sort_m_dschrg'] = -1
    else: ## if this is not a discharge assessment
        row['sort_m_dschrg'] = 0

    ## scenario: some patients may readdmit to NH at the same date of hospital admission
    ## in this case, hospital claim should rank earlier than assessment (any assessment other than the discharge assessment)
    ## we would use sort_h_hospital to rank MDS and hospital claim with the same "rd"
    if pd.isnull(row['FAC_PRVDR_INTRNL_ID']):
        row['sort_h_hospital'] = -1
    else:
        row['sort_h_hospital'] = 0

    return row

# <editor-fold desc="PART 1: SELECT COMMON BENE_ID BETWEEN MEDPAR AND MDS; WRITE COMMON BENE_ID TO CSV">

def select_common_id(medpar_path, mds_path, writepath):
    ## This function selects common beneficiary id between pressre ulcer hospital claims and MDS assessments

    ## read in pressure ulcer hospital claims
    medpar = [dd.read_parquet(path, engine='fastparquet') for path in medpar_path]
    medpar = dd.concat(medpar)

    ## read in mds cleaned data
    mds = [dd.read_parquet(path, engine='fastparquet') for path in mds_path]
    mds = dd.concat(mds)

    ## select common BENE_ID
    id_medpar = set(medpar.BENE_ID) ## unique BENE_ID for medpar claims
    id_mds = set(mds.BENE_ID) ## unique BENE_ID for MDS
    common_id = id_medpar & id_mds ## select the intersection

    ## write unique common BENE_ID to csv
    # pd.DataFrame(list(common_id)).to_csv(writepath, index=False)

    print('common bene id is written to ', writepath)

##########################################################################################################
# </editor-fold>



# <editor-fold desc="PART 2: CONCATENATE MEDPAR AND MDS FOR EACH BENE_ID; RANK RECORDS BY DATE; WRITE TO CSV">

def concat_rank(years, common_id_path, medpar_path, mds_path, concat_path, concat_rank_path):
    ## this function concat pressure ulcer claims and mds assessments, and
    ## rank them by date for each beneficiary

    # ## 1) read in common id csv
    # ## 2) select claims and assessments with common BENE_id
    # ## 3) concat and rank medpar and mds

    # read in common bene_id
    common_id = dd.read_csv(common_id_path,
                             header=None, names=['BENE_ID'])


    ## separate bene_id to 200 partitions to reduce data size
    common_id = common_id.repartition(npartitions=200)

    ## for each beneficiary,
    ## concat and rank his/her mds and medpar claims
    for i in range(common_id.npartitions): ## loop through each partition
        ## select common bene_id within the partition
        cid = common_id.partitions[i]

        mpn = pd.DataFrame()
        for path in medpar_path:
            ## read in each year's hospital pressure ulcer claims
            medpar = dd.read_parquet(path, engine='fastparquet')
            ## select hospital claims with these bene_ids
            medpar = medpar.merge(cid, on='BENE_ID', how='inner')
            ## concat all years of selected claims
            mpn = dd.concat([mpn, medpar], axis=0)

        mdn = pd.DataFrame()
        for path in mds_path:
            ## read in mds for each year
            mds = dd.read_parquet(path, engine='fastparquet')
            ## select mds with these bene_ids
            mds = mds.merge(cid, on='BENE_ID', how='inner')
            ## concat all years of selected mds
            mdn = dd.concat([mdn, mds], axis=0)

        ## concat selected medpar and mds records
        mmdf = dd.concat([mpn, mdn], axis=0)
        mmdf = mmdf.apply(assign_ranking_date, axis=1)

        ## write concat medpar and mds to csv
        # mmdf.compute().to_csv(concat_path + 'concat_{}.csv'.format(i),
        #                       index=False)
        del mmdf
        del mpn
        del mdn

        ## read the concated data back in
        mmdf = pd.read_csv(concat_path + 'concat_{}.csv'.format(i))

        # rank the records by
        # rd (ranking date), sort_m_a0310e, sort_m_a0310f, sort_m_dschrg and sort_h_hospital
        # in ascending order for each BENE_ID
        mmdf = mmdf.astype({'rd': 'datetime64[ns]'})
        mmdf = \
            mmdf.\
                groupby('BENE_ID').\
                apply(lambda g: g.sort_values(['rd', 'sort_m_a0310e', 'sort_m_a0310f', 'sort_m_dschrg', 'sort_h_hospital'])).\
                reset_index(drop=True)
        ## create a rank column to record the order of each mds or claim
        mmdf['rank'] = mmdf.groupby('BENE_ID').cumcount().reset_index(drop=True)
        ## for some variables, recording their values on the prior record to help determine patients coming from nursing home.
        shifted = \
            mmdf[['BENE_ID', 'rd',
                  'FAC_PRVDR_INTRNL_ID', 'STATE_CD', 'A0310F_ENTRY_DSCHRG_CD', 'A2000_DSCHRG_DT',
                  'A2100_DSCHRG_STUS_CD', 'TRGT_DT']].\
                groupby('BENE_ID').\
                shift(1)
        ## merge values from the previous record with current records and rename columns of previous records as "_lag"
        mmdf = mmdf.join(shifted.rename(columns=lambda x: x+"_lag"))

        ## write data to csv
        # mmdf.to_csv(concat_rank_path + 'concat_rank_{}.csv'.format(i),
        #             index=False)
    print('data is written to \n', concat_path, '\n and \n', concat_rank_path)
# </editor-fold>

if __name__ == '__main__':
    import yaml
    from dask.distributed import Client
    client = Client("10.50.86.250:50521")

    years = list(range(2011, 2018))
    ## define file paths
    yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

    # ## run the function to select common bene_id for main (primary) and secondary hospital claims
    # select_common_id(path['2_concat_mds_medpar']['input_main'], path['2_concat_mds_medpar']['input_mds'], writepath=path['2_concat_mds_medpar']['output_common_id_main'])
    # select_common_id(path['2_concat_mds_medpar']['input_spu'], path['2_concat_mds_medpar']['input_mds'], writepath=path['2_concat_mds_medpar']['output_common_id_spu'])

    ## concat and rank MDS and hospital claims
    concat_rank(years,
                path['2_concat_mds_medpar']['output_common_id_main'],
                path['2_concat_mds_medpar']['input_main'],
                path['2_concat_mds_medpar']['input_mds'],
                path['2_concat_mds_medpar']['output_main'][0],
                path['2_concat_mds_medpar']['output_main'][1])
    concat_rank(years,
                path['2_concat_mds_medpar']['output_common_id_spu'],
                path['2_concat_mds_medpar']['input_spu'],
                path['2_concat_mds_medpar']['input_mds'],
                path['2_concat_mds_medpar']['output_spu'][0],
                path['2_concat_mds_medpar']['output_spu'][1])

    # ## flow chart calculation
    # main = dd.read_csv(path['2_concat_mds_medpar']['output_main'][1] + '*.csv',
    #                    dtype={'PRVDR_NUM': 'object'})
    # main['rd'] = main['rd'].astype('datetime64[ns]')
    # spu  = dd.read_csv(path['2_concat_mds_medpar']['output_spu'][1]+ '*.csv',
    #                    dtype={'PRVDR_NUM': 'object'})
    # spu['rd'] = spu['rd'].astype('datetime64[ns]')
    #
    # print(main[~((main.rd < '2011-04-11') |
    #              ((main.rd >= '2015-10-01') &
    #               (main.rd < '2016-01-01')))].MEDPAR_ID.unique().size.compute()) #483261 + 1(na)
    # print(spu[~((spu.rd < '2011-04-11') |
    #             ((spu.rd >= '2015-10-01') &
    #              (spu.rd < '2016-01-01')))].MEDPAR_ID.unique().size.compute()) #1374636 + 1(na)

