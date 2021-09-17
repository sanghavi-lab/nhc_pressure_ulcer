# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# ----------------------------------------------------------------------------------------------------------------------#

## THIS CODE IS USED TO CREATE THE THIRD EXHIBIT IN THE PAPER:
## THE MDS REPORTING RATE OF PRESSURE ULCER BY SHORT- VS LONG-STAY RESIDENTS, CLAIM TYPES, AND HIGHEST PRESSURE ULCER STAGE
## FINAL OUTPUT IS A TABLE FOR THE REPORTING RATE AND A TBALE FOR COUNTS OF PRESSURE ULCER CLAIMS

import pandas as pd
import numpy as np
import yaml

pd.set_option('display.max_columns', 500)
pd.set_option('display.max_rows', 100)

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

def check_report_rate3(row):
    ## create a column "m_pu3" to indicate
    ## if the nursing home report at least one pu stage correctly within one level of
    ## the stage of the highest-staged pressure ulcer diagnosis claims(higher or lower)
    ## stage 3 and stage 4 can also be mapped to unstageable and vice versa


    if (row['highest stage']==1) & \
            ((row[m_pu_col[1]] > 0) or
            (row[m_pu_col[2]] > 0)):
        row['m_pu3'] = 1
    elif (row['highest stage']==2) & \
            ((row[m_pu_col[2]] > 0) or
             (row[m_pu_col[1]] > 0) or
             (row[m_pu_col[3]] > 0)):
        row['m_pu3'] = 1
    elif (row['highest stage']==3) & \
            ((row[m_pu_col[3]] > 0) or
             (row[m_pu_col[2]] > 0) or
             (row[m_pu_col[4]] > 0) or
             (row[m_pu_col[6]] > 0)):
        row['m_pu3'] = 1
    ## there is no higher stage for stage 4 pu
    elif (row['highest stage']==4) & \
            ((row[m_pu_col[4]] > 0) or
             (row[m_pu_col[3]] > 0) or
             (row[m_pu_col[6]] > 0)):
        row['m_pu3'] = 1
    elif (row['highest stage']==5) & \
            ((row[m_pu_col[6]] > 0) or
             (row[m_pu_col[3]] > 0) or
             (row[m_pu_col[4]] > 0) or
             (row[m_pu_col[7]] > 0)):
        row['m_pu3'] = 1
    elif (row['highest stage']==6) & (row[m_pu_col[7]] > 0):
        row['m_pu3'] = 1
    ## if there is no specific pressure ulcer stage recorded in claims, m_pu3 is set to missing
    elif (row['highest stage'] == 0 | pd.isna(row['highest stage'])):
        row['m_pu3'] = np.nan
    else:
        row['m_pu3'] = 0
    return row

def calculate_reporting_rate(df, question,  bystay=False, bynh=False, byyear=False):
    ## this function calculate the aggregate reporting rate
    ## for different questions:
    ## question1 - if the nursing home report any pressure ulcer in MDS
    ## question3 - if the nursing home report any pressure ulcer at the correct stage or one stage higher or lower in MDS
    if question==1:
        df.loc[:, 'm_pu1'] = df.loc[:, 'm_pu']
        if bystay & bynh:
            rate = df.groupby(['MCARE_ID', 'highest stage', 'short_stay'])['m_pu1'].mean()
        elif byyear & bynh: ## this is used for appendix
            rate = df.groupby(['MEDPAR_YR_NUM', 'MCARE_ID'])['m_pu1'].mean()
        elif bystay:
            rate = df.groupby(['highest stage', 'short_stay'])['m_pu1'].mean()
        elif bynh:
            rate = df.groupby(['highest stage', 'MCARE_ID'])['m_pu1'].mean()
        else:
            rate = df.groupby('highest stage').m_pu1.mean()

    elif question==3:
        df = df.apply(check_report_rate3, axis=1)
        if bystay & bynh:
            rate = df.groupby(['MCARE_ID', 'highest stage', 'short_stay'])['m_pu3'].mean()
        elif byyear & bynh: ## this is used for appendix
            rate = df.groupby(['MEDPAR_YR_NUM', 'MCARE_ID'])['m_pu3'].mean()
        elif bystay:
            rate = df.groupby(['highest stage', 'short_stay'])['m_pu3'].mean()
        elif bynh_only:
            rate = df.groupby('MCARE_ID')['m_pu3'].mean()
        else:
            rate = df.groupby('highest stage').m_pu3.mean()

    return rate

if __name__=='__main__':
    ## define paths
    yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
    path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

    ## define paths
    years = range(2011, 2018)

    ## read in data
    main = pd.read_csv(path['exhibits']['input'] + 'main_data_final.csv', low_memory=False)
    main_report_at_correct_stage = main.apply(check_report_rate3, axis=1)
    main_report_at_correct_stage.to_csv(path['exhibits']['input'] + 'main_data_final_report_correctly.csv',
                                        index=False)
    secondary = pd.read_csv(path['exhibits']['input'] + 'secondary_only_data_final.csv', low_memory=False)
    snf = pd.read_csv(path['exhibits']['input_snf'] + 'main_snf_final_data.csv', low_memory=False)
    
    main['claims_type'] = 'MedPAR Hospital Claims with Primary Diagnosis as Pressure Ulcer'
    secondary['claims_type'] = 'MedPAR Hospital Claims with Secondary Diagnosis as Pressure Ulcer'
    snf['claims_type'] = 'MedPAR SNF Claims with Primary Diagnosis as Pressure Ulcer'
    
    all_data = pd.concat([main, secondary, snf])
    all_data['MCARE_ID'] = all_data['MCARE_ID'].astype('str')

    # <editor-fold desc="CALCULATE MEAN REPORTING RATE">
    # ## define colnames and row names of final results table
    col_names   = {0: 'report at all (all claims)',
                   1: 'report at correct stage (+/-1) (claims with a specified stage)'}
    row_names   = ['MedPAR Hospital Claims with Primary Diagnosis as Pressure Ulcer',
                   'MedPAR Hospital Claims with Secondary Diagnosis as Pressure Ulcer',
                   'MedPAR SNF Claims with Primary Diagnosis as Pressure Ulcer']
    
    ## secify arguments for functions
    args = [[(main, 1),
             (main, 3)],
            [(secondary, 1),
            (secondary, 3)],
            [(snf, 1),
             (snf, 3)]]
    
    ## calculate MDS report rate by claims type,
    ## short- vs. long-stay residents and by the highest pressure ulcer stage
    report_rate_all = pd.DataFrame()
    for i in range(len(args)): # i=0, 1, 2
        report_rate = pd.DataFrame.from_dict({'highest stage':[], 'short_stay':[]})
        for j in range(len(args[i])):  # j=0, 1, 2
            df = args[i][j][0]
            rate = calculate_reporting_rate(df, args[i][j][1], bystay=True). \
                                reset_index()
            report_rate = pd.merge(report_rate, rate, on=['highest stage', 'short_stay'], how='outer')
    
        report_rate.columns = ['highest stage', 'short_stay',
                                'report at all (all claims)',
                                'report at correct stage (+/-1) (claims with a specified stage)']
    
        report_rate['claims_type'] = row_names[i]
    
        report_rate_all = pd.concat([report_rate_all, report_rate], axis=0)
    report_rate_all.to_excel(path['exhibits']['input'] + 'analysis/report rate/report_rate_table.xlsx')
    # # # </editor-fold>
    
    # <editor-fold desc="CALCULATE NURSING HOME REPORTING RATE QUANTILE - USING ALL DATA OR ONLY MEDIUM AND LARGE NURSING HOMES">
    ## calculate reporting rate for each nursing homes;
    ## then calculate the 25 and 75 percentile reporting rate by nursing home, stay and highest stage
    # (this is unweighted percentile and is not used in the table)
    report_rate_nh_all = []
    report_rate_nh_quantile_main_and_sec = pd.DataFrame()
    
    for i in range(len(args)): # i=0, 1, 2
        report_rate_nh = pd.DataFrame.from_dict({'MCARE_ID':[], 'highest stage':[], 'short_stay':[]})
        report_rate_nh_ml = pd.DataFrame.from_dict({'MCARE_ID':[], 'highest stage':[], 'short_stay':[]})
        for j in range(len(args[i])):  # j=0, 1
    
            df = args[i][j][0]
    
            rate_nh = calculate_reporting_rate(df, args[i][j][1], bynh=True, bystay=True). \
                                rename(col_names[j]).reset_index()
            report_rate_nh = pd.merge(report_rate_nh, rate_nh, on=['MCARE_ID', 'highest stage', 'short_stay'], how='outer')
    
        report_rate_nh_quantile = report_rate_nh.groupby(['highest stage', 'short_stay']).quantile([0.25, 0.75])
        report_rate_nh_quantile = report_rate_nh_quantile.reset_index()
    
        ## write nursing home reporting rate to csv to use sas calculate weighted 25th and 75th percentile
        report_rate_nh['claims_type'] = row_names[i]
        report_rate_nh_all.append(report_rate_nh)
    
        report_rate_nh_quantile['claims_type'] = row_names[i]
        report_rate_nh_quantile_main_and_sec = pd.concat([report_rate_nh_quantile_main_and_sec, report_rate_nh_quantile], axis=0)
    
    report_rate_nh_all = pd.concat(report_rate_nh_all)
    report_rate_nh_all['MCARE_ID'] = report_rate_nh_all['MCARE_ID'].astype('str')
    
    report_rate_nh_all.to_csv(path['exhibits']['input']  + 'analysis/report rate/report_rate_nh_table.csv', index=False)
    report_rate_nh_quantile_main_and_sec.to_csv(path['exhibits']['input']  + 'analysis/report rate/report_rate_nh_quantile_table.csv')
    
    # calculate reporting rate weighted 25th and 75th percentiles
    # also see sas code exhibit3_weighted.sas
    # <editor-fold desc="CALCULATE WEIGHT USING TOTAL RESIDENT COUNTS OR USING CLAIMS COUNT">

    
    ## read in report rate table
    df = pd.read_csv(path['exhibits']['input'] + 'analysis/report rate/report_rate_nh_table.csv')
    if sum(['Unnamed: 0' == col for col in df.columns]) > 0:
        df = df.drop(columns=['Unnamed: 0'])
    
    ## weight = nursing home # of claims across years / total # of claims of all nursing homes
    all_data_claims = \
        all_data. \
            groupby(['claims_type', 'MCARE_ID', 'highest stage', 'short_stay'])['MEDPAR_ID'].\
            count().rename('nclaims').reset_index()
    
    all_data_claims_sum = \
        all_data. \
            groupby(['claims_type', 'highest stage', 'short_stay'])['MEDPAR_ID'].\
            count().rename('total_claims').reset_index()
    
    all_data_claims = \
        all_data_claims.merge(all_data_claims_sum,
                              on=['claims_type', 'highest stage', 'short_stay'])
    
    all_data_claims['weight_claims'] = all_data_claims['nclaims']/all_data_claims['total_claims']
    
    ## merge nursing home weight with denominator files;
    ## the weighted percentile is calculated in exhibit3_weighted.sas
    df_weight = df.merge(all_data_claims, on=['claims_type', 'highest stage', 'short_stay', 'MCARE_ID'])
    if sum(['Unnamed: 0' == col for col in df_weight.columns]) > 0:
        df_weight = df_weight.drop(columns=['Unnamed: 0'])
    
    df_weight.to_csv(path['exhibits']['input'] + 'analysis/report rate/report_rate_nh_table_weight2.csv')
    #</editor-fold>
    
    ## create the denominator count table
    denominator_count = \
        all_data. \
            groupby(['claims_type', 'highest stage', 'short_stay'])['MEDPAR_ID'].\
            count().rename('count').reset_index()
    denominator_count.to_csv(path['exhibits']['input'] + 'analysis/report rate/report_rate_denominator_count_table.csv',
                             index=False)

    ## <editor-fold desc="SENSITIVITY ANALYSIS">
    # REPORTING RATES FOR MDS WITHOUT THE RESTRICTION TO RETURN TO THE SAME NURSING HOME WITHIN 1 DAY
    # read in data
    main_sen = pd.read_csv(path['8_sl_stay_medpar_mds']['output_sensitivity'][0],
                            low_memory=False)
    main_sen = main_sen.apply(assign_highest_stage, axis=1)
    main_sen['claims_type'] = row_names[0]
    
    spu_sen  = pd.read_csv(path['8_sl_stay_medpar_mds']['output_sensitivity'][1],
                            low_memory=False)
    spu_sen = spu_sen.apply(assign_highest_stage, axis=1)
    spu_sen['claims_type'] = row_names[1]
    all_sample = pd.concat([main_sen, spu_sen])
    all_sample.groupby(['claims_type', 'highest stage', 'short_stay'])['MEDPAR_ID'].count().\
        to_csv(path['exhibits']['input'] + 'analysis/sensitivity/count_table_sensitivity_noreturn.csv')

    print(all_sample.groupby(['claims_type', 'short_stay'])['m_pu'].mean())


    args_sen = [[(main_sen, 1),
                 (main_sen, 3)],
                [(spu_sen, 1),
                (spu_sen, 3)]]
    report_rate_sen = pd.DataFrame()
    for i in range(len(args_sen)): # i=0, 1
        report_rate = pd.DataFrame.from_dict({'highest stage':[], 'short_stay':[]})
        for j in range(len(args_sen[i])):  # j=0, 1
            df = args_sen[i][j][0]
            rate = calculate_reporting_rate(df, args_sen[i][j][1], bystay=True). \
                                reset_index()
            report_rate = pd.merge(report_rate, rate, on=['highest stage', 'short_stay'], how='outer')
    
        report_rate.columns = ['highest stage', 'short_stay',
                                'report at all (all claims)',
                                'report at correct stage (+/-1) (claims with a specified stage)']
    
        report_rate['claims_type'] = row_names[i]
    
        report_rate_sen = pd.concat([report_rate_sen, report_rate], axis=0)
    report_rate_sen.to_excel(path['exhibits']['input'] + 'analysis/sensitivity/reoprt_rate_table_sensitivity_noreturn.xlsx')


    ## REPORTING RATES FOR SENSITIVITY ANALYSIS OF INCLUDING EXTRA MDS
    ## read in data
    main_sen = pd.read_csv(path['sensitivity']['output'][0], low_memory=False)
    spu_sen  = pd.read_csv(path['sensitivity']['output'][1], low_memory=False)
    
    args_sen = [[(main_sen, 1),
             (main_sen, 3)],
            [(spu_sen, 1),
            (spu_sen, 3)]]
    
    report_rate_sen = pd.DataFrame()
    for i in range(len(args_sen)): # i=0, 1
        report_rate = pd.DataFrame.from_dict({'highest stage':[], 'short_stay':[]})
        for j in range(len(args_sen[i])):  # j=0, 1
            df = args_sen[i][j][0]
            df = df.rename(columns={'m_pu7': 'm_pu'})
            rate = calculate_reporting_rate(df, args_sen[i][j][1], bystay=True). \
                                reset_index()
            report_rate = pd.merge(report_rate, rate, on=['highest stage', 'short_stay'], how='outer')
    
        report_rate.columns = ['highest stage', 'short_stay',
                                'report at all (all claims)',
                                'report at correct stage (+/-1) (claims with a specified stage)']
    
        report_rate['claims_type'] = row_names[i]
    
        report_rate_sen = pd.concat([report_rate_sen, report_rate], axis=0)
    report_rate_sen.to_excel(path['exhibits']['input'] + 'analysis/sensitivity/reoprt_rate_table_sensitivity.xlsx')
    # </editor-fold>

