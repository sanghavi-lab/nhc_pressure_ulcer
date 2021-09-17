# Code Description

These notes describe in sequence the code files used for the analysis.

## Software
We used Python 3.8, SAS 9.4 and Stata/MP 15.0.

## Steps

**1. Identify MedPAR pressure ulcer claims**

We used 100% sample of Medicare fee-for-service beneficiaries during 2011-2017. 
We used hospital admission data and SNF claims from the Medicare Provider Analysis and Review (MedPAR) file
and the 26 diagnosis codes and POA indicators to identify claims related to pressure ulcer 
and divide them into 1) primary pressure ulcer diagnosis hospital admission claims, 2) secondary pressure ulcer diagnosis hospital admission claims and 3) SNF claims.
Outputs from these scripts were linked with MDS assessments on the resident-level.

| Script Name | Script Description | Input Files | Output Files | New Variables Created |
| --------- | --------- | --------- | --------- | --------- | 
| processMEDPAR.py | This script selects two types of IP claims and merges claims with MBSF Base File: <br> (1) primary pressure ulcer diagnosis hospital admission claims where the admitting, the first or the second diagnosis code is related to pressure ulcer and the corresponding POA is "Y" <br> (2) secondary pressure ulcer diagnosis hospital admission claims where any other 23 diagnosis codes is related to pressure ulcer and the corresponding POA is "Y" | 2011 - 2017 MedPAR files | Parquet files in main_pu_claims_medpar and secondary_only_pu_claims_medpar | **r**: an indicator coded as "h" denotes that the record is a MedPAR claim |
| processSNF.py | This script selects primary SNF claims where the admitting, the first or the second diagnosis cdoe is related to pressure ulcer, and merges them with MBSF Base File. | 2011 - 2017 MedPAR files | Parquet files in main_pu_claims_snf | **r**: an indicator coded as "h" denotes that the record is a MedPAR claim |

**2. Clean MDS assessments**

We cleaned the MDS assessments, selecting useful variables such as assessment type, target date, discharge date and 
M0300 items. Outputs from these scripts were linked with MedPAR pressure ulcer claims.

| Script Name | Script Description | Input Files | Output Files | New Variables Created |
| --------- | --------- | --------- | --------- | --------- | 
| processMDS.py | This script cleans up MDS. | MDS from 2011 - 2017 | Parquet files in cleaned_mds_unique | **m_pu**: an indicator of whether any of the M0300A, M0300B1 - M0300G1 items is larger than 0<br> **r**: an indicator coded as "m" denotes that the record is an MDS assessment |

**3. Link MedPAR claims and MDS assessments**

We linked MedPAR claims and MDS assessments on the resident-level based on two criteria.
For hospital claims, we required residents to have a MDS discharge assessment within one day of hospital admission 
and required residents to be readmitted to the same nursing home within one day of hospital discharge. 
We linked the MDS discharge assessment to hospital claims.
For SNF claims, all MDS assessments within the patient’s stay at the facility were linked with the claim, 
except for Entry and Death Tracking Record because they don’t contain any pressure ulcer item.
The output from this step was the denominator files including information from claims and MDS assessments. 
The outputs were merged with CASPER for nursing home-level characteristics.

| Script Name | Script Description | Input Files | Output Files | New Variables Created |
| --------- | --------- | --------- | --------- | --------- | 
| 2_concat_mds_medpar.py | For each beneficiary, concatenate his/her pressure ulcer claims and MDS assessments and order them by hospital admission date or MDS target date. | Parquet output from processMEDPAR.py and processMDS.py | CSV files in concat_rank and concat_secondary_only | **rd**: a date column to sort claims and mds <br> **sort_***: several columns to sort claims and mds if ***rd*** is a tie <br> ***_lag**: several variables record values of some MDS items from the prior record |
| 3_select_pu_in.py | For each claim, look for its prior record to determine if the patient is admitted to hospital within one day of being discharged from the nursing home | CSV output from 2_concat_rank_mds_medpar.py | CSV files in main_pu_claims_from_nh and secondary_only_pu_claims_from_nh |  |
| 4_select_samenh_medpar.py | For each claim, determine if the patient returns to the same nursing home within one day after hospitalization | Output files from 2_concat_rank_mds_medpar.py and 3_select_mds_within_snf_stay.py | CSV files in main_pu_claims_from_samenh and secondary_only_pu_claims_from_samenh | ***_next**: several variables record values of MDS items and claim information from the next record |
| 5_merge_mds_medpar_samenh.py | For each claim, merge it with the closest prior MDS discharge assessment for patients who have pressure ulcer during nursing home residency and return to the same nursing home within one day of hospital discharge | Output files from 2_concat_rank_mds_medpar.py, 3_select_mds_within_snf_stay.py and 4_select_samenh_medpar.py | Parquet files in main_merge_samenh and secondary_only_merge_samenh |  |
| 2_merge_snf_mds.py | For each SNF pressure ulcer claim, merge it with the MDS assessments of the same beneficiary. | Output parquets from processSNF.py and processMDS.py | Parquet files in main_merge_snf_mds |  |
| 3_select_mds_within_snf_stay.py | For each SNF pressure ulcer claim, select linked MDS assessments within the resident's stay at the nursing home. | Output from 2_merge_snf_mds.py | CSV files in main_merge_snf_mds_within_stay | **end_date**: a date variable indicating the end of nursing home stay, which is either the "DSCHRG_DT" or the "CVRD_LVL_CARE_THRU_DT" |


**4. Construct patient- and facility-level variables**

We used claims data, MBSF Base file and MBSF Chronic Condition file to construct patient-level variables: age, sex, dual status, short- vs long-stay, highest pressure ulcer stage, comorbidity score, chronic conditions.
We also merged LTCFocus and CASPER with claims data to construct nursing home variables: size, region, ownership. We also created patient- and facility-level variables for
dual, highest pressure ulcer stage and race. Finally, we merged publicly available NHC data of star-ratings and nursing home quality measures with denominator files.
The outputs from this step are the final sample data used to build model and to create exhibits.

| Script Name | Script Description | Input Files | Output Files | New Variables Created |
| --------- | --------- | --------- | --------- | --------- | 
| 6_merge_facility_data.py | Merge denominator files with LCTFocus and CASPER for facility-level variables. | CASPER <br> LTCFocus <br> 2011 - 2017 MBSF Summary Files <br> Output from 5_merge_mds_medpar_samenh.py and 3_select_mds_within_snf_stay.py | CSV files in main_merge_samenh_mbsf_fac, secondary_only_merge_samenh_mbsf_fac and main_merge_snf_mds_within_stay_mbsf_fac  |  |
| 7_merge_mbsfcc.py | Merge MBSF Chronic Conditions with denominator files. | 2011 - 2017 MBSF Chronic Condition Files <br> Output from 6_merge_mbsf_and_fac.py | Parquet files in main_merge_samenh_mbsf_cc and secondary_only_merge_samenh_mbsf_cc |  |
| 8_sl_stay_medpar_mds.py | Identify long-stay and short-stay nursing home residents. For each record in denominator files, look back 100 days to find if there is a 5-day PPS assessment. If there is one, the resident is a short-stay resident. | Output from 2_concat_mds_medpar.py and 7_merge_mbsfcc.py | CSV files: main_merge_samenh_sl.csv and secondary_only_merge_samenh_sl.csv | **short_stay**: a binary indicator of whether a resident is a short-stay resident |
| prepare_comorb_data.sas | Reshape denominator files from wide to long format. | main_merge_samenh_sl.csv |  | **patid**: MEDPAR_ID that uniquely identifies a MedPAR claim<br> **DX**: diagnosis code<br> **Dx_CodeType**: ICD9-CM or ICD10-CM |
| comorbidity.sas | Create comorbidity score based on this [paper](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3100405/). |  | A CSV file: main_comorbidity.csv | **combinedscore**: comorbidity score |
| medicare_count.py | Count the number of unique Medicare Fee-for-serivce residents, the percentage of dual residents and race mix (the percentage of white, black, hispanic, asian, american indian and other residents) for each nursing home within a year. | Raw MDS from 2011 - 2017 <br> Raw MBSF from 2011 - 2017| nh_variables.csv | **medicare_count**: the number of Fee-for-service Medicare residents in each nursing home within the year<br> **{race}_percent**: nursing home race mix <br> **dual_percent**: percentage of dually elligible residents in each nursing home each year| 
| 9_merge_star_ratings.py | Merge NHC star-ratings, quality ratings and pressure ulcer quality measures with denominator files. Create claims-based pressure ulcer rate for each nursing home. | 2011 - 2017 NHC data <br> main_merge_samenh_sl.csv and secondary_only_merge_samenh_sl.csv | CSV files: main_merge_star.csv and secondary_only_merge_star.csv | **pu_rate_medicare**: a nursing home claims-based pressure ulcer rate, calculated by the number of pressure ulcer Medicare IP claims within a year divided by the numebr of Medicare residents in a nursing home |
| 10_construct_model_data.py | Construct patient- and facility-level variables and output final datasets for analysis. | main_comorbidity.csv, main_merge_star.csv and secondary_only_merge_star.csv  | main_model_data.csv, secondary_only_model_data.csv, main_data_final.csv, and secondary_only_data_final.csv |  |
| 10_construct_snf_data.py | Clean the SNF denominator file for main exhibits. | CSV files in main_merge_snf_mds_within_stay_mbsf_fac | main_snf_final_data.csv | **highest stage**: the highest pressure ulcer stage recorded in claims. |
| 10_confirm_snf_mds_matching.py | Include only MDS assessments that are linked to SNF claims from the same nursing home in the final MDS and SNF analytical sample data . | main_snf_final_data.csv | --------- | 

**5. Statistical Analysis (Generate exhibits)**

This section describes scripts used to generate the four exhibits in the paper. Exhibit 2 shows the demographics of primary hospital claims population. 
Exhibit 3 shows the national reporting rate for MDS pressure ulcer items stratified by claims type, short- vs. long-stay and by highest pressure ulcer stage.
Exhibit 4 displays the predictive reporting rates for hypothetical residents using the parameters of multilevel models.
Exhibit 5 displays the relationship between primary hospital claims-based pressure ulcer rates and the NHC MDS-based pressure ulcer quality measures and ratings.

| Script Name | Script Description | Input Files | Output Files |
| --------- | --------- | --------- | --------- | 
| exhibit2.py | Create a descriptive table of demographic information of the primary hospital claims population stratified by short- vs. long-stay and highest pressure ulcer stage. | main_data_final.csv | exhibit2_bystay.xlsx | 
| exhibit3.py | Create a table of the national reporting rate of MDS pressure ulcer items stratified by claims type, short- vs. long-stay and highest pressure ulcer stage. Create a table of pressure ulcer reporting rate for each nursing home that was used to calculate weighted 25th and 75th percentiles of nursing home reporting rate.| main_model_data.csv, secondary_only_model_data.csv, main_snf_final_data.csv | report_rate_table.xlsx, report_rate_nh_table_weight2.csv, report_rate_denominator_count_table.csv| 
| exhibit3_weighted.sas | Calculate 25th and 75th percentiles of nursing home reporting rates, weighted by the number of pressure ulcer claims. | report_rate_nh_table_weight2.csv | exhibit3_quantile_weight_by_claims.csv | 
| (exhibit4) model_final.do | Build a multilevel model with nursing home random intercepts using primary hospital claims, and calculate predictive reporting rates for various hypothetical residents with different races, different pressure ulcers and living in different nursing homes. | main_model_data.csv | Regression result tables and predictive reporting rate tables | 
| exhibit5.py | Create a table of primary hospital claims-based pressure ulcer rates distribution and the NHC MDS-based ulcer measures and star-ratings, as well as correlations between claims-based and MDS-based measures for each year. | main_data_final.csv | main_corr_pu_rate_medicare_and_qm_score_byyear.csv, main_pu_rate_medicare2011-2017_quintile_QM.csv | 










