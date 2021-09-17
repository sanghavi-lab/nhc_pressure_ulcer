# ----------------------------------------------------------------------------------------------------------------------#
# Project: NURSING HOME PRESSURE ULCER
# Author: Zoey Chen
# ----------------------------------------------------------------------------------------------------------------------#

## THIS CODE IS USED TO CREATE THE SECOND EXHIBIT IN THE PAPER:
## THE DESCRIPTIVE ANALYSIS OF PRIMARY HOSPITAL CLAIMS POPULATION
## FINAL OUTPUT IS A TABLE DESCRIBING ALL INDIVIDUAL-LEVEL VARIABLES

import pandas as pd
import yaml

## define paths
years = range(2011, 2018)
yaml_path = '/gpfs/data/sanghavi-lab/Zoey/gardner/nhc_pressure_ulcer/final_code/'
path = yaml.safe_load(open(yaml_path + 'data_path.yaml'))

read_file = ['main_data_final.csv', 'secondary_only_data_final.csv']

final_table = []
stay = ['short_stay', 'long_stay']
short_stay = [True, False]

for n in range(len(read_file)):
    if n==0:

        ## read in final dataset
        df = pd.read_csv(path['exhibits']['input'] + read_file[n], low_memory=False)

        ## define categorical columns
        ccol = ['race_name']
        ## define numeric columns
        ncol = ['age', 'female', 'dual', 'disability', 'combinedscore', 'count_cc']
        ## define chronic conditions columns
        cc_col = [l for l in list(df.columns) if l.endswith('_final')]

        ## combine american indian and other for cell reporting rules
        df = df.replace({'race_name': {"american indian": "other"}})

        ## calculate the count of chronic conditions for each patient
        df['count_cc'] = df.apply(lambda x: x[cc_col].sum(), axis=1)

        ## calculate sample size by short- vs. long-stay and highest pressure ulcer
        print(df.groupby(['short_stay', 'highest stage'])['BENE_ID'].count())
        ## calculate sample size by short- vs. long-stay and race categories
        print(df.groupby(['short_stay','race_name'])['BENE_ID'].count())

        ## calculate the grand mean of numeric variables by short- vs. long-stay
        for col in ncol:
            print(df.groupby('short_stay')[col].mean())

        ## calculate percentage of highest pressure ulcer stage for each race
        race_by_stage = df.groupby(['short_stay', 'highest stage', 'race_name'])['BENE_ID'].count().rename('mean')

        race_by_stage = (race_by_stage/race_by_stage.groupby(level=[0, 2]).sum()).reset_index()
        print(race_by_stage)
        race_by_stage.columns = ['short_stay', 'var', 'highest stage', 'mean']
        race_by_stage = race_by_stage.sort_values(by='mean', ascending=False)

        ## calculate mean of the numeric variables within each ulcer stage
        mean_by_stage = []
        for col in ncol:
            mean_var = df.groupby(['short_stay', 'highest stage'])[col].mean().rename('mean').reset_index()
            mean_var['var'] = col
            mean_by_stage.append(mean_var)
        mean_by_stage.append(race_by_stage)
        mean_by_stage = pd.concat(mean_by_stage)

        table = mean_by_stage.pivot_table(index=['short_stay', 'var'], columns='highest stage', values='mean').reset_index()

        table['data'] = read_file[n]
        final_table.append(table)

pd.concat(final_table).to_excel(path['exhibits']['output'] + 'exhibit2_raw.xlsx')



