import pandas as pd
import warnings
from unidecode import unidecode

warnings.filterwarnings("ignore")

iso_df = pd.read_csv('input/isocodes.csv')  
codes_original = pd.read_json('input/geonames.json')

nan_counts_pc = []
nan_counts_l = []

for index, row in iso_df.iterrows():
    iso2 = row['iso2']
    iso3 = row['iso3']

    # Crunchbase Dataframe
    df = pd.read_csv('input/foodtech.csv')
    df = df[df['country_code'] == iso3]
    df = df[['uuid','country_code', 'state_code', 'region', 'city', 'address', 'postal_code']]

    df['city'] = df['city'].apply(unidecode)
    df['region'] = df['region'].apply(unidecode)

    df['city'] = df['city'].str.lower()
    df['region'] = df['region'].str.lower()

    # Postal Codes
    codes = codes_original.copy()     
    codes = codes[codes['country_code'] == iso2]

    codes['admin_name1'] = codes['admin_name1'].apply(lambda x: unidecode(x) if x is not None else None)
    codes['admin_name2'] = codes['admin_name2'].apply(lambda x: unidecode(x) if x is not None else None)
    codes['place_name'] = codes['place_name'].apply(lambda x: unidecode(x) if x is not None else None)

    # `admin_name2`
    codes1 = codes.copy()
    codes1 = codes1[['postal_code','admin_name1', 'admin_name2', 'latitude', 'longitude']]

    codes1['admin_name1'] = codes1['admin_name1'].str.lower()
    codes1['admin_name2'] = codes1['admin_name2'].str.lower()

    codes1.drop_duplicates(subset=['admin_name1', 'admin_name2'], inplace=True)

    merged_df = pd.merge(df, codes1, left_on=['city', 'region'], right_on=['admin_name2', 'admin_name1'], how='left')
    merged_df.drop_duplicates(subset=['uuid'], inplace=True)

    del codes1

    # `place_name`
    codes2 = codes.copy()
    codes2 = codes2[['postal_code','admin_name1', 'place_name', 'latitude', 'longitude']]

    codes2['admin_name1'] = codes2['admin_name1'].str.lower()
    codes2['place_name'] = codes2['place_name'].str.lower()

    codes2.drop_duplicates(subset=['admin_name1', 'place_name'], inplace=True)

    merged_df = pd.merge(merged_df, codes2, left_on=['city', 'region'], right_on=['place_name', 'admin_name1'], how='left')

    merged_df['postal_code_y'].fillna(merged_df['postal_code'], inplace=True)

    merged_df['latitude_x'].fillna(merged_df['latitude_y'], inplace=True)
    merged_df['longitude_x'].fillna(merged_df['longitude_y'], inplace=True)

    merged_df = merged_df.iloc[:, 0:12]

    # `place_name` ONLY
    codes2.drop_duplicates(subset=['place_name'], inplace=True)
    merged_df = pd.merge(merged_df, codes2, left_on=['city'], right_on=['place_name'], how='left')

    merged_df['postal_code_y'].fillna(merged_df['postal_code'], inplace=True)

    merged_df['latitude_x'].fillna(merged_df['latitude'], inplace=True)
    merged_df['longitude_x'].fillna(merged_df['longitude'], inplace=True)

    merged_df = merged_df.iloc[:, 0:12]

    del codes2

    # Filling NaNs with Existing Postal Codes
    merged_df['postal_code_y'].fillna(merged_df['postal_code_x'], inplace=True)

    # Across rows: df
    codes_df = df[df['postal_code'].notna()]

    codes_df = codes_df[['region', 'city', 'postal_code']]
    codes_df.drop_duplicates(subset=['region', 'city'], inplace=True)

    merged_df = pd.merge(merged_df, codes_df, left_on=['city', 'region'], right_on=['city', 'region'], how='left')
    merged_df['postal_code_y'].fillna(merged_df['postal_code'], inplace=True)

    merged_df = merged_df.iloc[:, 0:12]

    # Fill coordinates based on above filling
    codes3 = codes.copy()
    codes3 = codes3[['postal_code', 'latitude', 'longitude']]

    merged_df = pd.merge(merged_df, codes3, left_on=['postal_code_y'], right_on=['postal_code'], how='left')

    merged_df['latitude_x'].fillna(merged_df['latitude'], inplace=True)
    merged_df['longitude_x'].fillna(merged_df['longitude'], inplace=True)

    merged_df = merged_df.iloc[:, 0:12]

    del codes3

    # Checking NaNs
    # nan_df_pc = merged_df[merged_df['postal_code_y'].isna()]
    # nan_df_pc['region'].value_counts()

    # nan_df_l = merged_df[merged_df['latitude_x'].isna()]
    # nan_df_l['region'].value_counts()

    # Save to csv
    merged_df = merged_df.rename(columns={'postal_code_x': 'pc_crunchbase',
                                          'postal_code_y': 'pc_filled',
                                          'latitude_x':    'latitude',
                                          'longitude_x':   'longitude'})

    merged_df.to_csv('general/' + iso3 + '_processed.csv')

    # Checking NaNs
    nan_count_pc = merged_df['pc_filled'].isna().sum()
    nan_counts_pc.append({'iso3': iso3, 'nan_count': nan_count_pc})

    nan_count_l = merged_df['latitude'].isna().sum()
    nan_counts_l.append({'iso3': iso3, 'nan_count': nan_count_l})

# Create a DataFrame from the list of dictionaries
nan_counts_df_pc = pd.DataFrame(nan_counts_pc)
nan_counts_df_l = pd.DataFrame(nan_counts_l)

# Save the DataFrame to a CSV file
nan_counts_df_pc.to_csv('output/nan_counts_per_country_PC.csv', index=False)
nan_counts_df_l.to_csv('output/nan_counts_per_country_L.csv', index=False)