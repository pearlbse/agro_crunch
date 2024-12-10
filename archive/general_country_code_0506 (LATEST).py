import pandas as pd
import warnings
from unidecode import unidecode

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

warnings.filterwarnings("ignore")

iso_df = pd.read_csv('input/isocodes.csv')  
codes_original = pd.read_json('input/geonames.json')

nan_counts_pc = []
nan_counts_l = []

for index, row in iso_df.iterrows():
    iso2 = row['iso2']
    iso3 = row['iso3']

    print(iso3)
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

    #### 1. EXACT MATCHES ####

    # `place_name`
    codes1 = codes.copy()
    codes1 = codes1[['postal_code', 'latitude', 'longitude', 'admin_name1', 'place_name']]

    codes1['admin_name1'] = codes1['admin_name1'].str.lower()
    codes1['place_name'] = codes1['place_name'].str.lower()

    codes1.drop_duplicates(subset=['admin_name1', 'place_name'], inplace=True)

    merged_df = pd.merge(df, codes1, left_on=['city', 'region'], right_on=['place_name', 'admin_name1'], how='left')

    merged_df = merged_df.iloc[:, 0:10]

    # `admin_name2`
    codes2 = codes.copy()
    codes2 = codes2[['postal_code', 'latitude', 'longitude', 'admin_name1', 'admin_name2']]

    codes2['admin_name1'] = codes2['admin_name1'].str.lower()
    codes2['admin_name2'] = codes2['admin_name2'].str.lower()

    codes2.drop_duplicates(subset=['admin_name1', 'admin_name2'], inplace=True)

    merged_df = pd.merge(merged_df, codes2, left_on=['city', 'region'], right_on=['admin_name2', 'admin_name1'], how='left')
    # merged_df.drop_duplicates(subset=['uuid'], inplace=True)

    merged_df['postal_code_y'].fillna(merged_df['postal_code'], inplace=True)

    merged_df['latitude_x'].fillna(merged_df['latitude_y'], inplace=True)
    merged_df['longitude_x'].fillna(merged_df['longitude_y'], inplace=True)

    merged_df = merged_df.iloc[:, 0:10]

    # `admin_name3`
    codes3 = codes.copy()
    codes3 = codes3[['postal_code','admin_name1', 'admin_name3', 'latitude', 'longitude']]

    codes3['admin_name1'] = codes3['admin_name1'].str.lower()
    codes3['admin_name3'] = codes3['admin_name3'].str.lower()

    codes3.drop_duplicates(subset=['admin_name1', 'admin_name3'], inplace=True)

    merged_df = pd.merge(merged_df, codes3, left_on=['city', 'region'], right_on=['admin_name3', 'admin_name1'], how='left')

    merged_df['postal_code_y'].fillna(merged_df['postal_code'], inplace=True)

    merged_df['latitude_x'].fillna(merged_df['latitude'], inplace=True)
    merged_df['longitude_x'].fillna(merged_df['longitude'], inplace=True)

    merged_df = merged_df.iloc[:, 0:10]

    # `place_name` ONLY
    codes1.drop_duplicates(subset=['place_name'], inplace=True)
    merged_df = pd.merge(merged_df, codes1, left_on=['city'], right_on=['place_name'], how='left')

    merged_df['postal_code_y'].fillna(merged_df['postal_code'], inplace=True)

    merged_df['latitude_x'].fillna(merged_df['latitude'], inplace=True)
    merged_df['longitude_x'].fillna(merged_df['longitude'], inplace=True)

    merged_df = merged_df.iloc[:, 0:10]

    print('Exact matches done!')

    #### 2. PARTIAL MATCHES ####

    # Define the columns for substring containment
    left_on = 'city'
    right_on = {
        'codes1': 'place_name',
        'codes2': 'admin_name2',
        'codes3': 'admin_name3'
    }

    # Define a function for substring matching and selecting the best match
    def substring_select_best_match(row, dfs, left_col, right_cols):
        best_match = None
        best_postal_code = None
        best_lat = None
        best_long = None
        
        for df_name, right_col in right_cols.items():
            # Filter out NaN values before applying str.contains
            matches = dfs[df_name][right_col].str.contains(row[left_col], case=False, na=False)
            if any(matches):
                best_match = dfs[df_name][right_col][matches].iloc[0]
                best_postal_code = dfs[df_name].loc[matches, 'postal_code'].iloc[0]
                best_lat = dfs[df_name].loc[matches, 'latitude'].iloc[0]
                best_long = dfs[df_name].loc[matches, 'longitude'].iloc[0]
                break  # Exit loop after finding a match
        
        return best_match, best_postal_code, 100, best_lat, best_long  # Always return best_score as 100

    filtered_df = merged_df[merged_df['postal_code_y'].isna()]

    if not filtered_df.empty:  # Check if filtered_df is not empty
        # Apply substring matching and select best match for each row in filtered_df
        matches_df = filtered_df.apply(lambda row: substring_select_best_match(row, {'codes1': codes1, 'codes2': codes2, 'codes3': codes3}, left_on, right_on), axis=1, result_type='expand')
        matches_df.columns = ['best_match', 'best_postal_code', 'best_score', 'best_lat', 'best_long']

        matches_df.replace('', pd.NA, inplace=True)

        merged_df = pd.merge(merged_df, matches_df, how='left', left_index=True, right_index=True)
        merged_df['postal_code_y'].fillna(merged_df['best_postal_code'], inplace=True)
        merged_df['latitude_x'].fillna(merged_df['best_lat'], inplace=True)
        merged_df['longitude_x'].fillna(merged_df['best_long'], inplace=True)

        merged_df = merged_df.iloc[:, 0:10]

        print('Partial matches done!')
        
    else:
        print("filtered_df is empty. Skipping substring matching and subsequent tasks.")


    #### 3. EXISTING POSTAL CODES ####

    # Across rows: df
    codes_df = merged_df[merged_df['postal_code_y'].notna()]

    codes_df = codes_df[['region', 'city', 'postal_code_y', 'latitude_x', 'longitude_x']]
    codes_df.drop_duplicates(subset=['region', 'city'], inplace=True)

    merged_df = pd.merge(merged_df, codes_df, left_on=['city', 'region'], right_on=['city', 'region'], how='left')

    merged_df['postal_code_y_x'].fillna(merged_df['postal_code_y_y'], inplace=True)
    merged_df['latitude_x_x'].fillna(merged_df['latitude_x_y'], inplace=True)
    merged_df['longitude_x_x'].fillna(merged_df['longitude_x_y'], inplace=True)

    merged_df = merged_df.iloc[:, 0:10]

    print('Existing postal codes done!')

    #### 4. FILL REMAINING WITH UNPROCESSED POSTAL CODES ####

    # Filling NaNs with Existing Postal Codes
    merged_df['postal_code_y_x'].fillna(merged_df['postal_code_x'], inplace=True)

    # Across rows: df
    codes_df = df[df['postal_code'].notna()]

    codes_df = codes_df[['region', 'city', 'postal_code']]
    codes_df.drop_duplicates(subset=['region', 'city'], inplace=True)


    merged_df = pd.merge(merged_df, codes_df, left_on=['city', 'region'], right_on=['city', 'region'], how='left')
    merged_df['postal_code_y_x'].fillna(merged_df['postal_code'], inplace=True)
    merged_df = merged_df.iloc[:, 0:10]

    # Fill coordinates based on above filling
    codes4 = codes.copy()
    codes4 = codes4[['postal_code', 'latitude', 'longitude']]
    codes4.drop_duplicates(subset=['postal_code'], inplace=True)

    merged_df = pd.merge(merged_df, codes4, left_on=['postal_code_y_x'], right_on=['postal_code'], how='left')

    merged_df['latitude_x_x'].fillna(merged_df['latitude'], inplace=True)
    merged_df['longitude_x_x'].fillna(merged_df['longitude'], inplace=True)

    merged_df = merged_df.iloc[:, 0:10]

    print('Filling done!')


    # Save to csv
    merged_df = merged_df.rename(columns={'postal_code_x': 'pc_crunchbase',
                                          'postal_code_y_x': 'pc_filled',
                                          'latitude_x_x':    'latitude',
                                          'longitude_x_x':   'longitude'})

    merged_df.to_csv('general/' + iso3 + '_processed.csv')

    print('Saved as csv!')


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