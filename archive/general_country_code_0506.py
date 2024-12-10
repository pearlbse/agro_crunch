import pandas as pd
import warnings
from unidecode import unidecode

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

warnings.filterwarnings("ignore")

iso_df = pd.read_csv('input/isocodes_subset2.csv')  
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

    #### 2. EXISTING POSTAL CODES: FUZZY MATCHES ####

    # # Filter out rows where both 'postal_code_x' and 'postal_code_y' are not NaN
    # filtered_df = merged_df[(merged_df['postal_code_x'].notna()) & (merged_df['postal_code_y'].isna())]

    # # Calculate the desired length based on the length of postal codes in codes['postal_code']
    # desired_length = len(codes['postal_code'].iloc[0])

    # # Create a new column in filtered_df to truncate 'postal_code_x' values
    # filtered_df['postal_code_x_truncated'] = filtered_df['postal_code_x'].str.slice(0, desired_length)

    # # Define a function for fuzzy matching and selecting the best match
    # def fuzzy_select_best_match(row_value, choices):
    #     # Ensure row_value is a string
    #     row_value = str(row_value)
    #     best_match = process.extractOne(row_value, choices, scorer=fuzz.token_sort_ratio)
    #     return best_match[0], best_match[1]

    # # Apply fuzzy matching and select best match for each value in 'postal_code_x_truncated' in the filtered_df
    # filtered_df['fuzzy_match'], filtered_df['fuzzy_score'] = zip(*filtered_df['postal_code_x_truncated'].apply(lambda x: fuzzy_select_best_match(x, codes['postal_code'])))
    # filtered_df = filtered_df[filtered_df['fuzzy_score'] > 50]

    # filtered_df = pd.merge(filtered_df, codes[['postal_code', 'latitude', 'longitude']], how='left', left_on='fuzzy_match', right_on='postal_code', suffixes=('', '_codes'))
    # filtered_df.drop_duplicates(subset=['uuid'], inplace=True)

    # merged_df = pd.merge(merged_df, filtered_df[['uuid','postal_code', 'latitude', 'longitude']], how='left', on='uuid')

    # merged_df['postal_code_y'].fillna(merged_df['postal_code'], inplace=True)
    # merged_df['latitude_x'].fillna(merged_df['latitude'], inplace=True)
    # merged_df['longitude_x'].fillna(merged_df['longitude'], inplace=True)

    # merged_df = merged_df.iloc[:, 0:10] 

    #### 3. PARTIAL MATCHES ####

    # Define the columns for substring containment
    left_on = 'city'
    right_on = {
        'codes1': 'place_name',
        'codes2': 'admin_name2',
        'codes3': 'admin_name3'
    }

    # Define a function for substring matching and selecting the best match
    def substring_select_best_match(row, dfs, left_col, right_cols):
        best_score = 0
        best_match = ''
        best_postal_code = ''
        best_lat = ''
        best_long = ''
        for df_name, right_col in right_cols.items():
            # Filter out NaN values before applying str.contains
            matches = dfs[df_name][right_col].str.contains(row[left_col], case=False, na=False)
            if any(matches):
                best_score = 100  # Max score since substring was found
                best_match = dfs[df_name][right_col][matches].iloc[0]
                best_postal_code = dfs[df_name].loc[matches, 'postal_code'].iloc[0]
                best_lat = dfs[df_name].loc[matches, 'latitude'].iloc[0]
                best_long = dfs[df_name].loc[matches, 'longitude'].iloc[0]
                break  # Exit loop after finding a match
        return best_match, best_postal_code, best_score, best_lat, best_long

    # Filter rows where postal_code_y is NaN
    filtered_df = merged_df[merged_df['postal_code_y'].isna()]

    # Apply substring matching and select best match for each row in filtered_df
    matches_df = filtered_df.apply(lambda row: substring_select_best_match(row, {'codes1': codes1, 'codes2': codes2, 'codes3': codes3}, left_on, right_on), axis=1, result_type='expand')
    matches_df.columns = ['best_match', 'best_postal_code', 'best_score', 'best_lat', 'best_long']

    matches_df.replace('', pd.NA, inplace=True)

    merged_df = pd.merge(merged_df, matches_df, how='left', left_index=True, right_index=True)
    merged_df['postal_code_y'].fillna(merged_df['best_postal_code'], inplace=True)
    merged_df['latitude_x'].fillna(merged_df['best_lat'], inplace=True)
    merged_df['longitude_x'].fillna(merged_df['best_long'], inplace=True)

    merged_df = merged_df.iloc[:, 0:10] 

    #### 4. EXISTING POSTAL CODES ####

    # Across rows: df
    codes_df = merged_df[merged_df['postal_code_y'].notna()]

    codes_df = codes_df[['region', 'city', 'postal_code_y', 'latitude_x', 'longitude_x']]
    codes_df.drop_duplicates(subset=['region', 'city'], inplace=True)

    merged_df = pd.merge(merged_df, codes_df, left_on=['city', 'region'], right_on=['city', 'region'], how='left')

    merged_df['postal_code_y_x'].fillna(merged_df['postal_code_y_y'], inplace=True)
    merged_df['latitude_x_x'].fillna(merged_df['latitude_x_y'], inplace=True)
    merged_df['longitude_x_x'].fillna(merged_df['longitude_x_y'], inplace=True)

    merged_df = merged_df.iloc[:, 0:10]

    #### 5. FILL REMAINING WITH UNPROCESSED POSTAL CODES ####

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

    # Save to csv
    merged_df = merged_df.rename(columns={'postal_code_x': 'pc_crunchbase',
                                          'postal_code_y_x': 'pc_filled',
                                          'latitude_x_x':    'latitude',
                                          'longitude_x_x':   'longitude'})

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