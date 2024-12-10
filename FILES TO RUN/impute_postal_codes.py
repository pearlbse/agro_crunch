import pandas as pd
import numpy as np
import warnings
from unidecode import unidecode

warnings.filterwarnings("ignore")

iso_df = pd.read_csv('../input/isocodes.csv')  
countries = pd.read_csv('../input/countries.csv')
df_original = pd.read_csv('../input/foodtech.csv')
codes_original = pd.read_json('../input/geonames.json')

nan_counts_pc = []
nan_counts_l = []

for index, row in iso_df.iterrows():
    iso2 = row['iso2']
    iso3 = row['iso3']

    country = countries.loc[countries['alpha-3'] == iso3, 'name'].iloc[0]

    print(iso3)

    # Crunchbase Dataframe
    df = df_original.copy()
    df = df[df['country_code'] == iso3]
    df = df[['uuid','country_code', 'state_code', 'region', 'city', 'address', 'postal_code']]

    df[['city', 'region']] = df[['city', 'region']].apply(lambda col: col.apply(unidecode).str.lower())
    df.rename(columns={'postal_code': 'pc_cb'}, inplace=True)

    columns_to_add = {'pc_fill': np.nan, 'lat_fill': np.nan, 'long_fill': np.nan}
    df = df.assign(**columns_to_add)

    # Postal Codes
    codes = codes_original.copy()     
    codes = codes[codes['country_code'] == iso2]

    codes = codes[['postal_code', 'latitude', 'longitude', 'place_name', 'admin_name1', 'admin_name2','admin_name3',]].copy()

    columns_to_normalize = ['admin_name1', 'admin_name2', 'admin_name3', 'place_name']

    # Lowercase and apply unidecode for each column
    for col in columns_to_normalize:
        codes[col] = codes[col].str.lower()
        codes[col] = codes[col].apply(lambda x: unidecode(x) if x is not None else None)

    #### 1. EXACT MATCHES ####

    def exact_match(codes, df, cb_cols, pc_cols, num):

        codes_copy = codes.copy()
        codes_copy.drop_duplicates(subset=pc_cols, inplace=True)

        df1 = pd.merge(df, codes_copy, left_on=cb_cols, right_on=pc_cols, how='left')

        df1['pc_fill'].fillna(df1['postal_code'], inplace=True)
        df1['lat_fill'].fillna(df1['latitude'], inplace=True)
        df1['long_fill'].fillna(df1['longitude'], inplace=True)

        df1 = df1.iloc[:, 0:num]
        
        return df1
    
    #### 1A. EXACT MATCHES: CB to PC ####

    # Define the combinations of columns
    combos = [
        (['city', 'region'], ['place_name', 'admin_name1']),
        (['city', 'region'], ['admin_name2', 'admin_name1']),
        (['city', 'region'], ['admin_name3', 'admin_name1']),
        (['city'], ['place_name'])
    ]

    # Iterate through each combination
    for cb_cols, pc_cols in combos:
        merged_df = exact_match(codes, df, cb_cols, pc_cols, 10)

    print('Exact matching (CB to PC) done!')

    #### 1B. EXACT MATCHES: CB to Asia Cities ####

    ref = pd.read_csv('input/asia_cities_mapped.csv')
    ref = ref[ref['old_country'] == country]
    ref.drop_duplicates(subset=['old_city', 'old_region'], inplace=True)

    for col in ref.columns:
        ref[col] = ref[col].str.lower()

    if not ref.empty:
        merged_df = pd.merge(merged_df, ref, how='left', left_on=['city'], right_on=['old_city'])

        combos = [
        (['new_city'], ['place_name']),
        (['new_city'], ['admin_name1']),
        (['new_city'], ['admin_name2']),
        (['new_city'], ['admin_name3'])
    ]

    # Iterate through each combination
        for pc_cols, cb_cols in combos:
            merged_df = exact_match(codes, merged_df, pc_cols, cb_cols, 11)

        print('Exact matching (CB to Asia city references) done!')
        
    #### 1C. EXACT MATCHES: CB to World Cities ####

    ref = pd.read_csv('input/world_cities_mapped.csv')
    ref = ref[ref['ISO'] == iso3]
    ref.drop_duplicates(subset=['old_city'], inplace=True)

    for col in ref.columns:
        ref[col] = ref[col].str.lower()

    if not ref.empty:
        merged_df = pd.merge(merged_df, ref, how='left', left_on=['city'], right_on=['old_city'])

        combos = [
        (['old_city'], ['place_name']),
        (['old_city'], ['admin_name1']),
        (['old_city'], ['admin_name2']),
        (['old_city'], ['admin_name3'])
    ]

    # Iterate through each combination
        for pc_cols, cb_cols in combos:
            merged_df = exact_match(codes, merged_df, pc_cols, cb_cols, 17)
        
        merged_df['lat_fill'].fillna(merged_df['lat_mapped'], inplace=True)
        merged_df['long_fill'].fillna(merged_df['long_mapped'], inplace=True)

        merged_df = merged_df.iloc[:, 0:11]

        print('Exact matching (CB to World city references) done!')


    #### 2. PARTIAL MATCHES ####

    # Define a function for substring matching and selecting the best match
    def substring_select_best_match(row, codes, left_col, right_cols):
        best_match = None
        best_postal_code = None
        best_lat = None
        best_long = None
        
        for right_col in right_cols:
            # Filter out NaN values before applying str.contains
            matches = codes[right_col].str.contains(row[left_col], case=False, na=False)
            if any(matches):
                best_match = codes.loc[matches, right_col].iloc[0]
                best_postal_code = codes.loc[matches, 'postal_code'].iloc[0]
                best_lat = codes.loc[matches, 'latitude'].iloc[0]
                best_long = codes.loc[matches, 'longitude'].iloc[0]
                break  # Exit loop after finding a match
        
        return best_match, best_postal_code, 100, best_lat, best_long  # Always return best_score as 100

    # Filter rows where pc_fill is NaN
    filtered_df = merged_df[merged_df['pc_fill'].isna()]

    if not filtered_df.empty:  # Check if filtered_df is not empty

        # Define the columns for substring containment
        left_on = 'city'
        right_on = ['place_name', 'admin_name2', 'admin_name3']

        # Apply substring matching and select best match for each row in filtered_df
        matches_df = filtered_df.apply(lambda row: substring_select_best_match(row, codes, left_on, right_on), axis=1, result_type='expand')
        matches_df.columns = ['best_match', 'best_postal_code', 'best_score', 'best_lat', 'best_long']

        matches_df.replace('', pd.NA, inplace=True)

        merged_df = pd.merge(merged_df, matches_df, how='left', left_index=True, right_index=True)
        merged_df['pc_fill'].fillna(merged_df['best_postal_code'], inplace=True)
        merged_df['lat_fill'].fillna(merged_df['best_lat'], inplace=True)
        merged_df['long_fill'].fillna(merged_df['best_long'], inplace=True)

        merged_df = merged_df.iloc[:, 0:11]
    else:
        print("filtered_df is empty. Skipping substring matching and subsequent tasks.")


    #### 3. EXISTING POSTAL CODES ####

    # Across rows: df
    codes_df = merged_df[merged_df['pc_cb'].notna()]

    codes_df = codes_df[['region', 'city', 'pc_cb']]
    codes_df.drop_duplicates(subset=['region', 'city'], inplace=True)

    merged_df = pd.merge(merged_df, codes_df, on=['region', 'city'], how='left')
    merged_df['pc_fill'].fillna(merged_df['pc_cb_y'], inplace=True)

    merged_df = merged_df.iloc[:, 0:10]

    merged_df = merged_df.drop_duplicates(subset='uuid')

    print('Existing postal codes done!')

    # Save to csv
    merged_df = merged_df.rename(columns={'pc_cb_x': 'pc_cb'})

    merged_df.to_csv('0508/' + iso3 + '_processed.csv')

    print('Saved as csv!')

    # Checking NaNs
    nan_count_pc = merged_df['pc_fill'].isna().sum()
    nan_counts_pc.append({'iso3': iso3, 'nan_count': nan_count_pc})

    nan_count_l = merged_df['lat_fill'].isna().sum()
    nan_counts_l.append({'iso3': iso3, 'nan_count': nan_count_l})

# Create a DataFrame from the list of dictionaries
nan_counts_df_pc = pd.DataFrame(nan_counts_pc)
nan_counts_df_l = pd.DataFrame(nan_counts_l)

# Save the DataFrame to a CSV file
nan_counts_df_pc.to_csv('output/nan_counts_per_country_PC.csv', index=False)
nan_counts_df_l.to_csv('output/nan_counts_per_country_L.csv', index=False)