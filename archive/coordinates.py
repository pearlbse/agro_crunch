import pandas as pd
import warnings
from unidecode import unidecode

warnings.filterwarnings("ignore")

iso_df = pd.read_csv('input/isocodes.csv') 
codes_df = pd.read_json('input/geonames.json')

nan_counts = []

for index, row in iso_df.iterrows():
    iso2 = row['iso2']
    iso3 = row['iso3']

    df = pd.read_csv('general/' + iso3 + '_processed.csv')

    codes = codes_df.copy()
    codes = codes[codes['country_code'] == iso2]

    codes.drop_duplicates(subset=['postal_code'], inplace=True)

    df['pc_filled'] = df['pc_filled'].astype(str).str.rstrip('.0')
    codes['postal_code'] = codes['postal_code'].astype(str).str.rstrip('.0')

    merged_df = pd.merge(df, codes[['postal_code', 'latitude', 'longitude']], left_on='pc_filled', right_on='postal_code', how='left')

    # merged_df.drop(columns=['postal_code'], inplace=True)

    merged_df.to_csv('coordinates/' + iso3 + '_coordinates.csv')

    nan_count = merged_df['latitude'].isna().sum()
    nan_counts.append({'iso3': iso3, 'nan_count': nan_count})

nan_counts_df = pd.DataFrame(nan_counts)

nan_counts_df.to_csv('output/nan_counts_coordinates.csv', index=False)
