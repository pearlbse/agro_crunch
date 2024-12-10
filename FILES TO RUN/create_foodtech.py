import pandas as pd

df = pd.read_csv('../bulk_download/organizations.csv')

df['category_groups_list'] = df['category_groups_list'].fillna('')
df = df[df['category_groups_list'].apply(lambda x: 'Agriculture and Farming' in x or 'Food and Beverage' in x)]

df.to_csv('../input/foodtech.csv')
