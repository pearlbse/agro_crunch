import os
import pandas as pd

def concatenate_csv_files(folder_path, output_file):
    # List to store individual dataframes
    df_list = []

    # Iterate over all files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            # Construct full file path
            file_path = os.path.join(folder_path, filename)
            # Read the CSV file into a dataframe
            df = pd.read_csv(file_path)
            # Append the dataframe to the list
            df_list.append(df)

    # Concatenate all dataframes in the list
    concatenated_df = pd.concat(df_list, ignore_index=True)
    concatenated_df = concatenated_df.drop_duplicates(subset='uuid')

    # Write the concatenated dataframe to a new CSV file
    concatenated_df.to_csv(output_file, index=False)

# Define the folder path and output file name
folder_path = '../countries/'
output_file = '../output/postal_codes.csv'

# Call the function to concatenate CSV files
concatenate_csv_files(folder_path, output_file)
