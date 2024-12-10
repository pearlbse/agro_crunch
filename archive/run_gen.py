import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import pandas as pd

# Read CSV file containing ISO codes for countries
iso_df = pd.read_csv('path_to_your_csv_file.csv')

# Load the Jupyter Notebook (.ipynb) file
with open('path_to_your_notebook.ipynb', 'r') as f:
    notebook = nbformat.read(f, as_version=4)

# Create a preprocessor to execute the notebook cells
preprocessor = ExecutePreprocessor(timeout=600, kernel_name='python3')  # Adjust timeout and kernel_name as needed

# Iterate over each row in the ISO dataframe
for index, row in iso_df.iterrows():
    iso2 = row['iso2']
    iso3 = row['iso3']
    
    # Modify the arguments 'iso2' and 'iso3' in the notebook's cells
    for cell in notebook.cells:
        if cell.cell_type == 'code':
            cell_source = cell.source.replace("iso2 = 'ES'", f"iso2 = '{iso2}'")
            cell_source = cell_source.replace("iso3 = 'ESP'", f"iso3 = '{iso3}'")
            cell.source = cell_source

    # Execute the modified notebook
    try:
        preprocessor.preprocess(notebook)
    except Exception as e:
        print(f"Error executing notebook for {iso2}: {e}")
        continue

    print(f"Notebook executed successfully for {iso2}")

# Save the modified notebook with executed cells
with open('path_to_save_modified_notebook.ipynb', 'w') as f:
    nbformat.write(notebook, f)
