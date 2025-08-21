import pandas as pd
import os
from glob import glob
from tqdm import tqdm
import time

# Path to the directory containing the Excel files
directory_path = r'C:\Users\s180020\Desktop\Orbis\Assets'

# Get all Excel files in the directory
excel_files = glob(os.path.join(directory_path, '*.xlsx'))

# Function to read and append sheets with the same name
def append_sheets(sheet_name, files):
    appended_data = []
    start_time = time.time()
    
    for i, file in enumerate(tqdm(files, desc=f'Processing sheet: {sheet_name}')):
        xls = pd.ExcelFile(file)
        if sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name)
            appended_data.append(df)
        
        elapsed_time = time.time() - start_time
        remaining_time = (elapsed_time / (i + 1)) * (len(files) - (i + 1))
        print(f"Currently analyzing: {file}")
        print(f"Estimated remaining time: {remaining_time:.2f} seconds")
    
    return pd.concat(appended_data, ignore_index=True)

# Appending each sheet separately
sheets = pd.ExcelFile(excel_files[0]).sheet_names
appended_data_dict = {sheet: append_sheets(sheet, excel_files) for sheet in sheets}

# Example sheet: 'Results'
results_data = appended_data_dict['Results']
results_data.to_csv('intermediate_results_data.csv', index=False)  # Save intermediate result to CSV
del results_data  # Free up memory

# Reload the intermediate results data to minimize memory usage
results_data = pd.read_csv('intermediate_results_data.csv')

# Transforming the 'Results' sheet into panel data
def extract_year_variable_currency(column_name):
    parts = column_name.split(' ')
    year = parts[-1]
    variable = ' '.join(parts[:-3]).replace('\n', ' ')
    currency = parts[-2]
    return variable, year, currency

# Melt the DataFrame to long format
melted_df = results_data.melt(id_vars=['Company name Latin alphabet', 'Country', 'Country ISO code'], 
                              var_name='Variable_Year', value_name='Value')

# Extract Variable, Year, and Currency from the 'Variable_Year' column
melted_df[['Variable', 'Year', 'Currency']] = melted_df['Variable_Year'].str.extract(r'(.+)\n(.+)\s(.+)$')
melted_df['Variable'] = melted_df['Variable'].str.replace('\n', ' ')
melted_df['Unit'] = 'A Million'

# Pivot the DataFrame to get variables as columns
panel_data = melted_df.pivot_table(index=['Company name Latin alphabet', 'Country', 'Country ISO code', 'Year', 'Currency', 'Unit'], 
                                   columns='Variable', values='Value').reset_index()

# Rename columns
panel_data = panel_data.rename(columns={'Company name Latin alphabet': 'Company Name'})

# Function to split DataFrame into chunks
def split_dataframe(df, chunk_size):
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]
    return chunks

# Define chunk size to ensure each file is within the limit
max_rows_per_chunk = 100000  # Adjust based on your needs

# Split the DataFrame into chunks
chunks = split_dataframe(panel_data, max_rows_per_chunk)

# Export each chunk to a separate STATA file
output_stata_base_path = os.path.join(directory_path, 'Transformed_Panel_Data_Part')
for i, chunk in enumerate(chunks):
    chunk.to_stata(f'{output_stata_base_path}_{i + 1}.dta', write_index=False)

print("Data transformation and export complete. Files saved as STATA files.")
