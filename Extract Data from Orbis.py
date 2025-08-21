import pandas as pd
import os
from glob import glob

# Path to the directory containing the Excel files
directory_path = r'C:\Users\habim\OneDrive - Hanken Svenska handelshogskolan\Desktop\All\Orbis\Profit_Loss'

# Get all Excel files in the directory
excel_files = glob(os.path.join(directory_path, '*.xlsx'))

# Function to read and append sheets with the same name
def append_sheets(sheet_name, files):
    appended_data = pd.DataFrame()
    for file in files:
        xls = pd.ExcelFile(file)
        if sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name)
            appended_data = pd.concat([appended_data, df], ignore_index=True)
    return appended_data

# Appending each sheet separately
sheets = pd.ExcelFile(excel_files[0]).sheet_names
appended_data_dict = {sheet: append_sheets(sheet, excel_files) for sheet in sheets}

# Example sheet: 'Results'
results_data = appended_data_dict['Results']

# Transforming the 'Results' sheet into panel data
# Extract year, variable name, and currency from the column names
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

# Export the panel data to Excel
output_file_path = os.path.join(directory_path, 'Transformed_Panel_Data.xlsx')
panel_data.to_excel(output_file_path, index=False)

print(f"Data transformation complete. File saved as '{output_file_path}'")
