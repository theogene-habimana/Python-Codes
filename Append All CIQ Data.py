import pandas as pd
import os
from glob import glob
from tqdm import tqdm

# Path to the directory containing the Excel files
directory_path = r'C:\Users\habim\Desktop\Non_Listed_CIQ\Append'
output_path = r'C:\Users\habim\Desktop\Non_Listed_CIQ\Append'

# Ensure the output directory exists
os.makedirs(output_path, exist_ok=True)

# Get all Excel files in the directory, ignoring temporary files
excel_files = [file for file in glob(os.path.join(directory_path, '*.xlsx')) if not os.path.basename(file).startswith('~$')]

# Function to remove unnamed columns
def remove_unnamed_columns(df):
    return df.loc[:, ~df.columns.str.contains('^Unnamed')]

# Function to read and append sheets with the same name
def append_sheets(sheet_name, files):
    appended_data = pd.DataFrame()
    for file in tqdm(files, desc=f'Appending {sheet_name}'):
        xls = pd.ExcelFile(file)
        if sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name)
            df = remove_unnamed_columns(df)  # Remove unnamed columns
            df['Company Name'] = os.path.basename(file).split('_')[1]
            if 'Filing Date' in df.columns:
                df['Filing Date'] = pd.to_datetime(df['Filing Date'], errors='coerce').dt.year  # Extract only the year, handle errors
            appended_data = pd.concat([appended_data, df], ignore_index=True)
    return appended_data

# Appending each sheet separately
first_file = pd.ExcelFile(excel_files[0])
sheets = first_file.sheet_names
appended_data_dict = {sheet: append_sheets(sheet, excel_files) for sheet in sheets}

# Save each appended sheet to the output directory
output_file_path = os.path.join(output_path, 'Appended_SheetsAll.xlsx')
with pd.ExcelWriter(output_file_path) as writer:
    for sheet, data in appended_data_dict.items():
        data.to_excel(writer, sheet_name=sheet, index=False)

print(f"Data appending complete. File saved as '{output_file_path}'")

