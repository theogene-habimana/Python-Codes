1. Append All CIQ Data.py

Purpose: Combines multiple Excel files from Capital IQ into one consolidated dataset
.

Features:

Iterates through all Excel files in a given directory.

Removes unnamed/temporary columns.

Extracts the Company Name and Filing Year from filenames and metadata.

Appends sheets with the same name across files into a single Excel workbook.

Output: Appended_SheetsAll.xlsx

2. Codes to panel data.py

Purpose: Converts wide-format regional Excel datasets (with years as columns) into long panel data by ISIN and Year
.

Features:

Cleans and normalizes headers.

Expands each ISIN to cover the full year range (2000–2024).

Reattaches company metadata (e.g., name, country, industry).

Replaces missing values with ".".

Output: *_Panel.xlsx (panel-format file)

3. Combine Orbis Data.py

Purpose: Merges Orbis Excel files (e.g., Assets or Profit & Loss statements) and transforms them into structured panel datasets
.

Features:

Appends sheets across all Orbis Excel files.

Transforms “Results” sheets into panel data.

Extracts Variable, Year, Currency from column names.

Splits large datasets into chunks and exports as STATA (.dta) files.

Output: Multiple Transformed_Panel_Data_Part*.dta files.

4. Download Data from Eikon API.py

Purpose: Automates data extraction from LSEG Refinitiv Eikon API
.

Features:

Pulls TR.SharesHeld, TR.InvestorType, ISIN, and filing dates for 2000–2024.

Handles year-end snapshots with fallback logic.

Writes results into STATA files (snapshot and aggregated by investor type).

Splits very large datasets into multiple .dta chunks for efficiency.

Output: Yearly SharesHeldYYYY.dta and SharesHeldYYYY_bytype.dta

5. Extract Data from Capital IQ.py

Purpose: Automates downloading company filings from Capital IQ via Selenium
.

Features:

Logs into Capital IQ using stored credentials.

Iterates through company IDs (from Excel file).

Navigates to annual report filings page and downloads reports.

Saves files into a predefined download directory.

Output: PDF/Excel reports in Downloaded_Reports/

6. Extract Data from Orbis.py

Purpose: Processes Orbis Profit & Loss Excel files into a standardized panel dataset
.

Features:

Appends all sheets across multiple Orbis files.

Melts and restructures “Results” sheets into long format.

Extracts Variable, Year, Currency.

Pivots data back into a clean panel format.

Output: Transformed_Panel_Data.xlsx

⚙️ Requirements

Python 3.8+

Libraries:

pandas

tqdm

openpyxl

selenium

eikon (for Refinitiv Eikon API)

WebDriver (Edge or Chrome depending on setup)
