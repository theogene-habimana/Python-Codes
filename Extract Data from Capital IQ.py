import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.options import Options  # Changed from Chrome to Edge

# Load the data with company IDs
df = pd.read_excel('C:/Users/habim/Desktop/company_data.xlsx')

# Set up the Edge WebDriver
options = Options()
options.add_experimental_option("prefs", {
    "download.default_directory": r"C:\Users\habim\Desktop\Downloaded_Reports",
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})
driver = webdriver.Edge(executable_path='C:/path/to/msedgedriver.exe', options=options)  # Path to Edge WebDriver

# Function to log into Capital IQ
def login():
    driver.get('https://www.capitaliq.com')
    time.sleep(2)  # Adjust based on your connection speed
    driver.find_element(By.ID, 'username').send_keys('YourUsername')
    driver.find_element(By.ID, 'password').send_keys('YourPassword', Keys.RETURN)

# Function to navigate and download reports
def download_reports(company_id):
    report_url = f'https://www.capitaliq.com/CIQDotNet/Filings/FilingsAnnualReports.aspx?CompanyId={company_id}'
    driver.get(report_url)
    # Assume the correct element ID for the download button
    download_button = driver.find_element(By.ID, 'download_button_id')
    download_button.click()
    time.sleep(5)  # Wait for the download to complete

# Main script execution
login()
time.sleep(5)  # Wait for the login to complete

for company_id in df['CompanyID']:
    download_reports(company_id)

driver.quit()  # Close the browser once done
