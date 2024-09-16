import pandas as pd
import numpy as np
import calendar

# Define the threshold value
threshold_hours = 7.5

# Step 2: Read the CSV file into a DataFrame
file_path = 'data/SAP Hours.csv'
sapdf = pd.read_csv(file_path, encoding='utf-8-sig')

# Step 3: Rename the columns
sapdf = sapdf.rename(columns={
    'Personnel No.': 'SAP ID',
    'Empl./appl.name': 'Employee',
    'Att./abs. type': 'AA code',
    'A/A type text': 'AA text',
    'Number (unit)': 'Hours'
})

# Step 4: Convert the Date column to datetime format
sapdf['Date'] = pd.to_datetime(sapdf['Date'], format='%d/%m/%Y')

# Step 5: Convert the Start time and End time columns to time format
sapdf['Start time'] = pd.to_datetime(sapdf['Start time'], format='%H:%M:%S', errors='coerce').dt.time
sapdf['End time'] = pd.to_datetime(sapdf['End time'], format='%H:%M:%S', errors='coerce').dt.time

# Step 6: Clean the Hours column and convert it to float
sapdf['Hours'] = sapdf['Hours'].str.replace(' H', '').astype(float)

# Step 7: Create a pivot table with Date and Employee as indexes and Hours and other columns as values
sappivot_df = sapdf.pivot_table(
    values=['Hours', 'AA text', 'Receiver', 'Short Text'],
    index=['Date', 'Employee'],
    aggfunc={'Hours': 'sum', 'AA text': 'first', 'Receiver': 'first', 'Short Text': 'first'}
).reset_index()

# Step 8: Add the Investigate column
sappivot_df['Investigate'] = sappivot_df['Hours'] > threshold_hours

# Step 9: Save the pivot table DataFrame back to a CSV file
sappivot_df.to_csv('data/SAP Hours_pivot.csv', index=False, encoding='utf-8-sig')

# Step 10: Extract month and year from Date
sapdf['YearMonth'] = sapdf['Date'].dt.to_period('M')

# Step 11: Create a new pivot table that sums up all the hours per Employee for any month
monthly_hours_df = sapdf.pivot_table(
    values='Hours',
    index=['Employee', 'YearMonth'],
    aggfunc='sum'
).reset_index()

# Step 12: Calculate the number of working days for each month
def working_days_in_month(year, month):
    month_range = calendar.monthrange(year, month)
    return np.busday_count(f'{year}-{month:02d}-01', f'{year}-{month:02d}-{month_range[1]}')  # Monday to Friday

# Step 13: Calculate the maximum working hours per month
monthly_hours_df['YearMonth'] = monthly_hours_df['YearMonth'].astype(str)
monthly_hours_df['Year'] = monthly_hours_df['YearMonth'].str[:4].astype(int)
monthly_hours_df['Month'] = monthly_hours_df['YearMonth'].str[5:7].astype(int)
monthly_hours_df['WorkingDays'] = monthly_hours_df.apply(lambda row: working_days_in_month(row['Year'], row['Month']), axis=1)
monthly_hours_df['MaxWorkingHours'] = monthly_hours_df['WorkingDays'] * threshold_hours

# Step 14: Add the Investigate column
monthly_hours_df['Investigate'] = monthly_hours_df['Hours'] > monthly_hours_df['MaxWorkingHours']

# Step 15: Save the monthly hours pivot table DataFrame to a CSV file
monthly_hours_df.to_csv('data/SAP Hours_monthly.csv', index=False, encoding='utf-8-sig')