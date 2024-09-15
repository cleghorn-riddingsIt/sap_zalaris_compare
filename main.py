import pandas as pd

# Step 2: Read the CSV file into a DataFrame
file_path = 'data/SAP Hours.csv'
df = pd.read_csv(file_path, encoding='utf-8-sig')

# Step 3: Rename the columns
df = df.rename(columns={
    'Personnel No.': 'SAP ID',
    'Empl./appl.name': 'Employee',
    'Att./abs. type': 'AA code',
    'A/A type text': 'AA text',
    'Number (unit)': 'Hours'
})

# Step 4: Convert the Date column to datetime format
df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')

# Step 5: Convert the Start time and End time columns to time format
df['Start time'] = pd.to_datetime(df['Start time'], format='%H:%M:%S', errors='coerce').dt.time
df['End time'] = pd.to_datetime(df['End time'], format='%H:%M:%S', errors='coerce').dt.time

# Step 6: Clean the Hours column and convert it to float
df['Hours'] = df['Hours'].str.replace(' H', '').astype(float)

# Step 7: Create a pivot table with Date and Employee as indexes and Hours and other columns as values
pivot_df = df.pivot_table(
    values=['Hours', 'AA text', 'Receiver', 'Short Text'],
    index=['Date', 'Employee'],
    aggfunc={'Hours': 'sum', 'AA text': 'first', 'Receiver': 'first', 'Short Text': 'first'}
).reset_index()

# Step 8: Add the Investigate column
pivot_df['Investigate'] = pivot_df['Hours'] > 7.5

# Step 9: Save the pivot table DataFrame back to a CSV file
pivot_df.to_csv('data/SAP Hours_pivot.csv', index=False, encoding='utf-8-sig')