
import pandas as pd
import numpy as np
from calendar import monthrange

THRESHOLD_HOURS = 7.5

def read_and_preprocess(file_path, is_sap=True):
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    
    common_renames = {
        'Personnel No.': 'ID',
        'Empl./appl.name' if is_sap else 'Name of employee or applicant': 'Employee',
        'Att./abs. type' if is_sap else 'Att./Absence type': 'AA code'
    }
    
    sap_specific = {
        'A/A type text': 'AA text',
        'Number (unit)': 'Hours'
    }
    
    df = df.rename(columns=common_renames | (sap_specific if is_sap else {}))
    
    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
    
    if is_sap:
        df['Hours'] = df['Hours'].str.replace(' H', '').astype(float)
        for col in ['Start time', 'End time']:
            df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce').dt.time
    else:
        df['Approval date'] = pd.to_datetime(df['Approval date'], format='%d/%m/%Y')
    
    return df

def create_pivot(df, is_sap=True):
    values = ['Hours', 'AA text', 'Receiver', 'Short Text'] if is_sap else ['Hours', 'AA code']
    agg_func = {'Hours': 'sum', 'AA text': 'first', 'Receiver': 'first', 'Short Text': 'first'} if is_sap else {'Hours': 'sum', 'AA code': 'first'}
    
    pivot_df = df.pivot_table(values=values, index=['Date', 'Employee'], aggfunc=agg_func).reset_index()
    pivot_df['Investigate'] = pivot_df['Hours'] > THRESHOLD_HOURS
    return pivot_df

def calculate_monthly_hours(df):
    df['YearMonth'] = df['Date'].dt.to_period('M')
    monthly_df = df.pivot_table(values='Hours', index=['Employee', 'YearMonth'], aggfunc='sum').reset_index()
    
    monthly_df['YearMonth'] = monthly_df['YearMonth'].astype(str)
    monthly_df[['Year', 'Month']] = monthly_df['YearMonth'].str.split('-', expand=True).astype(int)
    
    monthly_df['WorkingDays'] = monthly_df.apply(lambda row: np.busday_count(f"{row['Year']}-{row['Month']:02d}-01", 
                                                                             f"{row['Year']}-{row['Month']:02d}-{monthrange(row['Year'], row['Month'])[1]}"), axis=1)
    monthly_df['MaxWorkingHours'] = monthly_df['WorkingDays'] * THRESHOLD_HOURS
    monthly_df['Investigate'] = monthly_df['Hours'] > monthly_df['MaxWorkingHours']
    
    return monthly_df

def compare_hours(df1, df2):
    results = []
    for _, row in df1.iterrows():
        date = row['Date']
        employee = row['Employee']
        hours_df1 = row['Hours']
        
        # Find the corresponding row in df2
        df2_row = df2[(df2['Date'] == date) & (df2['Employee'] == employee)]
        
        if not df2_row.empty:
            hours_df2 = df2_row['Hours'].values[0]
            if hours_df1 > hours_df2:
                comparison = 1
            elif hours_df1 < hours_df2:
                comparison = 2
            else:
                comparison = 0
            
            if comparison != 0:
                results.append({
                    'Date': date,
                    'Employee': employee,
                    'Zalaris Hours': hours_df1,
                    'SAP Hours': hours_df2,
                    'Comparison': comparison
                })
    
    results_df = pd.DataFrame(results)
    return results_df

def main():
    results={}
    for source in ['SAP', 'Zalaris']:
        df = read_and_preprocess(f'data/{source} Hours.csv', is_sap=(source == 'SAP'))
        
        daily_df = create_pivot(df, is_sap=(source == 'SAP'))
        daily_df.to_csv(f'data/{source} Hours_pivot.csv', index=False, encoding='utf-8-sig')
        
        monthly_df = calculate_monthly_hours(df)
        monthly_df.to_csv(f'data/{source} Hours_monthly.csv', index=False, encoding='utf-8-sig')
        results[f'{source}_Daily'] = daily_df
        results[f'{source}_Monthly'] = monthly_df
        
    comparison_df = compare_hours(results['SAP_Daily'], results['Zalaris_Daily'])
    comparison_df.to_csv('data/Comparison_Results.csv', index=False, encoding='utf-8-sig')

        
    

if __name__ == "__main__":
    main()
