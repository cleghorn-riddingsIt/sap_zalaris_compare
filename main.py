
import pandas as pd
import numpy as np
from calendar import monthrange
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
THRESHOLD_HOURS = 8



def read_and_preprocess(file_path, is_zalaris=True):
    df = pd.read_csv(file_path, encoding='utf-8-sig')


    common_renames = {
        'Personnel No.': 'ID',
        'Empl./appl.name' if is_zalaris else 'Name of employee or applicant': 'Employee',
        'Att./abs. type' if is_zalaris else 'Att./Absence type': 'AA code'
    }
    
    zalaris_specific = {
        'A/A type text': 'AA text',
        'Number (unit)': 'Hours'
    }
    
    df = df.rename(columns=common_renames | (zalaris_specific if is_zalaris else {}))
    if is_zalaris:
        df['Hours'] = df['Hours'].str.replace(' H', '').astype(float)
        df['AA code'] = df['AA code'].astype(str)
        export_to_excel(df[(df['AA code'] == '800') | (df['AA code'] == '9020')],'data/output/clockinout_hours.xlsx') ##- save the clock in clock out and delivery times rows
        df = df[(df['AA code'] != '800') & (df['AA code'] != '9020')]  # Remove the clock in clock out and delivery times  columns
        export_to_excel(df,'data/output/wowbs_hours.xlsx') ##- actual wbs or wo hours
        for col in ['Start time', 'End time']:
            df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce').dt.time
    else:
        df['Approval date'] = pd.to_datetime(df['Approval date'], format='%d/%m/%Y',errors='coerce')
    
    df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
    
  
    
    return df

def create_daily_hours(df):
    values = ['Hours']
    agg_func = {'Hours': 'sum'}
    
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

def compare_hours(df1, df2, is_monthly=False):
    results = []
    datecol = 'YearMonth' if is_monthly else 'Date'
    for _, row in df1.iterrows():
        date = row[datecol]
        employee = row['Employee']
        hours_df1 = row['Hours']
        #assume no match
        comparison='Hours not found in SAP'
        hours_df2=-1 
        # Find the corresponding row in df2
        df2_row = df2[(df2[datecol] == date) & (df2['Employee'] == employee)]
        
        if not df2_row.empty:
            hours_df2 = df2_row['Hours'].values[0]
            if (hours_df1 - hours_df2)>0.5:
                comparison = 'Zalaris Hours Higher'
            elif (hours_df2 - hours_df1)>0.5:
                comparison = 'SAP Hours Higher'
            else:
                comparison = 'Equal Hours'
            
        if comparison != 'Equal Hours':
                results.append({
                    'Date': date,
                    'Employee': employee,
                    'Zalaris Hours': hours_df1,
                    'SAP Hours': hours_df2,
                    'Comparison': comparison
            })
    results_df = pd.DataFrame(results)
    mask = (results_df['Zalaris Hours'] == 0) & (results_df['SAP Hours'] == -1) #ignore anything with zero hours and -1(ie not in SAP)
    return results_df[~mask]
def export_to_excel(df, filename):
    try:
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        logging.info(f"DataFrame successfully exported to {filename}")
    except Exception as e:
        logging.error(f"Failed to export DataFrame to {filename}: {e}")

def main():
    results={}
    for source in ['SAP', 'Zalaris']:
        df = read_and_preprocess(f'data/{source} Hours.csv', is_zalaris=(source == 'Zalaris'))
        
        daily_df = create_daily_hours(df)
        export_to_excel(daily_df,f'data/output/{source} Hours_daily.xlsx')
        
        monthly_df = calculate_monthly_hours(df)
        export_to_excel(monthly_df,f'data/output/{source} Hours_monthly.xlsx')
        results[f'{source}_Daily'] = daily_df
        results[f'{source}_Monthly'] = monthly_df
        
    comparison_df = compare_hours(results['Zalaris_Daily'],results['SAP_Daily'])
    export_to_excel(comparison_df,'data/output/Comparison_Daily.xlsx')
    comparison_df = compare_hours(results['Zalaris_Monthly'],results['SAP_Monthly'],True)
    export_to_excel(comparison_df,'data/output/Comparison_Monthly.xlsx')

        
    

if __name__ == "__main__":
    main()
