import pandas as pd
import numpy as np
from calendar import monthrange
import logging
from typing import Dict, Any, List
from pathlib import Path
import sys

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

THRESHOLD_HOURS = 7.5

class FileFormatError(Exception):
    """Custom exception for file format errors."""
    pass

def validate_columns(df: pd.DataFrame, required_columns: List[str], file_type: str) -> None:
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise FileFormatError(f"{file_type} input file is in the wrong format. Missing columns: {', '.join(missing_columns)}")

def read_and_preprocess(file_path: str, is_sap: bool = True) -> pd.DataFrame:
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        if is_sap:
            required_columns = [
                "Personnel No.", "Empl./appl.name", "Status/Proc.Ind.", "Date", 
                "Att./abs. type", "A/A type text", "Number (unit)", "Start time", 
                "End time", "Receiver", "Activity Type", "Short Text", "Agent"
            ]
            validate_columns(df, required_columns, "SAP")
        else:
            required_columns = [
                "Personnel Number", "Name of employee or applicant", "Date",
                "Att./Absence type", "Hours", "Approval date"
            ]
            validate_columns(df, required_columns, "Zalaris")
        
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        logger.error(f"Empty file: {file_path}")
        sys.exit(1)
    except pd.errors.ParserError:
        logger.error(f"Error parsing CSV file: {file_path}")
        sys.exit(1)
    except FileFormatError as e:
        logger.error(str(e))
        sys.exit(1)

    common_renames: Dict[str, str] = {
        'Personnel No.' if is_sap else 'Personnel Number': 'ID',
        'Empl./appl.name' if is_sap else 'Name of employee or applicant': 'Employee',
        'Att./abs. type' if is_sap else 'Att./Absence type': 'AA code'
    }
    
    sap_specific: Dict[str, str] = {
        'A/A type text': 'AA text',
        'Number (unit)': 'Hours'
    }
    
    df = df.rename(columns={**common_renames, **(sap_specific if is_sap else {})})
    
    try:
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
    except ValueError:
        logger.error("Error parsing 'Date' column. Ensure it's in the format 'DD/MM/YYYY'.")
        sys.exit(1)

    if is_sap:
        df['Hours'] = pd.to_numeric(df['Hours'].str.replace(' H', ''), errors='coerce')
        for col in ['Start time', 'End time']:
            df[col] = pd.to_datetime(df[col], format='%H:%M:%S', errors='coerce').dt.time
    else:
        try:
            df['Approval date'] = pd.to_datetime(df['Approval date'], format='%d/%m/%Y')
        except ValueError:
            logger.error("Error parsing 'Approval date' column. Ensure it's in the format 'DD/MM/YYYY'.")
            sys.exit(1)
    
    return df

def create_pivot(df: pd.DataFrame, is_sap: bool = True) -> pd.DataFrame:
    values = ['Hours', 'AA text', 'Receiver', 'Short Text'] if is_sap else ['Hours', 'AA code']
    agg_func: Dict[str, Any] = {'Hours': 'sum', 'AA text': 'first', 'Receiver': 'first', 'Short Text': 'first'} if is_sap else {'Hours': 'sum', 'AA code': 'first'}
    
    pivot_df = df.pivot_table(values=values, index=['Date', 'Employee'], aggfunc=agg_func).reset_index()
    pivot_df['Investigate'] = pivot_df['Hours'] > THRESHOLD_HOURS
    return pivot_df

def calculate_monthly_hours(df: pd.DataFrame) -> pd.DataFrame:
    df['YearMonth'] = df['Date'].dt.to_period('M')
    monthly_df = df.pivot_table(values='Hours', index=['Employee', 'YearMonth'], aggfunc='sum').reset_index()
    
    monthly_df['YearMonth'] = monthly_df['YearMonth'].astype(str)
    monthly_df[['Year', 'Month']] = monthly_df['YearMonth'].str.split('-', expand=True).astype(int)
    
    monthly_df['WorkingDays'] = monthly_df.apply(lambda row: np.busday_count(
        f"{row['Year']}-{row['Month']:02d}-01", 
        f"{row['Year']}-{row['Month']:02d}-{monthrange(row['Year'], row['Month'])[1]}"
    ), axis=1)
    monthly_df['MaxWorkingHours'] = monthly_df['WorkingDays'] * THRESHOLD_HOURS
    monthly_df['Investigate'] = monthly_df['Hours'] > monthly_df['MaxWorkingHours']
    
    return monthly_df

def compare_hours(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    df1 = df1.set_index(['Date', 'Employee'])
    df2 = df2.set_index(['Date', 'Employee'])
    
    combined = pd.concat([df1['Hours'], df2['Hours']], axis=1, keys=['Zalaris Hours', 'SAP Hours'])
    combined = combined.reset_index()
    
    combined['Comparison'] = np.select(
        [combined['Zalaris Hours'] > combined['SAP Hours'],
         combined['Zalaris Hours'] < combined['SAP Hours']],
        [1, 2],
        default=0
    )
    
    return combined[combined['Comparison'] != 0].reset_index(drop=True)

def save_to_csv(df: pd.DataFrame, file_path: str) -> None:
    try:
        df.to_csv(file_path, index=False, encoding='utf-8-sig')
        logger.info(f"File saved successfully: {file_path}")
    except PermissionError:
        logger.error(f"Permission denied when saving file: {file_path}")
        raise
    except IOError as e:
        logger.error(f"IOError when saving file: {file_path}. Error: {str(e)}")
        raise

def main():
    results = {}
    data_dir = Path('data')
    
    for source in ['SAP', 'Zalaris']:
        try:
            df = read_and_preprocess(data_dir / f'{source} Hours.csv', is_sap=(source == 'SAP'))
            
            daily_df = create_pivot(df, is_sap=(source == 'SAP'))
            save_to_csv(daily_df, data_dir / f'{source} Hours_pivot.csv')
            
            monthly_df = calculate_monthly_hours(df)
            save_to_csv(monthly_df, data_dir / f'{source} Hours_monthly.csv')
            
            results[f'{source}_Daily'] = daily_df
            results[f'{source}_Monthly'] = monthly_df
        except Exception as e:
            logger.error(f"Error processing {source} data: {str(e)}")
            sys.exit(1)
    
    if 'SAP_Daily' in results and 'Zalaris_Daily' in results:
        comparison_df = compare_hours(results['SAP_Daily'], results['Zalaris_Daily'])
        save_to_csv(comparison_df, data_dir / 'Comparison_Results.csv')
    else:
        logger.warning("Unable to perform comparison due to missing data")
        sys.exit(1)

if __name__ == "__main__":
    main()