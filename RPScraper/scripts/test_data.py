import pandas as pd
import glob
import os
from pathlib import Path

def analyze_rpr_in_files():
    # Get path to data directory
    data_dir = Path(__file__).parent.parent / 'data' / 'dates'
    
    # Lists to store results
    all_dfs = []
    
    # Process GB files
    gb_files = glob.glob(str(data_dir / 'gb' / '*.csv'))
    for file in gb_files:
        try:
            df = pd.read_csv(file)
            if not df.empty:  # Only process non-empty dataframes
                df['date'] = os.path.basename(file).replace('.csv', '').replace('_', '-')
                df['country'] = 'gb'
                all_dfs.append(df)
        except pd.errors.EmptyDataError:
            print(f"Skipping empty file: {file}")
            continue
    
    # Process IRE files
    ire_files = glob.glob(str(data_dir / 'ire' / '*.csv'))
    for file in ire_files:
        try:
            df = pd.read_csv(file)
            if not df.empty:  # Only process non-empty dataframes
                df['date'] = os.path.basename(file).replace('.csv', '').replace('_', '-')
                df['country'] = 'ire'
                all_dfs.append(df)
        except pd.errors.EmptyDataError:
            print(f"Skipping empty file: {file}")
            continue
    
    if not all_dfs:
        print("No data files found!")
        return
    
    # Combine all data
    combined_df = pd.concat(all_dfs, ignore_index=True)
    
    # Print columns for debugging
    print("\nAvailable columns:", combined_df.columns.tolist())
    
    # Check if required columns exist
    required_columns = ['date', 'rpr', 'num']
    if not all(col in combined_df.columns for col in required_columns):
        print("Error: Some required columns are missing.")
        return
    
    # Convert date to datetime
    combined_df['date'] = pd.to_datetime(combined_df['date'])
    
    # Group by date and calculate statistics
    stats = combined_df.groupby('date').agg({
        'rpr': lambda x: (x.isna() | (x == '')).mean() * 100,  # Percentage of missing values
        'num': 'count'  # Total records
    }).reset_index()
    
    stats.columns = ['date', 'missing_rpr_pct', 'total_records']
    stats = stats.sort_values('date')
    
    # Print results
    print("\nRPR Missing Value Analysis by Date:")
    print("=" * 80)
    print(f"{'Date':<12} {'Total Records':<15} {'Missing RPR %':<10}")
    print("-" * 80)
    
    for _, row in stats.iterrows():
        print(f"{row['date'].strftime('%Y-%m-%d'):<12} {row['total_records']:<15} {row['missing_rpr_pct']:.2f}%")
    
    # Overall statistics
    total_records = combined_df.shape[0]
    total_missing = (combined_df['rpr'].isna() | (combined_df['rpr'] == '')).sum()
    overall_missing_pct = (total_missing / total_records) * 100
    
    print("\nOverall Statistics:")
    print(f"Total Records: {total_records}")
    print(f"Total Missing RPR: {total_missing}")
    print(f"Overall Missing Percentage: {overall_missing_pct:.2f}%")

if __name__ == "__main__":
    analyze_rpr_in_files()