import pandas as pd
import numpy as np
from datetime import datetime

def main():
    """
    Main function to process FII/DII options and futures data
    """
    print("=== FII/DII OPTIONS & FUTURES ANALYSIS ===")
    
    # File paths
    input_file = 'fii_dii_data_sep_25.csv'
    output_file = 'processed_options_futures.csv'
    
    try:
        # Step 1: Load data
        print("Loading data...")
        df = pd.read_csv(input_file)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(['date', 'institution', 'trade_type'])
        
        print(f"Data loaded: {df.shape} rows")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
        print(f"Available trade types: {list(df['trade_type'].unique())}")
        
        # Step 2: Filter out CASH data - keep only options and futures
        options_futures_types = [t for t in df['trade_type'].unique() 
                               if 'CASH' not in t.upper()]
        
        df_filtered = df[df['trade_type'].isin(options_futures_types)].copy()
        print(f"After removing CASH: {df_filtered.shape} rows")
        print(f"Remaining trade types: {options_futures_types}")
        
        # Step 3: Process OPTIONS (CALL and PUT)
        print("\nProcessing OPTIONS...")
        options_data = df_filtered[df_filtered['trade_type'].isin(['CALL', 'PUT'])]
        
        if not options_data.empty:
            options_pivot = options_data.pivot_table(
                index=['date', 'institution'], 
                columns='trade_type', 
                values='net_oi', 
                fill_value=0
            ).reset_index()
            
            # Calculate Options Net OI = CALL - PUT
            if 'CALL' in options_pivot.columns and 'PUT' in options_pivot.columns:
                options_pivot['OPTIONS_NET_OI'] = options_pivot['CALL'] - options_pivot['PUT']
            else:
                options_pivot['OPTIONS_NET_OI'] = 0
            
            # Rename for clarity
            options_pivot = options_pivot.rename(columns={
                'CALL': 'CALL_OI',
                'PUT': 'PUT_OI'
            })
            print(f"Options processed: {options_pivot.shape} rows")
        else:
            options_pivot = pd.DataFrame(columns=['date', 'institution', 'CALL_OI', 'PUT_OI', 'OPTIONS_NET_OI'])
            print("No options data found")
        
        # Step 4: Process FUTURES
        print("\nProcessing FUTURES...")
        futures_types = [t for t in options_futures_types if t not in ['CALL', 'PUT']]
        print(f"Futures types: {futures_types}")
        
        if futures_types:
            futures_data = df_filtered[df_filtered['trade_type'].isin(futures_types)]
            futures_pivot = futures_data.pivot_table(
                index=['date', 'institution'],
                columns='trade_type', 
                values='net_oi', 
                fill_value=0
            ).reset_index()
            
            # Clean column names
            futures_pivot.columns = [col.replace('-', '_') if col not in ['date', 'institution'] else col 
                                   for col in futures_pivot.columns]
            print(f"Futures processed: {futures_pivot.shape} rows")
        else:
            futures_pivot = pd.DataFrame(columns=['date', 'institution'])
            print("No futures data found")
        
        # Step 5: Combine all data
        print("\nCombining data...")
        if not options_pivot.empty and not futures_pivot.empty:
            final_df = pd.merge(futures_pivot, options_pivot, on=['date', 'institution'], how='outer')
        elif not options_pivot.empty:
            final_df = options_pivot.copy()
        elif not futures_pivot.empty:
            final_df = futures_pivot.copy()
        else:
            print("No data to process!")
            return
        
        final_df = final_df.fillna(0).sort_values(['date', 'institution'])
        
        # Step 6: Calculate daily changes
        print("\nCalculating daily changes...")
        numeric_columns = final_df.select_dtypes(include=[np.number]).columns.tolist()
        
        for col in numeric_columns:
            final_df[f'{col}_CHANGE'] = final_df.groupby('institution')[col].diff()
        
        # Step 7: Save results
        final_df.to_csv(output_file, index=False)
        print(f"\nResults saved to: {output_file}")
        
        # Step 8: Display summary
        print("\n=== SUMMARY ===")
        print(f"Final dataset shape: {final_df.shape}")
        print(f"Columns: {list(final_df.columns)}")
        
        print("\n=== LATEST DATA ===")
        latest_date = final_df['date'].max()
        latest_data = final_df[final_df['date'] == latest_date]
        print(f"Date: {latest_date}")
        for _, row in latest_data.iterrows():
            print(f"{row['institution']}:")
            for col in numeric_columns:
                if col in row:
                    print(f"  {col}: {row[col]:.2f}")
        
        print("\n=== OPTIONS NET OI ANALYSIS ===")
        if 'OPTIONS_NET_OI' in final_df.columns:
            options_summary = final_df.groupby('institution')['OPTIONS_NET_OI'].agg(['mean', 'std', 'min', 'max']).round(2)
            print(options_summary)
        
        print("\nAnalysis complete!")
        
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found!")
        print("Make sure the CSV file is in the same directory as this script.")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 