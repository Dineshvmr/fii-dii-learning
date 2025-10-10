import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
import getpass
from sqlalchemy import create_engine

class FIIStrengthCalculator:
    """
    FII strength calculation with combined participant data approach
    """
    
    def __init__(self):
        self.df = None
        
    def load_data(self):
        """Load and prepare data from PostgreSQL database"""
        # Hardcoded DB details (password prompted)
        host = "stagingdb.int.sensibull.com"
        port = "5432"
        dbname = "sensibull_qa"
        user = "sensibull_staging"
        password = getpass.getpass("Enter database password: ")
        
        # SQL query to fetch raw data (no hardcoded date filter to load all relevant data)
        query = """
        SELECT 
            traded_day as date,
            trading_institution as institution,
            trade_type,
            CASE 
                -- For CASH: Net = BUY - SELL
                WHEN trade_type = 'CASH' THEN 
                    SUM(CASE WHEN buy_or_sell = 'BUY' THEN value WHEN buy_or_sell = 'SELL' THEN -value END)
                
                -- For FUTURE-INDEX: Net = LONG - SHORT
                WHEN trade_type = 'FUTURE-INDEX' THEN 
                    SUM(CASE WHEN buy_or_sell = 'LONG' THEN value WHEN buy_or_sell = 'SHORT' THEN -value END)
                
                -- For CALL/PUT options: Net OI = LONG - SHORT
                WHEN trade_type IN ('CALL', 'PUT') THEN 
                    SUM(CASE WHEN buy_or_sell = 'LONG' THEN value WHEN buy_or_sell = 'SHORT' THEN -value END)
            END as net_oi
        FROM fii_dii_data
        WHERE 
            trade_type IN ('CASH', 'FUTURE-INDEX', 'CALL', 'PUT')
            AND (
                (trade_type = 'CASH' AND buy_or_sell IN ('BUY', 'SELL')) OR
                (trade_type IN ('FUTURE-INDEX', 'CALL', 'PUT') AND buy_or_sell IN ('LONG', 'SHORT'))
            )
        GROUP BY 
            traded_day, 
            trading_institution, 
            trade_type
        ORDER BY 
            traded_day, 
            trading_institution, 
            trade_type;
        """
        
        # Create SQLAlchemy engine for better pandas integration
        engine_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(engine_str)
        
        # Load data using pandas with SQLAlchemy
        df_raw = pd.read_sql(query, engine)
        df_raw['date'] = pd.to_datetime(df_raw['date'])
        engine.dispose()  # Close connection
        
        # Check if data is empty or missing net_oi
        if df_raw.empty:
            raise ValueError("No data returned from query. Check DB table and SQL conditions.")
        if 'net_oi' not in df_raw.columns:
            raise ValueError(f"net_oi column not found. Available columns: {df_raw.columns.tolist()}")
        
        # Pivot to wide format: net_oi as columns per trade_type
        df = df_raw.pivot(index=['date', 'institution'], columns='trade_type', values='net_oi').reset_index()
        
        # Rename columns to match expected format (drop 'CASH' if present, or keep for future use)
        col_mapping = {
            'FUTURE-INDEX': 'FUTURE_INDEX',
            'CALL': 'CALL_OI',
            'PUT': 'PUT_OI'
        }
        df = df.rename(columns=col_mapping)
        
        # Drop CASH column if it exists (Python doesn't use it currently)
        if 'CASH' in df.columns:
            df = df.drop(columns=['CASH'])
        
        # Sort by date and institution
        df = df.sort_values(['date', 'institution'])
        
        # Compute daily changes (diff) for relevant columns
        change_cols = ['FUTURE_INDEX', 'CALL_OI', 'PUT_OI']
        for col in change_cols:
            if col in df.columns:
                df[f'{col}_CHANGE'] = df.groupby('institution')[col].diff()
                # Forward fill the first diff as 0; drop NaNs later in analysis
                df[f'{col}_CHANGE'] = df[f'{col}_CHANGE'].fillna(0)
        
        # Filter to relevant institutions
        df = df[df['institution'].isin(['FII', 'PRO', 'CLIENT'])].copy()
        
        self.df = df
        return self.df
    
    def get_threshold(self, percentile, sorted_list):
        """Custom percentile calculation"""
        if not sorted_list or len(sorted_list) <= 1:
            return sorted_list[0] if sorted_list else 0
        
        t_pos = (percentile / 100) * (len(sorted_list) - 1)
        t_pos_int, t_pos_frac = divmod(t_pos, 1)
        t_pos_int = int(t_pos_int)
        
        if t_pos_int >= len(sorted_list) - 1:
            return sorted_list[-1]
        
        if t_pos_frac == 0:
            return sorted_list[t_pos_int]
        else:
            return sorted_list[t_pos_int] + (
                t_pos_frac * (sorted_list[t_pos_int + 1] - sorted_list[t_pos_int])
            )
    
    def get_strength_level(self, value, percentile_80, percentile_40_20, is_change=False):
        """Determine strength level based on percentile thresholds"""
        abs_value = abs(value)
        threshold = percentile_40_20 if not is_change else percentile_40_20  # 40% for OI, 20% for change
        
        if abs_value >= percentile_80:
            return "STRONG"
        elif abs_value >= threshold:
            return "MEDIUM"
        else:
            return "MILD"
    
    def get_direction(self, value, segment_name):
        """Determine bullish/bearish direction"""
        if 'Put Options' in segment_name:
            return "BEARISH" if value > 0 else "BULLISH"  # Opposite for puts
        else:
            return "BULLISH" if value > 0 else "BEARISH"

    def parse_strength(self, strength_str):
        """Parse final strength string to (intensity, direction) tuple"""
        if strength_str == "INDECISIVE":
            return ("INDECISIVE", "INDECISIVE")
        parts = strength_str.split()
        if len(parts) == 2:
            return (parts[0], parts[1])
        return ("INDECISIVE", "INDECISIVE")
    
    def apply_truth_table(self, oi_strength, oi_direction, change_strength, change_direction):
        """Apply truth table logic"""
        oi_combined = f"{oi_strength} {oi_direction}"
        change_combined = f"{change_strength} {change_direction}"
        
        truth_table = {
            ("STRONG BULLISH", "STRONG BULLISH"): "STRONG BULLISH",
            ("STRONG BULLISH", "MEDIUM BULLISH"): "STRONG BULLISH",
            ("STRONG BULLISH", "MILD BULLISH"): "STRONG BULLISH",
            ("STRONG BULLISH", "STRONG BEARISH"): "INDECISIVE",
            ("STRONG BULLISH", "MEDIUM BEARISH"): "INDECISIVE",
            ("STRONG BULLISH", "MILD BEARISH"): "STRONG BULLISH",
            ("MEDIUM BULLISH", "STRONG BULLISH"): "STRONG BULLISH",
            ("MEDIUM BULLISH", "MEDIUM BULLISH"): "MEDIUM BULLISH",
            ("MEDIUM BULLISH", "MILD BULLISH"): "MEDIUM BULLISH",
            ("MEDIUM BULLISH", "STRONG BEARISH"): "INDECISIVE",
            ("MEDIUM BULLISH", "MEDIUM BEARISH"): "INDECISIVE",
            ("MEDIUM BULLISH", "MILD BEARISH"): "MEDIUM BULLISH",
            ("MILD BULLISH", "STRONG BULLISH"): "MEDIUM BULLISH",
            ("MILD BULLISH", "MEDIUM BULLISH"): "MILD BULLISH",
            ("MILD BULLISH", "MILD BULLISH"): "MILD BULLISH",
            ("MILD BULLISH", "STRONG BEARISH"): "MEDIUM BEARISH",
            ("MILD BULLISH", "MEDIUM BEARISH"): "INDECISIVE",
            ("MILD BULLISH", "MILD BEARISH"): "INDECISIVE",
            ("STRONG BEARISH", "STRONG BULLISH"): "INDECISIVE",
            ("STRONG BEARISH", "MEDIUM BULLISH"): "INDECISIVE",
            ("STRONG BEARISH", "MILD BULLISH"): "STRONG BEARISH",
            ("STRONG BEARISH", "STRONG BEARISH"): "STRONG BEARISH",
            ("STRONG BEARISH", "MEDIUM BEARISH"): "STRONG BEARISH",
            ("STRONG BEARISH", "MILD BEARISH"): "STRONG BEARISH",
            ("MEDIUM BEARISH", "STRONG BULLISH"): "INDECISIVE",
            ("MEDIUM BEARISH", "MEDIUM BULLISH"): "INDECISIVE",
            ("MEDIUM BEARISH", "MILD BULLISH"): "MEDIUM BEARISH",
            ("MEDIUM BEARISH", "STRONG BEARISH"): "STRONG BEARISH",
            ("MEDIUM BEARISH", "MEDIUM BEARISH"): "MEDIUM BEARISH",
            ("MEDIUM BEARISH", "MILD BEARISH"): "MEDIUM BEARISH",
            ("MILD BEARISH", "STRONG BULLISH"): "MEDIUM BULLISH",
            ("MILD BEARISH", "MEDIUM BULLISH"): "INDECISIVE",
            ("MILD BEARISH", "MILD BULLISH"): "INDECISIVE",
            ("MILD BEARISH", "STRONG BEARISH"): "MEDIUM BEARISH",
            ("MILD BEARISH", "MEDIUM BEARISH"): "MILD BEARISH",
            ("MILD BEARISH", "MILD BEARISH"): "MILD BEARISH",
        }
        
        return truth_table.get((oi_combined, change_combined), "INDECISIVE")
    
    def calculate_combined_percentiles(self, historical_data):
        """Calculate combined percentiles for all participants together"""
        combined_data = historical_data[historical_data['institution'].isin(['FII', 'PRO', 'CLIENT'])]
        percentiles = {}
        
        # Index Futures - separate calculation
        if 'FUTURE_INDEX' in combined_data.columns:
            oi_values = combined_data['FUTURE_INDEX'].dropna()
            change_values = combined_data['FUTURE_INDEX_CHANGE'].dropna()
            
            if len(oi_values) > 0 and len(change_values) > 0:
                oi_sorted = sorted(np.abs(oi_values).tolist())
                change_sorted = sorted(np.abs(change_values).tolist())
                
                percentiles['Index Futures'] = {
                    'oi_p80': self.get_threshold(80, oi_sorted),
                    'oi_p40': self.get_threshold(40, oi_sorted),
                    'change_p80': self.get_threshold(80, change_sorted),
                    'change_p20': self.get_threshold(20, change_sorted)
                }
        
        # Combined Options - Call and Put together
        if 'CALL_OI' in combined_data.columns and 'PUT_OI' in combined_data.columns:
            call_oi = combined_data['CALL_OI'].dropna()
            put_oi = combined_data['PUT_OI'].dropna()
            combined_oi = pd.concat([call_oi, put_oi])
            
            call_change = combined_data['CALL_OI_CHANGE'].dropna()
            put_change = combined_data['PUT_OI_CHANGE'].dropna()
            combined_change = pd.concat([call_change, put_change])
            
            if len(combined_oi) > 0 and len(combined_change) > 0:
                oi_sorted = sorted(np.abs(combined_oi).tolist())
                change_sorted = sorted(np.abs(combined_change).tolist())
                
                percentiles['Combined Options'] = {
                    'oi_p80': self.get_threshold(80, oi_sorted),
                    'oi_p40': self.get_threshold(40, oi_sorted),
                    'change_p80': self.get_threshold(80, change_sorted),
                    'change_p20': self.get_threshold(20, change_sorted)
                }
        
        return percentiles
    
    def calculate_strength_for_date(self, target_date, combined_percentiles):
        """Calculate strength for specific date using fixed percentiles"""
        target_date = pd.to_datetime(target_date)

        # Net OI strength mapping combining call and put strengths
        net_oi_mapping = {
            (("MEDIUM", "BULLISH"), ("INDECISIVE", "INDECISIVE")): ("MEDIUM", "BULLISH"),
            (("MEDIUM", "BULLISH"), ("MEDIUM", "BEARISH")): ("INDECISIVE", "VOLATILE"),
            (("MEDIUM", "BULLISH"), ("MEDIUM", "BULLISH")): ("MEDIUM", "BULLISH"),
            (("MEDIUM", "BULLISH"), ("STRONG", "BEARISH")): ("MEDIUM", "BEARISH"),
            (("MEDIUM", "BULLISH"), ("STRONG", "BULLISH")): ("STRONG", "BULLISH"),
            (("MEDIUM", "BULLISH"), ("MILD", "BEARISH")): ("MEDIUM", "BULLISH"),
            (("MEDIUM", "BULLISH"), ("MILD", "BULLISH")): ("MEDIUM", "BULLISH"),
            (("MEDIUM", "BEARISH"), ("INDECISIVE", "INDECISIVE")): ("MEDIUM", "BEARISH"),
            (("MEDIUM", "BEARISH"), ("MEDIUM", "BEARISH")): ("MEDIUM", "BEARISH"),
            (("MEDIUM", "BEARISH"), ("MEDIUM", "BULLISH")): ("INDECISIVE", "NEUTRAL"),
            (("MEDIUM", "BEARISH"), ("STRONG", "BEARISH")): ("STRONG", "BEARISH"),
            (("MEDIUM", "BEARISH"), ("STRONG", "BULLISH")): ("STRONG", "BULLISH"),
            (("MEDIUM", "BEARISH"), ("MILD", "BEARISH")): ("MEDIUM", "BEARISH"),
            (("MEDIUM", "BEARISH"), ("MILD", "BULLISH")): ("MEDIUM", "BEARISH"),
            (("STRONG", "BULLISH"), ("INDECISIVE", "INDECISIVE")): ("STRONG", "BULLISH"),
            (("STRONG", "BULLISH"), ("MEDIUM", "BEARISH")): ("MEDIUM", "BULLISH"),
            (("STRONG", "BULLISH"), ("MEDIUM", "BULLISH")): ("STRONG", "BULLISH"),
            (("STRONG", "BULLISH"), ("STRONG", "BEARISH")): ("INDECISIVE", "VOLATILE"),
            (("STRONG", "BULLISH"), ("STRONG", "BULLISH")): ("STRONG", "BULLISH"),
            (("STRONG", "BULLISH"), ("MILD", "BEARISH")): ("STRONG", "BULLISH"),
            (("STRONG", "BULLISH"), ("MILD", "BULLISH")): ("STRONG", "BULLISH"),
            (("STRONG", "BEARISH"), ("INDECISIVE", "INDECISIVE")): ("STRONG", "BEARISH"),
            (("STRONG", "BEARISH"), ("MEDIUM", "BEARISH")): ("STRONG", "BEARISH"),
            (("STRONG", "BEARISH"), ("MEDIUM", "BULLISH")): ("MEDIUM", "BEARISH"),
            (("STRONG", "BEARISH"), ("STRONG", "BEARISH")): ("STRONG", "BEARISH"),
            (("STRONG", "BEARISH"), ("STRONG", "BULLISH")): ("INDECISIVE", "NEUTRAL"),
            (("STRONG", "BEARISH"), ("MILD", "BEARISH")): ("STRONG", "BEARISH"),
            (("STRONG", "BEARISH"), ("MILD", "BULLISH")): ("STRONG", "BEARISH"),
            (("MILD", "BULLISH"), ("INDECISIVE", "INDECISIVE")): ("INDECISIVE", "INDECISIVE"),
            (("MILD", "BULLISH"), ("MEDIUM", "BEARISH")): ("MILD", "BEARISH"),
            (("MILD", "BULLISH"), ("MEDIUM", "BULLISH")): ("MEDIUM", "BULLISH"),
            (("MILD", "BULLISH"), ("STRONG", "BEARISH")): ("STRONG", "BEARISH"),
            (("MILD", "BULLISH"), ("STRONG", "BULLISH")): ("STRONG", "BULLISH"),
            (("MILD", "BULLISH"), ("MILD", "BEARISH")): ("INDECISIVE", "INDECISIVE"),
            (("MILD", "BULLISH"), ("MILD", "BULLISH")): ("MILD", "BULLISH"),
            (("MILD", "BEARISH"), ("INDECISIVE", "INDECISIVE")): ("INDECISIVE", "INDECISIVE"),
            (("MILD", "BEARISH"), ("MEDIUM", "BEARISH")): ("MEDIUM", "BEARISH"),
            (("MILD", "BEARISH"), ("MEDIUM", "BULLISH")): ("MEDIUM", "BULLISH"),
            (("MILD", "BEARISH"), ("STRONG", "BEARISH")): ("STRONG", "BEARISH"),
            (("MILD", "BEARISH"), ("STRONG", "BULLISH")): ("STRONG", "BULLISH"),
            (("MILD", "BEARISH"), ("MILD", "BEARISH")): ("INDECISIVE", "INDECISIVE"),
            (("MILD", "BEARISH"), ("MILD", "BULLISH")): ("INDECISIVE", "INDECISIVE"),
        }
        
        target_data = self.df[self.df['date'] == target_date]
        results = []
        
        for institution in ['FII', 'PRO', 'CLIENT']:
            inst_data = target_data[target_data['institution'] == institution]
            if inst_data.empty:
                continue
            
            # Define segments
            segments = []
            if 'FUTURE_INDEX' in inst_data.columns and 'Index Futures' in combined_percentiles:
                segments.append(('Index Futures', 'FUTURE_INDEX', 'FUTURE_INDEX_CHANGE', 'Index Futures'))
            
            if 'CALL_OI' in inst_data.columns and 'Combined Options' in combined_percentiles:
                segments.append(('Call Options', 'CALL_OI', 'CALL_OI_CHANGE', 'Combined Options'))
            
            if 'PUT_OI' in inst_data.columns and 'Combined Options' in combined_percentiles:
                segments.append(('Put Options', 'PUT_OI', 'PUT_OI_CHANGE', 'Combined Options'))
            
            for segment_name, oi_col, change_col, percentile_key in segments:
                current_oi = inst_data[oi_col].iloc[0]
                current_change = inst_data[change_col].iloc[0]
                
                p_data = combined_percentiles[percentile_key]
                oi_p80, oi_p40 = p_data['oi_p80'], p_data['oi_p40']
                change_p80, change_p20 = p_data['change_p80'], p_data['change_p20']
                
                oi_strength = self.get_strength_level(current_oi, oi_p80, oi_p40, False)
                change_strength = self.get_strength_level(current_change, change_p80, change_p20, True)
                
                oi_direction = self.get_direction(current_oi, segment_name)
                change_direction = self.get_direction(current_change, segment_name)
                
                final_strength = self.apply_truth_table(oi_strength, oi_direction, change_strength, change_direction)
                
                results.append({
                    'date': target_date,
                    'institution': institution,
                    'segment': segment_name,
                    'net_oi': current_oi,
                    'change': current_change,
                    'final_strength': final_strength
                })
            
            # Calculate Net Options strength using call and put strengths
            call_result = next((r for r in results if r['segment'] == 'Call Options' and r['institution'] == institution), None)
            put_result = next((r for r in results if r['segment'] == 'Put Options' and r['institution'] == institution), None)
            
            if call_result and put_result:
                call_tuple = self.parse_strength(call_result['final_strength'])
                put_tuple = self.parse_strength(put_result['final_strength'])
                
                net_tuple = net_oi_mapping.get((call_tuple, put_tuple), ("INDECISIVE", "INDECISIVE"))
                net_intensity, net_sentiment = net_tuple
                net_final = f"{net_intensity} {net_sentiment}" if net_sentiment != "INDECISIVE" else "INDECISIVE"
                
                net_oi = call_result['net_oi'] - put_result['net_oi']
                net_change = call_result['change'] - put_result['change']
                
                results.append({
                    'date': target_date,
                    'institution': institution,
                    'segment': 'Net Options',
                    'net_oi': net_oi,
                    'change': net_change,
                    'final_strength': net_final
                })
        
        return results
    
    def calculate_last_n_days_strength(self, n_days=3, lookback_days=60):
        """
        Calculate strength for the last n trading days using fixed lookback percentiles
        """
        if self.df is None:
            self.load_data()
        
        # Get the last 60 unique trading dates for fixed percentiles
        all_dates = sorted(self.df['date'].unique(), reverse=True)
        last_60_dates = all_dates[:lookback_days] if len(all_dates) >= lookback_days else all_dates
        historical_data = self.df[self.df['date'].isin(last_60_dates)]
        combined_percentiles = self.calculate_combined_percentiles(historical_data)
        
        # Print percentile calculations once (fixed for last 60 days)
        print(f"\n--- FIXED Percentile Calculations (last {lookback_days} trading days) ---")
        print(f"Date range: {min(last_60_dates).date()} to {max(last_60_dates).date()}")
        print(f"Number of trading days: {len(last_60_dates)}")
        print(f"Total historical rows: {len(historical_data)}")
        
        combined_data_print = historical_data[historical_data['institution'].isin(['FII', 'PRO', 'CLIENT'])]
        
        # Index Futures
        if 'FUTURE_INDEX' in combined_data_print.columns:
            oi_values = combined_data_print['FUTURE_INDEX'].dropna()
            change_values = combined_data_print['FUTURE_INDEX_CHANGE'].dropna()
            if len(oi_values) > 0 and len(change_values) > 0:
                print("\nIndex Futures Percentiles:")
                print(f"  Num OI points: {len(oi_values)}, Num Change points: {len(change_values)}")
                if 'Index Futures' in combined_percentiles:
                    p = combined_percentiles['Index Futures']
                    print(f"  OI p80: {p['oi_p80']:.0f}, p40: {p['oi_p40']:.0f}")
                    print(f"  Change p80: {p['change_p80']:.0f}, p20: {p['change_p20']:.0f}")
        
        # Options: Combined, Call, Put, Net
        if 'CALL_OI' in combined_data_print.columns and 'PUT_OI' in combined_data_print.columns:
            call_oi = combined_data_print['CALL_OI'].dropna()
            put_oi = combined_data_print['PUT_OI'].dropna()
            call_change = combined_data_print['CALL_OI_CHANGE'].dropna()
            put_change = combined_data_print['PUT_OI_CHANGE'].dropna()
            
            # Combined
            if 'Combined Options' in combined_percentiles:
                p_comb = combined_percentiles['Combined Options']
                combined_oi_num = len(call_oi) + len(put_oi)
                combined_change_num = len(call_change) + len(put_change)
                print("\nCombined Options Percentiles:")
                print(f"  Num OI points (Call + Put): {len(call_oi)} + {len(put_oi)} = {combined_oi_num}, Num Change points: {len(call_change)} + {len(put_change)} = {combined_change_num}")
                print(f"  OI p80: {p_comb['oi_p80']:.0f}, p40: {p_comb['oi_p40']:.0f}")
                print(f"  Change p80: {p_comb['change_p80']:.0f}, p20: {p_comb['change_p20']:.0f}")
            
            # Call
            if len(call_oi) > 0 and len(call_change) > 0:
                call_oi_sorted = sorted(np.abs(call_oi.tolist()))
                call_change_sorted = sorted(np.abs(call_change.tolist()))
                call_oi_p80 = self.get_threshold(80, call_oi_sorted)
                call_oi_p40 = self.get_threshold(40, call_oi_sorted)
                call_change_p80 = self.get_threshold(80, call_change_sorted)
                call_change_p20 = self.get_threshold(20, call_change_sorted)
                print("\nCall Options Percentiles:")
                print(f"  Num OI points: {len(call_oi)}, Num Change points: {len(call_change)}")
                print(f"  OI p80: {call_oi_p80:.0f}, p40: {call_oi_p40:.0f}")
                print(f"  Change p80: {call_change_p80:.0f}, p20: {call_change_p20:.0f}")
            
            # Put
            if len(put_oi) > 0 and len(put_change) > 0:
                put_oi_sorted = sorted(np.abs(put_oi.tolist()))
                put_change_sorted = sorted(np.abs(put_change.tolist()))
                put_oi_p80 = self.get_threshold(80, put_oi_sorted)
                put_oi_p40 = self.get_threshold(40, put_oi_sorted)
                put_change_p80 = self.get_threshold(80, put_change_sorted)
                put_change_p20 = self.get_threshold(20, put_change_sorted)
                print("\nPut Options Percentiles:")
                print(f"  Num OI points: {len(put_oi)}, Num Change points: {len(put_change)}")
                print(f"  OI p80: {put_oi_p80:.0f}, p40: {put_oi_p40:.0f}")
                print(f"  Change p80: {put_change_p80:.0f}, p20: {put_change_p20:.0f}")
            
        print("\n" + "="*100)
        print(f"STRENGTH ANALYSIS FOR LAST {n_days} TRADING DAYS (using fixed {lookback_days}-day percentiles)")
        print("="*100)
        
        # Get the last n unique trading dates for strength calculation
        last_n_dates = all_dates[:n_days]
        
        all_results = []
        
        for date in reversed(last_n_dates):  # Show oldest to newest
            print(f"\n{'='*50}")
            print(f"DATE: {date.date()}")
            print(f"{'='*50}")
            
            results = self.calculate_strength_for_date(date, combined_percentiles)
            all_results.extend(results)
            
            # Display results for this date
            for institution in ['FII', 'PRO', 'CLIENT']:
                inst_results = [r for r in results if r['institution'] == institution]
                if inst_results:
                    print(f"\n{institution}:")
                    print("-" * 80)
                    for r in inst_results:
                        print(f"  {r['segment']:<15} | {r['final_strength']:<15} | Net OI: {r['net_oi']:>10,.0f} | Change: {r['change']:>10,.0f}")
        
        return all_results

# Main execution
if __name__ == "__main__":
    # Initialize calculator
    calculator = FIIStrengthCalculator()
    
    # Load data from DB (will prompt for credentials)
    df = calculator.load_data()
    
    # Calculate strength for last 3 days
    results = calculator.calculate_last_n_days_strength(3)
    
    print(f"\nAnalysis complete! Found {len(results)} strength calculations across 3 days.")
