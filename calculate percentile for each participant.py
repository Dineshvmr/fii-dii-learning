import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine

class FIIStrengthCalculatorRespective:
    """
    FII strength calculation with respective call and put percentiles
    """
    
    def __init__(self):
        self.df = None
        self.nifty_df = None
        self.engine = None
        
    def load_data(self):
        """Load and prepare data from local SQLite database"""
        # SQLite connection string
        db_path = "/Users/dinesh/Documents/SQLite/SQlite"
        engine_str = f"sqlite:///{db_path}"
        engine = create_engine(engine_str)
        
        # SQL query to fetch raw data - convert traded_day (text) to date
        query = """
        SELECT 
            date(traded_day/1000, 'unixepoch','localtime') as date,
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
            date(traded_day/1000, 'unixepoch','localtime'), 
            trading_institution, 
            trade_type
        ORDER BY 
            date(traded_day/1000, 'unixepoch','localtime'), 
            trading_institution, 
            trade_type;
        """
        
        # Load data using pandas with SQLAlchemy
        df_raw = pd.read_sql(query, engine)
        df_raw['date'] = pd.to_datetime(df_raw['date'])
        # No dispose needed for SQLite
        
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
    
    def create_engine(self):
        """Create and return SQLAlchemy engine"""
        db_path = "/Users/dinesh/Documents/SQLite/SQlite"
        engine_str = f"sqlite:///{db_path}"
        return create_engine(engine_str)
    
    def load_nifty_data(self):
        """Load NIFTY closing prices from database and compute changes"""
        if self.engine is None:
            self.engine = self.create_engine()
        
        query = "SELECT trading_date as date, close FROM nse_data ORDER BY date"
        self.nifty_df = pd.read_sql(query, self.engine)
        self.nifty_df['date'] = pd.to_datetime(self.nifty_df['date'])
        self.nifty_df = self.nifty_df.sort_values('date').reset_index(drop=True)
        self.nifty_df['nifty_change_pct'] = self.nifty_df['close'].pct_change() * 100
        # Fill first change as 0 if needed
        self.nifty_df['nifty_change_pct'] = self.nifty_df['nifty_change_pct'].fillna(0)
    
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
    
    def calculate_respective_percentiles(self, historical_data, institution):
        """Calculate respective percentiles for a specific institution"""
        inst_data = historical_data[historical_data['institution'] == institution]
        percentiles = {}
        
        # Index Futures - separate calculation
        if 'FUTURE_INDEX' in inst_data.columns:
            oi_values = inst_data['FUTURE_INDEX'].dropna()
            change_values = inst_data['FUTURE_INDEX_CHANGE'].dropna()
            
            if len(oi_values) > 0 and len(change_values) > 0:
                oi_sorted = sorted(np.abs(oi_values).tolist())
                change_sorted = sorted(np.abs(change_values).tolist())
                
                percentiles['Index Futures'] = {
                    'oi_p80': self.get_threshold(80, oi_sorted),
                    'oi_p40': self.get_threshold(40, oi_sorted),
                    'change_p80': self.get_threshold(80, change_sorted),
                    'change_p20': self.get_threshold(20, change_sorted)
                }
        
        # Call Options - separate calculation
        if 'CALL_OI' in inst_data.columns:
            call_oi = inst_data['CALL_OI'].dropna()
            call_change = inst_data['CALL_OI_CHANGE'].dropna()
            
            if len(call_oi) > 0 and len(call_change) > 0:
                oi_sorted = sorted(np.abs(call_oi).tolist())
                change_sorted = sorted(np.abs(call_change).tolist())
                
                percentiles['Call Options'] = {
                    'oi_p80': self.get_threshold(80, oi_sorted),
                    'oi_p40': self.get_threshold(40, oi_sorted),
                    'change_p80': self.get_threshold(80, change_sorted),
                    'change_p20': self.get_threshold(20, change_sorted)
                }
        
        # Put Options - separate calculation
        if 'PUT_OI' in inst_data.columns:
            put_oi = inst_data['PUT_OI'].dropna()
            put_change = inst_data['PUT_OI_CHANGE'].dropna()
            
            if len(put_oi) > 0 and len(put_change) > 0:
                oi_sorted = sorted(np.abs(put_oi).tolist())
                change_sorted = sorted(np.abs(put_change).tolist())
                
                percentiles['Put Options'] = {
                    'oi_p80': self.get_threshold(80, oi_sorted),
                    'oi_p40': self.get_threshold(40, oi_sorted),
                    'change_p80': self.get_threshold(80, change_sorted),
                    'change_p20': self.get_threshold(20, change_sorted)
                }
        
        return percentiles
    
    def calculate_strength_for_date(self, target_date, institution_percentiles):
        """Calculate strength for specific date"""
        target_date = pd.to_datetime(target_date)

        # Net OI strength mapping combining call and put strengths
        net_oi_mapping = {
            (("MEDIUM", "BULLISH"), ("INDECISIVE", "INDECISIVE")): ("MEDIUM", "BULLISH"),
            (("MEDIUM", "BULLISH"), ("MEDIUM", "BEARISH")): ("VOLATILE", "VOLATILE"),
            (("MEDIUM", "BULLISH"), ("MEDIUM", "BULLISH")): ("MEDIUM", "BULLISH"),
            (("MEDIUM", "BULLISH"), ("STRONG", "BEARISH")): ("MEDIUM", "BEARISH"),
            (("MEDIUM", "BULLISH"), ("STRONG", "BULLISH")): ("STRONG", "BULLISH"),
            (("MEDIUM", "BULLISH"), ("MILD", "BEARISH")): ("MEDIUM", "BULLISH"),
            (("MEDIUM", "BULLISH"), ("MILD", "BULLISH")): ("MEDIUM", "BULLISH"),
            (("MEDIUM", "BEARISH"), ("INDECISIVE", "INDECISIVE")): ("MEDIUM", "BEARISH"),
            (("MEDIUM", "BEARISH"), ("MEDIUM", "BEARISH")): ("MEDIUM", "BEARISH"),
            (("MEDIUM", "BEARISH"), ("MEDIUM", "BULLISH")): ("NEUTRAL", "NEUTRAL"),
            (("MEDIUM", "BEARISH"), ("STRONG", "BEARISH")): ("STRONG", "BEARISH"),
            (("MEDIUM", "BEARISH"), ("STRONG", "BULLISH")): ("STRONG", "BULLISH"),
            (("MEDIUM", "BEARISH"), ("MILD", "BEARISH")): ("MEDIUM", "BEARISH"),
            (("MEDIUM", "BEARISH"), ("MILD", "BULLISH")): ("MEDIUM", "BEARISH"),
            (("STRONG", "BULLISH"), ("INDECISIVE", "INDECISIVE")): ("STRONG", "BULLISH"),
            (("STRONG", "BULLISH"), ("MEDIUM", "BEARISH")): ("MEDIUM", "BULLISH"),
            (("STRONG", "BULLISH"), ("MEDIUM", "BULLISH")): ("STRONG", "BULLISH"),
            (("STRONG", "BULLISH"), ("STRONG", "BEARISH")): ("VOLATILE", "VOLATILE"),
            (("STRONG", "BULLISH"), ("STRONG", "BULLISH")): ("STRONG", "BULLISH"),
            (("STRONG", "BULLISH"), ("MILD", "BEARISH")): ("STRONG", "BULLISH"),
            (("STRONG", "BULLISH"), ("MILD", "BULLISH")): ("STRONG", "BULLISH"),
            (("STRONG", "BEARISH"), ("INDECISIVE", "INDECISIVE")): ("STRONG", "BEARISH"),
            (("STRONG", "BEARISH"), ("MEDIUM", "BEARISH")): ("STRONG", "BEARISH"),
            (("STRONG", "BEARISH"), ("MEDIUM", "BULLISH")): ("MEDIUM", "BEARISH"),
            (("STRONG", "BEARISH"), ("STRONG", "BEARISH")): ("STRONG", "BEARISH"),
            (("STRONG", "BEARISH"), ("STRONG", "BULLISH")): ("NEUTRAL", "NEUTRAL"),
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
            (("INDECISIVE", "INDECISIVE"), ("INDECISIVE", "INDECISIVE")): ("INDECISIVE", "INDECISIVE"),
            (("INDECISIVE", "INDECISIVE"), ("MEDIUM", "BEARISH")): ("MEDIUM", "BEARISH"),
            (("INDECISIVE", "INDECISIVE"), ("MEDIUM", "BULLISH")): ("MEDIUM", "BULLISH"),
            (("INDECISIVE", "INDECISIVE"), ("MILD", "BEARISH")): ("INDECISIVE", "INDECISIVE"),
            (("INDECISIVE", "INDECISIVE"), ("MILD", "BULLISH")): ("INDECISIVE", "INDECISIVE"),
            (("INDECISIVE", "INDECISIVE"), ("STRONG", "BEARISH")): ("STRONG", "BEARISH"),
            (("INDECISIVE", "INDECISIVE"), ("STRONG", "BULLISH")): ("STRONG", "BULLISH"),
        }
        
        target_data = self.df[self.df['date'] == target_date]
        results = []
        
        for institution in ['FII', 'PRO', 'CLIENT']:
            if institution not in institution_percentiles:
                continue
            inst_percentiles = institution_percentiles[institution]
            inst_data = target_data[target_data['institution'] == institution]
            if inst_data.empty:
                continue
            
            # Define segments
            segments = []
            if 'FUTURE_INDEX' in inst_data.columns and 'Index Futures' in inst_percentiles:
                segments.append(('Index Futures', 'FUTURE_INDEX', 'FUTURE_INDEX_CHANGE', 'Index Futures'))
            
            if 'CALL_OI' in inst_data.columns and 'Call Options' in inst_percentiles:
                segments.append(('Call Options', 'CALL_OI', 'CALL_OI_CHANGE', 'Call Options'))
            
            if 'PUT_OI' in inst_data.columns and 'Put Options' in inst_percentiles:
                segments.append(('Put Options', 'PUT_OI', 'PUT_OI_CHANGE', 'Put Options'))
            
            for segment_name, oi_col, change_col, percentile_key in segments:
                current_oi = inst_data[oi_col].iloc[0]
                current_change = inst_data[change_col].iloc[0]
                
                p_data = institution_percentiles[institution][percentile_key]
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
    
    def calculate_last_n_days_strength(self, n_days=1, lookback_days=60):
        """
        Calculate strength for the last n trading days using fixed lookback percentiles
        """
        if self.df is None:
            self.load_data()
        if self.nifty_df is None:
            self.load_nifty_data()
        
        # Get the last 60 unique trading dates for fixed percentiles
        all_dates = sorted(self.df['date'].unique(), reverse=True)
        last_60_dates = all_dates[:lookback_days] if len(all_dates) >= lookback_days else all_dates
        historical_data = self.df[self.df['date'].isin(last_60_dates)]
        
        # Compute percentiles per institution
        institution_percentiles = {}
        for inst in ['FII', 'PRO', 'CLIENT']:
            inst_hist = historical_data[historical_data['institution'] == inst].copy()
            if not inst_hist.empty:
                institution_percentiles[inst] = self.calculate_respective_percentiles(inst_hist, inst)
        
        # Print percentile calculations once (fixed for last 60 days)
        print(f"\n--- FIXED Percentile Calculations (last {lookback_days} trading days) ---")
        print(f"Date range: {min(last_60_dates).date()} to {max(last_60_dates).date()}")
        print(f"Number of trading days: {len(last_60_dates)}")
        print(f"Total historical rows: {len(historical_data)}")
        
        for inst in ['FII', 'PRO', 'CLIENT']:
            if inst not in institution_percentiles:
                continue
            inst_percentiles = institution_percentiles[inst]
            inst_data_print = historical_data[historical_data['institution'] == inst]
            
            print(f"\n{inst} Percentiles:")
            
            # Index Futures
            if 'FUTURE_INDEX' in inst_data_print.columns and 'Index Futures' in inst_percentiles:
                oi_values = inst_data_print['FUTURE_INDEX'].dropna()
                change_values = inst_data_print['FUTURE_INDEX_CHANGE'].dropna()
                if len(oi_values) > 0 and len(change_values) > 0:
                    print("\n  Index Futures:")
                    print(f"    Num OI points: {len(oi_values)}, Num Change points: {len(change_values)}")
                    p = inst_percentiles['Index Futures']
                    print(f"    OI p80: {p['oi_p80']:.0f}, p40: {p['oi_p40']:.0f}")
                    print(f"    Change p80: {p['change_p80']:.0f}, p20: {p['change_p20']:.0f}")
            
            # Call Options
            if 'CALL_OI' in inst_data_print.columns and 'Call Options' in inst_percentiles:
                call_oi = inst_data_print['CALL_OI'].dropna()
                call_change = inst_data_print['CALL_OI_CHANGE'].dropna()
                if len(call_oi) > 0 and len(call_change) > 0:
                    p_call = inst_percentiles['Call Options']
                    print("\n  Call Options:")
                    print(f"    Num OI points: {len(call_oi)}, Num Change points: {len(call_change)}")
                    print(f"    OI p80: {p_call['oi_p80']:.0f}, p40: {p_call['oi_p40']:.0f}")
                    print(f"    Change p80: {p_call['change_p80']:.0f}, p20: {p_call['change_p20']:.0f}")
            
            # Put Options
            if 'PUT_OI' in inst_data_print.columns and 'Put Options' in inst_percentiles:
                put_oi = inst_data_print['PUT_OI'].dropna()
                put_change = inst_data_print['PUT_OI_CHANGE'].dropna()
                if len(put_oi) > 0 and len(put_change) > 0:
                    p_put = inst_percentiles['Put Options']
                    print("\n  Put Options:")
                    print(f"    Num OI points: {len(put_oi)}, Num Change points: {len(put_change)}")
                    print(f"    OI p80: {p_put['oi_p80']:.0f}, p40: {p_put['oi_p40']:.0f}")
                    print(f"    Change p80: {p_put['change_p80']:.0f}, p20: {p_put['change_p20']:.0f}")
        
        print("\n" + "="*100)
        print(f"STRENGTH ANALYSIS FOR LAST {n_days} TRADING DAY (using fixed {lookback_days}-day percentiles)")
        print("="*100)
        
        # Print the header for the CSV format
        print("Date, Participant, Segment, Strength, Net OI, OI Change")
        
        # Get the last n unique trading dates for strength calculation
        last_n_dates = all_dates[:n_days]
        
        all_results = []
        segments_list = ['Index Futures', 'Call Options', 'Put Options', 'Net Options']
        
        for date in reversed(last_n_dates):  # Show oldest to newest
            results = self.calculate_strength_for_date(date, institution_percentiles)
            all_results.extend(results)
            
            # Display results for this date in CSV format
            for institution in ['FII', 'PRO', 'CLIENT']:
                inst_results = {r['segment']: r for r in results if r['institution'] == institution}
                
                for segment in segments_list:
                    segment_data = inst_results.get(segment, {})
                    if segment_data:
                        strength = segment_data.get('final_strength', 'INDECISIVE')
                        net_oi = segment_data.get('net_oi', 0)
                        oi_change = segment_data.get('change', 0)
                        print(f"{date.date()},{institution},{segment},{strength},{net_oi},{oi_change}")
        
        return all_results

    def calculate_accuracy_summary(self, lookback_days=60):
        """
        Calculate prediction accuracy summary for last lookback_days
        """
        if self.df is None:
            self.load_data()
        if self.nifty_df is None:
            self.load_nifty_data()
        
        all_dates = sorted(self.df['date'].unique(), reverse=True)
        analysis_dates = all_dates[1:lookback_days]  # Exclude the latest date, take next 59 for analysis
        last_3_dates = all_dates[:3]
        historical_data = self.df[self.df['date'].isin(all_dates[:lookback_days])]
        
        # Compute percentiles per institution for accuracy
        institution_percentiles_acc = {}
        for inst in ['FII', 'PRO', 'CLIENT']:
            inst_hist = historical_data[historical_data['institution'] == inst].copy()
            if not inst_hist.empty:
                institution_percentiles_acc[inst] = self.calculate_respective_percentiles(inst_hist, inst)
        
        # Expectation table mapping
        expectations = {
            ("INDECISIVE", "MEDIUM BEARISH"): {"CLIENT": "FLAT OR UP", "OTHER": "DOWN"},
            ("INDECISIVE", "MEDIUM BULLISH"): {"CLIENT": "FLAT OR DOWN", "OTHER": "FLAT OR UP"},
            ("INDECISIVE", "MILD BEARISH"): {"CLIENT": "", "OTHER": ""},
            ("INDECISIVE", "MILD BULLISH"): {"CLIENT": "", "OTHER": ""},
            ("INDECISIVE", "STRONG BEARISH"): {"CLIENT": "FLAT OR UP", "OTHER": "DOWN"},
            ("INDECISIVE", "STRONG BULLISH"): {"CLIENT": "DOWN", "OTHER": "FLAT OR UP"},
            ("MEDIUM BEARISH", "INDECISIVE"): {"CLIENT": "FLAT OR UP", "OTHER": "DOWN"},
            ("MEDIUM BEARISH", "MEDIUM BEARISH"): {"CLIENT": "FLAT OR UP", "OTHER": "DOWN"},
            ("MEDIUM BEARISH", "MEDIUM BULLISH"): {"CLIENT": "VOLATILE", "OTHER": "NEUTRAL"},
            ("MEDIUM BEARISH", "MILD BEARISH"): {"CLIENT": "FLAT OR UP", "OTHER": "FLAT OR DOWN"},
            ("MEDIUM BEARISH", "MILD BULLISH"): {"CLIENT": "FLAT OR UP", "OTHER": "FLAT OR DOWN"},
            ("MEDIUM BEARISH", "STRONG BEARISH"): {"CLIENT": "FLAT OR UP", "OTHER": "DOWN"},
            ("MEDIUM BEARISH", "STRONG BULLISH"): {"CLIENT": "DOWN", "OTHER": "FLAT OR UP"},
            ("MEDIUM BULLISH", "INDECISIVE"): {"CLIENT": "FLAT OR DOWN", "OTHER": "UP"},
            ("MEDIUM BULLISH", "MEDIUM BEARISH"): {"CLIENT": "FLAT", "OTHER": "NEUTRAL"},
            ("MEDIUM BULLISH", "MEDIUM BULLISH"): {"CLIENT": "FLAT OR DOWN", "OTHER": "UP"},
            ("MEDIUM BULLISH", "MILD BEARISH"): {"CLIENT": "FLAT OR DOWN", "OTHER": "UP"},
            ("MEDIUM BULLISH", "MILD BULLISH"): {"CLIENT": "FLAT OR DOWN", "OTHER": "UP"},
            ("MEDIUM BULLISH", "STRONG BEARISH"): {"CLIENT": "FLAT OR UP", "OTHER": "DOWN"},
            ("MEDIUM BULLISH", "STRONG BULLISH"): {"CLIENT": "DOWN", "OTHER": "FLAT OR UP"},
            ("MILD BEARISH", "INDECISIVE"): {"CLIENT": "", "OTHER": ""},
            ("MILD BEARISH", "MEDIUM BEARISH"): {"CLIENT": "FLAT OR UP", "OTHER": "FLAT OR DOWN"},
            ("MILD BEARISH", "MEDIUM BULLISH"): {"CLIENT": "FLAT OR DOWN", "OTHER": "FLAT OR UP"},
            ("MILD BEARISH", "MILD BEARISH"): {"CLIENT": "", "OTHER": ""},
            ("MILD BEARISH", "MILD BULLISH"): {"CLIENT": "", "OTHER": ""},
            ("MILD BEARISH", "STRONG BEARISH"): {"CLIENT": "FLAT OR UP", "OTHER": "DOWN"},
            ("MILD BEARISH", "STRONG BULLISH"): {"CLIENT": "DOWN", "OTHER": "FLAT OR UP"},
            ("MILD BULLISH", "INDECISIVE"): {"CLIENT": "", "OTHER": ""},
            ("MILD BULLISH", "MEDIUM BEARISH"): {"CLIENT": "FLAT OR UP", "OTHER": "DOWN"},
            ("MILD BULLISH", "MEDIUM BULLISH"): {"CLIENT": "FLAT OR DOWN", "OTHER": "FLAT OR UP"},
            ("MILD BULLISH", "MILD BEARISH"): {"CLIENT": "", "OTHER": ""},
            ("MILD BULLISH", "MILD BULLISH"): {"CLIENT": "FLAT OR DOWN", "OTHER": "UP"},
            ("MILD BULLISH", "STRONG BEARISH"): {"CLIENT": "FLAT OR UP", "OTHER": "DOWN"},
            ("MILD BULLISH", "STRONG BULLISH"): {"CLIENT": "DOWN", "OTHER": "FLAT OR UP"},
            ("STRONG BEARISH", "INDECISIVE"): {"CLIENT": "FLAT OR UP", "OTHER": "FLAT OR DOWN"},
            ("STRONG BEARISH", "MEDIUM BEARISH"): {"CLIENT": "UP", "OTHER": "FLAT OR DOWN"},
            ("STRONG BEARISH", "MEDIUM BULLISH"): {"CLIENT": "FLAT OR UP", "OTHER": "FLAT OR DOWN"},
            ("STRONG BEARISH", "MILD BEARISH"): {"CLIENT": "UP", "OTHER": "FLAT OR DOWN"},
            ("STRONG BEARISH", "MILD BULLISH"): {"CLIENT": "UP", "OTHER": "FLAT OR DOWN"},
            ("STRONG BEARISH", "STRONG BEARISH"): {"CLIENT": "UP", "OTHER": "DOWN"},
            ("STRONG BEARISH", "STRONG BULLISH"): {"CLIENT": "VOLATILE", "OTHER": "NEUTRAL"},
            ("STRONG BULLISH", "INDECISIVE"): {"CLIENT": "FLAT OR DOWN", "OTHER": "UP"},
            ("STRONG BULLISH", "MEDIUM BEARISH"): {"CLIENT": "FLAT OR DOWN", "OTHER": "UP"},
            ("STRONG BULLISH", "MEDIUM BULLISH"): {"CLIENT": "FLAT OR DOWN", "OTHER": "UP"},
            ("STRONG BULLISH", "MILD BEARISH"): {"CLIENT": "FLAT OR DOWN", "OTHER": "UP"},
            ("STRONG BULLISH", "MILD BULLISH"): {"CLIENT": "FLAT OR DOWN", "OTHER": "UP"},
            ("STRONG BULLISH", "STRONG BEARISH"): {"CLIENT": "FLAT", "OTHER": "VOLATILE"},
            ("STRONG BULLISH", "STRONG BULLISH"): {"CLIENT": "DOWN", "OTHER": "UP"},
            ("INDECISIVE", "INDECISIVE"): {"CLIENT": "", "OTHER": ""},
        }
        
        # Prepare NIFTY lookup
        date_to_nifty = dict(zip(self.nifty_df['date'], self.nifty_df['close']))
        
        # Sorted FII dates
        sorted_trading_dates = sorted(all_dates)
        date_to_idx = {date: idx for idx, date in enumerate(sorted_trading_dates)}
        
        # Counts for each institution
        institutions = ['CLIENT', 'PRO', 'FII']
        
        thresholds = [0.2, 0.3, 0.4]
        for actual_threshold in thresholds:
            print(f"\n--- Accuracy with threshold {actual_threshold}% ---")
            
            counts = {inst: {'correct': 0, 'wrong': 0, 'indecisive': 0} for inst in institutions}
            
            last_3_dates = all_dates[:3]
            
            for date in reversed(analysis_dates):
                # Skip if no next day or no strength data
                date_idx = date_to_idx.get(date)
                if date_idx is None or date_idx + 1 >= len(sorted_trading_dates):
                    continue
                
                next_date = sorted_trading_dates[date_idx + 1]
                next_nifty = date_to_nifty.get(next_date)
                if next_nifty is None:
                    continue
                
                current_nifty = date_to_nifty.get(date)
                if current_nifty is None:
                    continue
                
                next_change_pct = ((next_nifty - current_nifty) / current_nifty) * 100
                
                results = self.calculate_strength_for_date(date, institution_percentiles_acc)
                
                for institution in institutions:
                    inst_results = {r['segment']: r for r in results if r['institution'] == institution}
                    call_result = inst_results.get('Call Options')
                    put_result = inst_results.get('Put Options')
                    
                    if not call_result or not put_result:
                        continue
                    
                    call_strength = call_result['final_strength']
                    put_strength = put_result['final_strength']
                    
                    key = (call_strength, put_strength)
                    exp = expectations.get(key, {'CLIENT': '', 'OTHER': ''})
                    
                    client_exp = exp.get('CLIENT', '')
                    other_exp = exp.get('OTHER', '')
                    
                    # Determine for CLIENT
                    is_client = institution == 'CLIENT'
                    exp_str = client_exp if is_client else other_exp
                    if not exp_str:
                        counts[institution]['indecisive'] += 1
                        continue
                    
                    if date in last_3_dates:
                        print(f"Debug for {date.date()}, {institution}: call='{call_strength}', put='{put_strength}', key={key}, exp_str='{exp_str}'")
                    
                    is_up = next_change_pct > 0
                    is_down = next_change_pct < 0
                    is_flat = abs(next_change_pct) <= actual_threshold
                    is_volatile = abs(next_change_pct) > actual_threshold
                    
                    correct = False
                    if exp_str == "UP":
                        correct = is_up
                    elif exp_str == "DOWN":
                        correct = is_down
                    elif exp_str in ["FLAT", "NEUTRAL"]:
                        correct = is_flat
                    elif exp_str == "FLAT OR UP":
                        correct = next_change_pct >= -actual_threshold
                    elif exp_str == "FLAT OR DOWN":
                        correct = next_change_pct <= actual_threshold
                    elif exp_str == "VOLATILE":
                        correct = is_volatile
                    else:
                        counts[institution]['indecisive'] += 1
                        continue
                    
                    if correct:
                        counts[institution]['correct'] += 1
                    else:
                        counts[institution]['wrong'] += 1
            
            print("\tCorrect\tWrong\tIndecisive\tCorrect %")
            for inst in ['CLIENT', 'PRO', 'FII']:
                c, w, i = counts[inst]['correct'], counts[inst]['wrong'], counts[inst]['indecisive']
                total_dec = c + w
                pct = (c / total_dec * 100) if total_dec > 0 else 0
                print(f"{inst}\t{c}\t{w}\t{i}\t{pct:.2f}%")
        
        # Print number of analyzed days
        print(f"\nAnalyzed {len(analysis_dates)} days")

# Main execution
if __name__ == "__main__":
    # Initialize calculator
    calculator = FIIStrengthCalculatorRespective()
    
    # Load data from DB (will prompt for credentials)
    df = calculator.load_data()
    print(f"Loaded {len(df)} rows from database.")
    
    # Calculate strength for last 1 day (latest available)
    results = calculator.calculate_last_n_days_strength(1)
    
    # Calculate accuracy summary for last 60 days
    calculator.calculate_accuracy_summary(60)
    
    print(f"\nAnalysis complete! Found {len(results)} strength calculations across 1 day.")
