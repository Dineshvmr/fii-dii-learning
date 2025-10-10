import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class FIIStrengthCalculator:
    """
    Implement the exact FII strength calculation logic
    """
    
    def __init__(self, data_file):
        self.data_file = data_file
        self.df = None
        self.strength_results = []
        
    def load_data(self):
        """Load and prepare data"""
        print("Loading data...")
        self.df = pd.read_csv(self.data_file)
        self.df['date'] = pd.to_datetime(self.df['date'])
        self.df = self.df.sort_values(['date', 'institution'])
        
        # Filter out DII data as per requirements
        self.df = self.df[self.df['institution'].isin(['FII', 'PRO', 'CLIENT'])].copy()
        
        print(f"Data loaded: {self.df.shape}")
        print(f"Institutions: {list(self.df['institution'].unique())}")
        print(f"Date range: {self.df['date'].min()} to {self.df['date'].max()}")
        
        return self.df
    
    def get_threshold(self, percentile, sorted_list):
        """Custom percentile calculation to match platform logic"""
        t_pos = (percentile / 100) * (len(sorted_list) - 1)
        t_pos_int, t_pos_frac = divmod(t_pos, 1)
        t_pos_int = int(t_pos_int)
        if t_pos_frac == 0:
            return sorted_list[t_pos_int]
        else:
            return sorted_list[t_pos_int] + (
                t_pos_frac * (sorted_list[t_pos_int + 1] - sorted_list[t_pos_int])
            )
    
    def get_strength_level(self, value, percentile_80, percentile_40_20, is_change=False):
        """
        Determine strength level based on percentile thresholds
        """
        abs_value = abs(value)
        
        if is_change:  # Change uses 20% as lower threshold
            if abs_value >= percentile_80:
                return "STRONG"
            elif abs_value >= percentile_40_20:
                return "MEDIUM"
            else:
                return "MILD"
        else:  # Outstanding OI uses 40% as lower threshold
            if abs_value >= percentile_80:
                return "STRONG"
            elif abs_value >= percentile_40_20:
                return "MEDIUM"
            else:
                return "MILD"
    
    def get_direction(self, value, segment_type):
        """
        Determine bullish/bearish direction based on value and segment
        """
        if segment_type in ['FUTURE_INDEX', 'FUTURE-INDEX', 'INDEX_FUTURES']:
            return "BULLISH" if value > 0 else "BEARISH"
        elif segment_type in ['CALL', 'CALL_OPTIONS','CALL_OI','Call Options']:
            return "BULLISH" if value > 0 else "BEARISH"
        elif segment_type in ['PUT', 'PUT_OPTIONS','PUT_OI','Put Options']:
            return "BEARISH" if value > 0 else "BULLISH"  # Opposite for puts
        else:
            return "BULLISH" if value > 0 else "BEARISH"
    
    def apply_truth_table(self, oi_strength, oi_direction, change_strength, change_direction):
        """
        Apply the truth table logic to determine final strength
        """
        oi_combined = f"{oi_strength} {oi_direction}"
        change_combined = f"{change_strength} {change_direction}"
        
        # Truth table mapping
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
    
    def calculate_combined_options_percentiles(self, historical_data, institution):
        """
        Calculate combined percentiles for Call and Put options together
        """
        hist_inst_data = historical_data[historical_data['institution'] == institution]
        
        if len(hist_inst_data) < 10:
            return None, None, None, None
        
        # Combine Call and Put OI values
        call_oi_values = hist_inst_data['CALL_OI'].dropna()
        put_oi_values = hist_inst_data['PUT_OI'].dropna()
        combined_oi_values = pd.concat([call_oi_values, put_oi_values])
        
        # Combine Call and Put Change values
        call_change_values = hist_inst_data['CALL_OI_CHANGE'].dropna()
        put_change_values = hist_inst_data['PUT_OI_CHANGE'].dropna()
        combined_change_values = pd.concat([call_change_values, put_change_values])
        
        # Calculate percentiles using combined data
        oi_abs_values = np.abs(combined_oi_values)
        oi_sorted = sorted(oi_abs_values.tolist())
        
        change_abs_values = np.abs(combined_change_values)
        change_sorted = sorted(change_abs_values.tolist())
        
        if len(oi_sorted) == 0 or len(change_sorted) == 0:
            return None, None, None, None
        
        oi_p80 = self.get_threshold(80, oi_sorted)
        oi_p40 = self.get_threshold(40, oi_sorted)
        change_p80 = self.get_threshold(80, change_sorted)
        change_p20 = self.get_threshold(20, change_sorted)
        
        return oi_p80, oi_p40, change_p80, change_p20
    
    def calculate_strength_for_date(self, target_date, lookback_days=60):
        """
        Calculate strength for a specific date using 60-day lookback
        """
        print(f"\nCalculating strength for {target_date}...")
        
        # Convert target_date to datetime
        target_date = pd.to_datetime(target_date)
        
        # Get last 60 trading days (actual trading dates, not calendar days)
        # Include current day for analysis
        all_dates_before_target = self.df[self.df['date'] <= target_date]['date'].unique()
        all_dates_before_target = sorted(all_dates_before_target, reverse=True)

        # Take last 60 trading days
        if len(all_dates_before_target) >= lookback_days:
            lookback_dates = all_dates_before_target[:lookback_days]
        else:
            lookback_dates = all_dates_before_target

        historical_data = self.df[self.df['date'].isin(lookback_dates)]
        
        if lookback_dates:
            print(f"Using last {len(lookback_dates)} trading days from {min(lookback_dates).date()} to {max(lookback_dates).date()}")
        else:
            print("No historical trading days available")
        
        print(f"Historical data points: {len(historical_data)}")
        
        # Get target date data
        target_data = self.df[self.df['date'] == target_date]
        
        if target_data.empty:
            print(f"No data available for {target_date}")
            return []
        
        results = []
        
        # Process each institution and segment
        for institution in ['FII', 'PRO', 'CLIENT']:
            inst_data = target_data[target_data['institution'] == institution]
            
            if inst_data.empty:
                continue
                
            print(f"\nProcessing {institution}...")
            
            # Get available columns for analysis
            available_columns = inst_data.columns.tolist()
            
            # Pre-calculate combined options percentiles for this institution
            options_percentiles = None
            if 'CALL_OI' in available_columns and 'PUT_OI' in available_columns:
                options_percentiles = self.calculate_combined_options_percentiles(historical_data, institution)
            
            # Define segments to analyze
            segments_to_analyze = []
            
            # Index Futures
            if 'FUTURE_INDEX' in available_columns:
                segments_to_analyze.append(('Index Futures', 'FUTURE_INDEX', 'FUTURE_INDEX_CHANGE', 'INDIVIDUAL'))
            
            # Call Options - using combined percentiles
            if 'CALL_OI' in available_columns and options_percentiles[0] is not None:
                segments_to_analyze.append(('Call Options', 'CALL_OI', 'CALL_OI_CHANGE', 'COMBINED_OPTIONS'))
            
            # Put Options - using combined percentiles
            if 'PUT_OI' in available_columns and options_percentiles[0] is not None:
                segments_to_analyze.append(('Put Options', 'PUT_OI', 'PUT_OI_CHANGE', 'COMBINED_OPTIONS'))
            
            for segment_name, oi_col, change_col, calculation_type in segments_to_analyze:
                if oi_col not in available_columns or change_col not in available_columns:
                    continue
                    
                # Get current values
                current_oi = inst_data[oi_col].iloc[0]
                current_change = inst_data[change_col].iloc[0]
                
                # Calculate percentiles based on type
                if calculation_type == 'COMBINED_OPTIONS':
                    # Use pre-calculated combined percentiles
                    oi_p80, oi_p40, change_p80, change_p20 = options_percentiles
                else:
                    # Individual calculation for Index Futures
                    hist_inst_data = historical_data[historical_data['institution'] == institution]
                    
                    if len(hist_inst_data) < 10:
                        print(f"  Insufficient historical data for {segment_name}")
                        continue
                    
                    # Calculate percentiles for Outstanding OI using custom method
                    oi_values = hist_inst_data[oi_col].dropna()
                    oi_abs_values = np.abs(oi_values)
                    oi_sorted = sorted(oi_abs_values.tolist())

                    oi_p80 = self.get_threshold(80, oi_sorted)
                    oi_p40 = self.get_threshold(40, oi_sorted)

                    # Calculate percentiles for Change using custom method
                    change_values = hist_inst_data[change_col].dropna()
                    change_abs_values = np.abs(change_values)
                    change_sorted = sorted(change_abs_values.tolist())

                    change_p80 = self.get_threshold(80, change_sorted)
                    change_p20 = self.get_threshold(20, change_sorted)
                
                # Debug for PRO Index Futures
                if institution == 'PRO' and segment_name == 'Index Futures':
                    print(f"  DEBUG for {institution} {segment_name}:")
                    print(f"    Current OI: {current_oi}")
                    print(f"    Current Change: {current_change}")
                    print(f"    OI percentiles - 80th: {oi_p80:.2f}, 40th: {oi_p40:.2f}")
                    print(f"    Change percentiles - 80th: {change_p80:.2f}, 20th: {change_p20:.2f}")
                    print(f"    OI abs value: {abs(current_oi)}, vs 80th: {oi_p80}, vs 40th: {oi_p40}")
                    print(f"    Change abs value: {abs(current_change)}, vs 80th: {change_p80}, vs 20th: {change_p20}")
                
                # Determine strength levels
                oi_strength = self.get_strength_level(current_oi, oi_p80, oi_p40, is_change=False)
                change_strength = self.get_strength_level(current_change, change_p80, change_p20, is_change=True)
                
                # Determine directions
                oi_direction = self.get_direction(current_oi, segment_name)
                change_direction = self.get_direction(current_change, segment_name)
                
                # Apply truth table
                final_strength = self.apply_truth_table(oi_strength, oi_direction, 
                                                      change_strength, change_direction)
                
                # Store result
                result = {
                    'date': target_date,
                    'institution': institution,
                    'segment': segment_name,
                    'net_oi': current_oi,
                    'change': current_change,
                    'oi_strength': f"{oi_strength} {oi_direction}",
                    'change_strength': f"{change_strength} {change_direction}",
                    'final_strength': final_strength,
                    'oi_percentiles': f"80th: {oi_p80:.0f}, 40th: {oi_p40:.0f}",
                    'change_percentiles': f"80th: {change_p80:.0f}, 20th: {change_p20:.0f}"
                }
                
                results.append(result)
                
                # Print result
                print(f"  {segment_name}:")
                print(f"    Net OI: {current_oi:,.0f} -> {oi_strength} {oi_direction}")
                print(f"    Change: {current_change:,.0f} -> {change_strength} {change_direction}")
                print(f"    Final Strength: {final_strength}")
        
        return results
    
    def calculate_latest_day_strength(self):
        """
        Calculate strength for the latest available trading day
        """
        if self.df is None:
            self.load_data()
        
        latest_date = self.df['date'].max()
        print(f"Calculating strength for latest trading day: {latest_date.date()}")
        
        results = self.calculate_strength_for_date(latest_date)
        
        # Display results in a formatted table
        if results:
            print("\n" + "="*100)
            print(f"STRENGTH ANALYSIS FOR {latest_date.date()}")
            print("="*100)
            
            df_results = pd.DataFrame(results)
            
            # Group by institution
            for institution in ['FII', 'PRO', 'CLIENT']:
                inst_results = df_results[df_results['institution'] == institution]
                if not inst_results.empty:
                    print(f"\n{institution}:")
                    print("-" * 80)
                    for _, row in inst_results.iterrows():
                        print(f"  {row['segment']:<15} | {row['final_strength']:<15} | Net OI: {row['net_oi']:>10,.0f} | Change: {row['change']:>10,.0f}")
        
        return results

# Main execution
if __name__ == "__main__":
    # Initialize calculator
    calculator = FIIStrengthCalculator('processed_options_futures.csv')
    
    # Calculate strength for latest day
    results = calculator.calculate_latest_day_strength()
    
    print(f"\nAnalysis complete! Found {len(results)} strength calculations.")