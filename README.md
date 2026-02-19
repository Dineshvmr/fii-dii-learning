# FII/DII Learning & Analysis

[![GitHub Repo](https://img.shields.io/badge/GitHub-Repo-blue?logo=github)](https://github.com/Dineshvmr/fii-dii-learning)

This project provides Python tools for analyzing **FII (Foreign Institutional Investors)** and **DII (Domestic Institutional Investors)** activity in Indian **options and futures** markets. It processes raw trading data to compute net open interest (OI), daily changes, and advanced **strength indicators** using percentile-based thresholds and a custom truth table logic.

## 🚀 Key Features

- **Data Processing**: Filter and pivot FII/DII data for options (CALL/PUT) and futures (INDEX_FUTURES, etc.), excluding CASH segments.
- **Strength Calculation**: Computes **STRONG/MEDIUM/MILD** BULLISH/BEARISH/INDECISIVE signals for FII, PRO, CLIENT across segments using:
  - 60-day lookback percentiles for OI (80th/40th) and changes (80th/20th).
  - Truth table combining OI strength/direction with change strength/direction.
- **Summary Reports**: Daily analysis with formatted output tables.
- **Custom Calculators**: Multiple versions for percentile methods, AI summaries, and combined CALL/PUT analysis.

## 📁 Project Structure

```
FII DII Learning/
├── fii_dii_learning.py              # Main data processor (raw → processed CSV)
├── fii_strength_calculator.py       # Core strength calculator class
├── fii_strength_calculator_3.py     # Enhanced version
├── custom_percentile_method.py      # Alternative percentile logic
├── fii_ai_summary_oct7.py           # AI-powered summaries
├── processed_options_futures.csv    # Sample processed data
├── *.ipynb                          # Jupyter notebooks for exploration
└── .gitignore
```

## 🛠️ Requirements

```bash
pip install pandas numpy
```

## 📊 Usage

### 1. Prepare Data
Place raw FII/DII CSV file (expected format):
```
date,institution,trade_type,net_oi
2024-09-25,FII,FUTURE_INDEX,12345
2024-09-25,FII,CALL,6789
...
```
Update `input_file` path in `fii_dii_learning.py` if needed (default: `fii_dii_data_sep_25.csv`).

### 2. Process Data
```bash
python fii_dii_learning.py
```
- Outputs: `processed_options_futures.csv` with pivoted OI, changes (e.g., `CALL_OI`, `OPTIONS_NET_OI_CHANGE`).

### 3. Calculate Strengths
```bash
python fii_strength_calculator.py
```
- Analyzes latest trading day.
- Outputs formatted strength table, e.g.:
```
FII:
  Index Futures   | STRONG BULLISH | Net OI: 1,234,567 | Change: 89,012
  Call Options    | MEDIUM BEARISH | Net OI: 456,789   | Change: -12,345
```

### 4. Other Tools
- `fii_strength_calculator_call_put_seperate_oct_7.py`: Separate CALL/PUT analysis.
- Notebooks: `Untitled.ipynb` for interactive exploration.

## 🔍 Strength Logic

1. **OI Strength**: Compare abs(OI) to 60-day 80th/40th percentiles.
2. **Change Strength**: Compare abs(Change) to 60-day 80th/20th percentiles.
3. **Direction**: Bullish/Bearish based on segment (PUTs inverted).
4. **Truth Table**: Combines OI + Change for final signal (e.g., STRONG BULLISH OI + MILD BEARISH Change → STRONG BULLISH).

See `fii_strength_calculator.py` for full implementation.

## 📈 Example Output

```
=== STRENGTH ANALYSIS FOR 2024-10-07 ===
FII:
  Index Futures | STRONG BULLISH | Net OI: 2,345,678 | Change: 123,456
PRO:
  Put Options   | MILD BEARISH   | Net OI: 987,654   | Change: -45,678
```

## 🤝 Contributing

1. Fork the repo.
2. Create a feature branch.
3. Commit changes.
4. Push and open a PR.

## 📄 License

MIT License - feel free to use and modify.

## 🙏 Acknowledgments

Built for Indian markets FII/DII analysis enthusiasts.