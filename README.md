# Springer-Capital_DE_Home_Test
# Referral Data Pipeline (Data Engineer Take-Home Test)

## 📌 Overview
This project builds a data pipeline using Python (Pandas) to analyze a referral program and detect invalid/fraudulent rewards.

## ⚙️ Features
- Data loading from multiple CSV sources
- Data profiling (null & distinct counts)
- Data cleaning and transformation
- Complex joins across 7 tables
- Business logic validation for fraud detection
- Final report generation

## 📊 Output
- Generates `final_report.csv`
- Includes validation column: `is_business_logic_valid`

## ▶️ How to Run

```bash
pip install -r requirements.txt
python your_script.py
