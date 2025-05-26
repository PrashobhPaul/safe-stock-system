import openai
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

# Set your OpenAI API key here
openai.api_key = "YOUR_API_KEY"

# Connect to SQLite database
conn = sqlite3.connect("stock_data.db")

# Load recent data (last 2 weeks and 1 year for trend analysis)
query = """
SELECT * FROM stock_prices
WHERE date >= date('now', '-370 days')
"""
df = pd.read_sql(query, conn)

# Preprocess: Get latest close, 1-year and 6-month growth, weekly trends
df['date'] = pd.to_datetime(df['date'])
latest_date = df['date'].max()

result = []
for stock in df['stock'].unique():
    stock_df = df[df['stock'] == stock].sort_values('date')
    if len(stock_df) < 200:
        continue

    try:
        current_price = stock_df[stock_df['date'] == latest_date]['close'].values[0]
        one_year_ago = latest_date - timedelta(days=365)
        six_months_ago = latest_date - timedelta(days=182)
        week_ago = latest_date - timedelta(days=7)

        price_1y = stock_df[stock_df['date'] >= one_year_ago]['close'].iloc[0]
        price_6m = stock_df[stock_df['date'] >= six_months_ago]['close'].iloc[0]
        price_1w = stock_df[stock_df['date'] >= week_ago]['close'].iloc[0]

        growth_1y = ((current_price - price_1y) / price_1y) * 100
        growth_6m = ((current_price - price_6m) / price_6m) * 100
        weekly_change = ((current_price - price_1w) / price_1w) * 100

        result.append({
            "stock": stock,
            "current_price": current_price,
            "growth_1y": round(growth_1y, 2),
            "growth_6m": round(growth_6m, 2),
            "weekly_change": round(weekly_change, 2)
        })
    except:
        continue

# Convert to prompt
recommendation_input = pd.DataFrame(result).sort_values(by='weekly_change')

prompt = f"""
You are a stock advisor. Based on the following data, suggest up to 5 safe stocks to BUY tomorrow.
Conditions:
- Only BUY stocks with negative weekly performance but positive 6-month or 1-year growth.
- Return JSON with: stock name, reason, target buy price.

Data:
{recommendation_input.to_string(index=False)}
"""

response = openai.ChatCompletion.create(
    model="gpt-4",
    messages=[{"role": "user", "content": prompt}],
    temperature=0.2
)

reply = response['choices'][0]['message']['content']

# Save GPT output
with open("gpt_recommendation.json", "w") as f:
    f.write(reply)

print("GPT analysis saved to gpt_recommendation.json")
conn.close()
