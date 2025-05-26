import streamlit as st
import json
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="Daily Stock Recommendations", layout="wide")

st.title("Safe-Bet Stock Picks for Tomorrow")

# Load GPT results
try:
    with open("gpt_recommendation.json", "r") as f:
        gpt_data = json.load(f)

    if isinstance(gpt_data, list):
        df = pd.DataFrame(gpt_data)
        st.success(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}" )
        st.dataframe(df, use_container_width=True)
    else:
        st.warning("The GPT response format is not recognized. Please check the JSON structure.")

except FileNotFoundError:
    st.error("No recommendations found. Run gpt_analyze.py to generate.")
except json.JSONDecodeError:
    st.error("Error reading GPT output. JSON might be malformed.")

# Optional: Budget Input
budget = st.number_input("Optional: Enter your budget for tomorrow (INR)", min_value=1000, value=100000, step=1000)

# Display suggested quantity per stock if price is available
if 'df' in locals() and 'target buy price' in df.columns:
    df['Quantity'] = (budget / df['target buy price']).astype(int)
    st.subheader("With Your Budget:")
    st.dataframe(df[['stock name', 'target buy price', 'Quantity', 'reason']], use_container_width=True)

st.markdown("---")
st.caption("This app uses daily GPT-4 analysis and Yahoo Finance data to suggest safe investment opportunities. Use at your own discretion.")
