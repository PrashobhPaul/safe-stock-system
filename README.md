# Safe-Bet Stock Recommendation System

This app fetches real-time stock data for a custom list of 50 stocks, uses GPT-4 to generate buy/sell suggestions based on smart rules, and displays them in a Streamlit interface.

### 🔧 Setup

1. Clone the repo
2. Install requirements:
3. Add your OpenAI API key in `gpt_analyze.py` or use an `.env` loader

### 🚀 Run Manually

```bash
python data_fetch.py
python gpt_analyze.py
streamlit run streamlit_app.py
