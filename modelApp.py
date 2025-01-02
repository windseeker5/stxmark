import sqlite3
import pandas as pd
import requests
from datetime import datetime, timedelta



DB_NAME = "stocks.db"
TABLE_NAME = "stock_data"
SENTIMENT_API_KEY = "b54f86efb06248cc80a85d20150a9e3c"  # Replace with your News API key or other sentiment provider
SENTIMENT_API_URL = "https://newsapi.org/v2/everything"  # Replace with your sentiment provider's URL



def fetch_top_performers():
    """Fetch stocks with cdpp >= 2 from the database."""
    with sqlite3.connect(DB_NAME) as conn:
        query = f"""
            SELECT *
            FROM {TABLE_NAME}
            WHERE cdpp >= 2
        """
        df = pd.read_sql_query(query, conn)
    return df




def fetch_sentiment(symbol):
    """Fetch sentiment for a stock symbol using an external API."""
    try:
        params = {
            "q": symbol,
            "apiKey": SENTIMENT_API_KEY,
            "from": (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d'),
            "sortBy": "relevancy",
            "language": "en",
        }
        response = requests.get(SENTIMENT_API_URL, params=params)
        response.raise_for_status()
        articles = response.json().get("articles", [])
        
        # Analyze sentiment: count positive, negative, and neutral articles
        positive, negative, neutral = 0, 0, 0
        for article in articles:
            title = article.get("title", "") or ""
            description = article.get("description", "") or ""
            content = title + " " + description
            if "good" in content.lower() or "positive" in content.lower():
                positive += 1
            elif "bad" in content.lower() or "negative" in content.lower():
                negative += 1
            else:
                neutral += 1

        # Calculate sentiment score
        total = positive + negative + neutral
        sentiment_score = (positive - negative) / total if total > 0 else 0
        return sentiment_score, positive, negative, neutral
    except Exception as e:
        print(f"Error fetching sentiment for {symbol}: {e}")
        return 0, 0, 0, 0



def add_sentiment_analysis(df):
    """Add sentiment analysis results to the DataFrame."""
    sentiments = []
    for symbol in df["Symbol"].unique():
        print(f"Fetching sentiment for {symbol}...")
        sentiment_score, positive, negative, neutral = fetch_sentiment(symbol)
        sentiments.append((symbol, sentiment_score, positive, negative, neutral))

    # Convert to DataFrame and merge with the original data
    sentiment_df = pd.DataFrame(sentiments, columns=["Symbol", "Sentiment_Score", "Positive_Count", "Negative_Count", "Neutral_Count"])
    df = df.merge(sentiment_df, on="Symbol", how="left")
    return df


def save_to_database(df):
    """Save the updated DataFrame back to a new table in the database."""
    with sqlite3.connect(DB_NAME) as conn:
        df.to_sql("stock_model", conn, if_exists="replace", index=False)
        print("Data with sentiment analysis saved to 'stock_model' table.")




def main():
    # Step 1: Fetch top performers
    top_performers = fetch_top_performers()
    print(f"Fetched {len(top_performers)} top-performing stocks.")

    # Step 2: Add sentiment analysis
    updated_data = add_sentiment_analysis(top_performers)

    # Step 3: Save to database
    save_to_database(updated_data)



if __name__ == "__main__":
    main()
