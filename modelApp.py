import sqlite3
import pandas as pd

DB_NAME = "stocks.db"
TABLE_NAME = "stock_data"
MODEL_TABLE_NAME = "stock_model"



# Function to calculate rolling metrics
def calculate_rolling_metrics(df, window):
    # Group by Symbol and apply rolling metrics
    df[f'Average_Daily_Return_{window}'] = df.groupby('Symbol')['Performance'].rolling(window=window).mean().reset_index(0, drop=True)
    df[f'Volatility_{window}'] = df.groupby('Symbol')['Performance'].rolling(window=window).std().reset_index(0, drop=True)
    return df



def create_stock_model():
    with sqlite3.connect(DB_NAME) as conn:
        # Load all data from the stock_data table
        query = f"SELECT * FROM {TABLE_NAME}"
        df = pd.read_sql_query(query, conn)

    # Ensure data is sorted by Symbol and Date
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values(by=['Symbol', 'Date'], inplace=True)

    # Calculate additional columns
    window = 5  # You can adjust the rolling window size here
    df = calculate_rolling_metrics(df, window)

    # Add Recent Performance (last 5 days of Performance)
    df['Recent_Performance'] = df.groupby('Symbol')['Performance'].rolling(window=5).mean().reset_index(0, drop=True)

    # Apply the filter after calculations
    filtered_df = df[(df['Performance'] >= 2) & (df['cdpp'] >= 2)].copy()

    # Save the data into the stock_model table
    with sqlite3.connect(DB_NAME) as conn:
        cursor = conn.cursor()

        # Create stock_model table
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {MODEL_TABLE_NAME} (
                Date TEXT,
                Open REAL,
                High REAL,
                Low REAL,
                Close REAL,
                Adj_Close REAL,
                Volume INTEGER,
                Symbol TEXT,
                Performance REAL,
                cdpp INTEGER,
                Average_Daily_Return_{window} REAL,
                Volatility_{window} REAL,
                Recent_Performance REAL
            )
        ''')

        # Save filtered DataFrame to the new table
        filtered_df.to_sql(MODEL_TABLE_NAME, conn, if_exists='replace', index=False)

        print(f"Data saved to {MODEL_TABLE_NAME} with additional columns.")







if __name__ == "__main__":
    create_stock_model()
