"""
Extract financial news from NewsAPI
"""
import requests
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()
API_KEY = os.getenv('NEWS_API_KEY')


def extract_financial_news(
        keywords=['stocks', 'market', 'finance', 'economy'],
        days_back=7,
        page_size=100
):
    """
    Fetch financial news from NewsAPI

    Args:
        keywords: List of financial keywords to search
        days_back: How many days of history to fetch
        page_size: Number of articles (max 100 for free tier)

    Returns:
        pandas DataFrame with articles
    """
    print(f"📡 Fetching financial news...")
    print(f"   Keywords: {', '.join(keywords)}")
    print(f"   Period: Last {days_back} days")

    # Calculate date range
    to_date = datetime.now()
    from_date = to_date - timedelta(days=days_back)

    # Build query
    query = ' OR '.join(keywords)

    url = 'https://newsapi.org/v2/everything'
    params = {
        'q': query,
        'from': from_date.strftime('%Y-%m-%d'),
        'to': to_date.strftime('%Y-%m-%d'),
        'language': 'en',
        'sortBy': 'publishedAt',
        'pageSize': page_size,
        'apiKey': API_KEY
    }

    try:
        print(f"   Making API request...")
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        if data['status'] == 'ok':
            articles = data['articles']

            # Convert to DataFrame
            df = pd.DataFrame(articles)

            # Basic cleaning
            if not df.empty:
                # Extract source name from nested dict
                df['source_name'] = df['source'].apply(
                    lambda x: x.get('name', 'Unknown') if isinstance(x, dict) else 'Unknown'
                )

                # Convert published date
                df['publishedAt'] = pd.to_datetime(df['publishedAt'])

                # Add extraction timestamp
                df['extracted_at'] = datetime.now()

                print(f"✅ Successfully extracted {len(df)} articles")
                return df
            else:
                print("⚠️  No articles found")
                return pd.DataFrame()
        else:
            print(f"❌ API Error: {data.get('message')}")
            return pd.DataFrame()

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return pd.DataFrame()


def save_raw_data(df, filename='raw_news.csv'):
    """Save raw data to CSV"""
    if not df.empty:
        filepath = f'../data/{filename}'
        df.to_csv(filepath, index=False)
        print(f"💾 Saved to {filepath}")
        return filepath
    return None


if __name__ == "__main__":
    # Test the function
    print("=" * 60)
    print("TESTING EXTRACT MODULE")
    print("=" * 60)

    df = extract_financial_news(days_back=7, page_size=50)

    if not df.empty:
        print("\n📊 Sample data:")
        print(df[['title', 'source_name', 'publishedAt']].head())

        # Save for inspection
        save_raw_data(df)
    else:
        print("❌ No data extracted")


