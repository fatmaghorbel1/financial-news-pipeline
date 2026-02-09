"""
Load data to DuckDB database
"""
import pandas as pd
import duckdb
from datetime import datetime


def load_to_duckdb(df, db_path='../data/financial_news.db'):
    """
    Load enriched data into DuckDB

    Args:
        df: DataFrame with sentiment scores
        db_path: Path to DuckDB database
    """
    print("\n💾 LOADING TO DATABASE")
    print("=" * 60)

    if df.empty:
        print("❌ DataFrame is empty!")
        return False

    print(f"   Database: {db_path}")
    print(f"   Records to load: {len(df)}")

    try:
        # Connect to DuckDB
        conn = duckdb.connect(db_path)

        # Drop existing table to avoid schema conflicts
        conn.execute("DROP TABLE IF EXISTS news_sentiment")

        # Create table with exact columns we have
        conn.execute("""
            CREATE TABLE news_sentiment (
                title VARCHAR,
                description VARCHAR,
                url VARCHAR,
                source_name VARCHAR,
                publishedAt TIMESTAMP,
                date DATE,
                hour INTEGER,
                sentiment_compound DOUBLE,
                sentiment_positive DOUBLE,
                sentiment_negative DOUBLE,
                sentiment_neutral DOUBLE,
                sentiment_label VARCHAR,
                extracted_at TIMESTAMP
            )
        """)

        print("   ✅ Table 'news_sentiment' created")

        # Prepare data
        df_to_load = df.copy()

        # Add extracted_at if missing
        if 'extracted_at' not in df_to_load.columns:
            df_to_load['extracted_at'] = datetime.now()

        # Ensure date column is proper date type
        if 'date' in df_to_load.columns:
            df_to_load['date'] = pd.to_datetime(df_to_load['date']).dt.date

        # Select columns in exact order matching table
        columns = [
            'title', 'description', 'url', 'source_name',
            'publishedAt', 'date', 'hour',
            'sentiment_compound', 'sentiment_positive', 'sentiment_negative',
            'sentiment_neutral', 'sentiment_label', 'extracted_at'
        ]

        # Only use columns that exist
        available_cols = [c for c in columns if c in df_to_load.columns]
        df_to_load = df_to_load[available_cols]

        print(f"   📋 Columns to insert: {len(available_cols)}")

        # Insert data
        conn.execute("INSERT INTO news_sentiment SELECT * FROM df_to_load")

        # Get statistics
        total_records = conn.execute("SELECT COUNT(*) FROM news_sentiment").fetchone()[0]

        print(f"   ✅ Loaded {len(df_to_load)} records")
        print(f"   📊 Total records in database: {total_records}")

        # Show latest records
        print(f"\n📰 Latest 5 Articles in Database:")
        latest = conn.execute("""
            SELECT 
                title,
                sentiment_label,
                sentiment_compound,
                date
            FROM news_sentiment
            ORDER BY publishedAt DESC
            LIMIT 5
        """).df()

        if not latest.empty:
            # Truncate long titles for display
            latest['title'] = latest['title'].str[:50]
            print(latest.to_string(index=False))

        conn.close()
        print(f"\n✅ Database operation complete")
        return True

    except Exception as e:
        print(f"❌ Database error: {e}")
        print(f"   Available columns in DataFrame: {list(df.columns)}")
        import traceback
        traceback.print_exc()
        return False


def query_database(db_path='../data/financial_news.db', query=None):
    """Query the database"""
    if query is None:
        query = "SELECT COUNT(*) as total_articles FROM news_sentiment"

    try:
        conn = duckdb.connect(db_path, read_only=True)
        result = conn.execute(query).df()
        conn.close()
        return result
    except Exception as e:
        print(f"❌ Query error: {e}")
        return None


if __name__ == "__main__":
    # Test
    print("=" * 60)
    print("TESTING DATABASE LOAD")
    print("=" * 60)

    df = pd.read_csv('../data/news_with_sentiment.csv')

    success = load_to_duckdb(df)

    if success:
        print("\n🔍 Database Statistics:")

        # Total articles
        stats = query_database(query="""
            SELECT 
                COUNT(*) as total_articles,
                COUNT(DISTINCT date) as days_covered,
                MIN(publishedAt) as oldest_article,
                MAX(publishedAt) as newest_article
            FROM news_sentiment
        """)
        print(stats.to_string(index=False))

        # Sentiment breakdown
        sentiment_dist = query_database(query="""
            SELECT 
                sentiment_label,
                COUNT(*) as count,
                ROUND(AVG(sentiment_compound), 3) as avg_score
            FROM news_sentiment
            GROUP BY sentiment_label
            ORDER BY count DESC
        """)
        print("\n📊 Sentiment Distribution:")
        print(sentiment_dist.to_string(index=False))