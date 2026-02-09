"""
Transform: Add sentiment analysis
"""
import pandas as pd
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime


def add_sentiment_analysis(df):
    """
    Add sentiment scores to articles

    Args:
        df: Clean news DataFrame

    Returns:
        DataFrame with sentiment columns
    """
    print("\n🤖 SENTIMENT ANALYSIS")
    print("=" * 60)

    if df.empty:
        print("❌ DataFrame is empty!")
        return df

    # Initialize VADER
    analyzer = SentimentIntensityAnalyzer()

    print(f"   Analyzing {len(df)} articles...")

    # Function to analyze single article
    def analyze_article(row):
        # Combine title + description for better analysis
        text = str(row.get('title', '')) + ' ' + str(row.get('description', ''))

        # Get sentiment scores
        scores = analyzer.polarity_scores(text)

        return pd.Series({
            'sentiment_compound': scores['compound'],  # -1 to +1
            'sentiment_positive': scores['pos'],  # 0 to 1
            'sentiment_negative': scores['neg'],  # 0 to 1
            'sentiment_neutral': scores['neu']  # 0 to 1
        })

    # Apply sentiment analysis
    sentiment_df = df.apply(analyze_article, axis=1)

    # Combine with original data
    df_with_sentiment = pd.concat([df, sentiment_df], axis=1)

    # Categorize sentiment
    def categorize_sentiment(compound_score):
        if compound_score >= 0.05:
            return 'positive'
        elif compound_score <= -0.05:
            return 'negative'
        else:
            return 'neutral'

    df_with_sentiment['sentiment_label'] = df_with_sentiment['sentiment_compound'].apply(
        categorize_sentiment
    )

    # Add date features for analysis
    if 'publishedAt' in df_with_sentiment.columns:
        df_with_sentiment['publishedAt'] = pd.to_datetime(df_with_sentiment['publishedAt'])
        df_with_sentiment['date'] = df_with_sentiment['publishedAt'].dt.date
        df_with_sentiment['hour'] = df_with_sentiment['publishedAt'].dt.hour
        df_with_sentiment['day_of_week'] = df_with_sentiment['publishedAt'].dt.day_name()

    # Show distribution
    print(f"\n📊 Sentiment Distribution:")
    sentiment_counts = df_with_sentiment['sentiment_label'].value_counts()
    for label, count in sentiment_counts.items():
        percentage = (count / len(df_with_sentiment)) * 100
        print(f"   {label.capitalize():10s}: {count:3d} ({percentage:5.1f}%)")

    # Statistics
    avg_sentiment = df_with_sentiment['sentiment_compound'].mean()
    print(f"\n📈 Average Sentiment Score: {avg_sentiment:.3f}")

    if avg_sentiment >= 0.25:
        print("   ➜ Overall: POSITIVE market sentiment")
    elif avg_sentiment <= -0.25:
        print("   ➜ Overall: NEGATIVE market sentiment")
    else:
        print("   ➜ Overall: NEUTRAL market sentiment")

    print(f"\n✅ Sentiment analysis complete")

    return df_with_sentiment


def save_transformed_data(df, filename='news_with_sentiment.csv'):
    """Save transformed data"""
    if not df.empty:
        filepath = f'../data/{filename}'
        df.to_csv(filepath, index=False)
        print(f"💾 Saved to {filepath}")
        return filepath
    return None


if __name__ == "__main__":
    # Test
    print("=" * 60)
    print("TESTING SENTIMENT TRANSFORMATION")
    print("=" * 60)

    df_clean = pd.read_csv('../data/clean_news.csv')

    df_final = add_sentiment_analysis(df_clean)

    if not df_final.empty:
        save_transformed_data(df_final)

        # Show sample
        print(f"\n📰 Sample Articles with Sentiment:")
        print(df_final[['title', 'sentiment_compound', 'sentiment_label']].head(10))