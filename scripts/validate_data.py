"""
Data quality validation
"""
import pandas as pd
from datetime import datetime


def validate_news_data(df):
    """
    Comprehensive data quality checks

    Args:
        df: Raw news DataFrame

    Returns:
        cleaned DataFrame, quality report dict
    """
    print("\n🔍 RUNNING DATA QUALITY CHECKS")
    print("=" * 60)

    if df.empty:
        print("❌ DataFrame is empty!")
        return df, {'status': 'FAILED', 'reason': 'Empty dataframe'}

    initial_count = len(df)

    # Initialize quality report
    quality_report = {
        'status': 'PASSED',
        'timestamp': datetime.now().isoformat(),
        'initial_records': initial_count,
        'checks': {}
    }

    # CHECK 1: Missing critical fields
    print("\n📋 Check 1: Missing Critical Fields")
    critical_fields = ['title', 'description', 'url', 'publishedAt']
    missing_counts = {}

    for field in critical_fields:
        if field in df.columns:
            missing = df[field].isnull().sum()
            missing_counts[field] = missing
            if missing > 0:
                print(f"   ⚠️  {field}: {missing} missing ({missing / initial_count * 100:.1f}%)")
            else:
                print(f"   ✅ {field}: No missing values")
        else:
            print(f"   ❌ {field}: Column not found!")
            missing_counts[field] = initial_count

    quality_report['checks']['missing_values'] = missing_counts

    # CHECK 2: Duplicates
    print("\n📋 Check 2: Duplicate Articles")
    duplicates = df.duplicated(subset=['title', 'publishedAt']).sum()
    quality_report['checks']['duplicates'] = duplicates

    if duplicates > 0:
        print(f"   ⚠️  Found {duplicates} duplicate articles ({duplicates / initial_count * 100:.1f}%)")
    else:
        print(f"   ✅ No duplicates found")

        # CHECK 3: Data freshness
        print("\n📋 Check 3: Data Freshness")
        if 'publishedAt' in df.columns:
            df['publishedAt'] = pd.to_datetime(df['publishedAt'], utc=True)
            oldest_article = df['publishedAt'].min()
            newest_article = df['publishedAt'].max()

            # Convert to timezone-naive for comparison
            oldest_naive = oldest_article.tz_localize(None) if oldest_article.tzinfo else oldest_article
            newest_naive = newest_article.tz_localize(None) if newest_article.tzinfo else newest_article

            age_days = (datetime.now() - oldest_naive.to_pydatetime()).days

            print(f"   📅 Oldest article: {oldest_naive.strftime('%Y-%m-%d %H:%M')}")
            print(f"   📅 Newest article: {newest_naive.strftime('%Y-%m-%d %H:%M')}")
            print(f"   📊 Data spans: {age_days} days")

            quality_report['checks']['freshness'] = {
                'oldest': oldest_naive.isoformat(),
                'newest': newest_naive.isoformat(),
                'span_days': age_days
            }
    # CHECK 4: Content quality
    print("\n📋 Check 4: Content Quality")

    # Remove rows with very short titles (likely junk)
    short_titles = (df['title'].str.len() < 10).sum() if 'title' in df.columns else 0

    # Remove rows with very short descriptions
    short_descriptions = (df['description'].str.len() < 20).sum() if 'description' in df.columns else 0

    quality_report['checks']['content_quality'] = {
        'short_titles': short_titles,
        'short_descriptions': short_descriptions
    }

    if short_titles > 0:
        print(f"   ⚠️  {short_titles} articles with very short titles")
    if short_descriptions > 0:
        print(f"   ⚠️  {short_descriptions} articles with very short descriptions")

    # CLEANING: Apply fixes
    print("\n🧹 CLEANING DATA")
    print("=" * 60)

    df_clean = df.copy()

    # Remove nulls in critical fields
    df_clean = df_clean.dropna(subset=['title', 'description'])
    print(f"   ✅ Removed rows with null title/description")

    # Remove duplicates
    df_clean = df_clean.drop_duplicates(subset=['title', 'publishedAt'])
    print(f"   ✅ Removed duplicate articles")

    # Remove short content
    if 'title' in df_clean.columns:
        df_clean = df_clean[df_clean['title'].str.len() >= 10]
    if 'description' in df_clean.columns:
        df_clean = df_clean[df_clean['description'].str.len() >= 20]
    print(f"   ✅ Removed low-quality content")

    final_count = len(df_clean)
    removed = initial_count - final_count

    quality_report['final_records'] = final_count
    quality_report['removed_records'] = removed
    quality_report['removal_percentage'] = (removed / initial_count * 100) if initial_count > 0 else 0

    print(f"\n📊 SUMMARY")
    print("=" * 60)
    print(f"   Initial records: {initial_count}")
    print(f"   Clean records:   {final_count}")
    print(f"   Removed:         {removed} ({quality_report['removal_percentage']:.1f}%)")

    # Determine overall status
    if final_count == 0:
        quality_report['status'] = 'FAILED'
        quality_report['reason'] = 'No records after cleaning'
    elif quality_report['removal_percentage'] > 50:
        quality_report['status'] = 'WARNING'
        quality_report['reason'] = 'High removal rate'
    else:
        quality_report['status'] = 'PASSED'

    print(f"   Status: {quality_report['status']}")

    return df_clean, quality_report


def save_quality_report(report, filename='quality_report.txt'):
    """Save quality report to file"""
    filepath = f'../data/{filename}'

    with open(filepath, 'w') as f:
        f.write("DATA QUALITY REPORT\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Timestamp: {report['timestamp']}\n")
        f.write(f"Status: {report['status']}\n\n")
        f.write(f"Initial Records: {report['initial_records']}\n")
        f.write(f"Final Records: {report['final_records']}\n")
        f.write(f"Removed: {report['removed_records']} ({report['removal_percentage']:.1f}%)\n\n")
        f.write("CHECKS:\n")
        for check_name, check_data in report['checks'].items():
            f.write(f"\n{check_name}:\n")
            f.write(f"  {check_data}\n")

    print(f"📄 Quality report saved to {filepath}")


if __name__ == "__main__":
    # Test with the CSV we created
    print("=" * 60)
    print("TESTING VALIDATION MODULE")
    print("=" * 60)

    df_raw = pd.read_csv('../data/raw_news.csv')

    df_clean, report = validate_news_data(df_raw)

    if not df_clean.empty:
        df_clean.to_csv('../data/clean_news.csv', index=False)
        print(f"\n💾 Saved clean data to data/clean_news.csv")

    save_quality_report(report)