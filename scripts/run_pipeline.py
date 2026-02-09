"""
Master pipeline script - runs all steps
"""
from extract_news import extract_financial_news, save_raw_data
from validate_data import validate_news_data, save_quality_report
from transform_sentiment import add_sentiment_analysis, save_transformed_data
from load_to_database import load_to_duckdb
from datetime import datetime
import sys


def run_full_pipeline(days_back=7, page_size=50):
    """Run complete ETL pipeline"""

    print("\n")
    print("=" * 70)
    print("🚀 FINANCIAL NEWS ETL PIPELINE")
    print("=" * 70)
    print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    pipeline_status = {
        'start_time': datetime.now(),
        'steps': {},
        'overall_status': 'SUCCESS'
    }

    # STEP 1: EXTRACT
    print("\n" + "=" * 70)
    print("📍 STEP 1/4: EXTRACT")
    print("=" * 70)

    try:
        df_raw = extract_financial_news(days_back=days_back, page_size=page_size)

        if df_raw.empty:
            print("❌ PIPELINE FAILED: No data extracted")
            pipeline_status['overall_status'] = 'FAILED'
            pipeline_status['steps']['extract'] = 'FAILED - No data'
            return pipeline_status

        save_raw_data(df_raw)
        pipeline_status['steps']['extract'] = f'SUCCESS - {len(df_raw)} articles'

    except Exception as e:
        print(f"❌ Extract failed: {e}")
        pipeline_status['overall_status'] = 'FAILED'
        pipeline_status['steps']['extract'] = f'FAILED - {str(e)}'
        return pipeline_status

    # STEP 2: VALIDATE
    print("\n" + "=" * 70)
    print("📍 STEP 2/4: VALIDATE")
    print("=" * 70)

    try:
        df_clean, quality_report = validate_news_data(df_raw)

        if df_clean.empty:
            print("❌ PIPELINE FAILED: No clean data after validation")
            pipeline_status['overall_status'] = 'FAILED'
            pipeline_status['steps']['validate'] = 'FAILED - No clean data'
            return pipeline_status

        save_quality_report(quality_report)
        pipeline_status['steps']['validate'] = f'SUCCESS - {len(df_clean)} clean records'

    except Exception as e:
        print(f"❌ Validation failed: {e}")
        pipeline_status['overall_status'] = 'FAILED'
        pipeline_status['steps']['validate'] = f'FAILED - {str(e)}'
        return pipeline_status

    # STEP 3: TRANSFORM
    print("\n" + "=" * 70)
    print("📍 STEP 3/4: TRANSFORM (Sentiment Analysis)")
    print("=" * 70)

    try:
        df_final = add_sentiment_analysis(df_clean)

        if df_final.empty:
            print("❌ PIPELINE FAILED: Transformation failed")
            pipeline_status['overall_status'] = 'FAILED'
            pipeline_status['steps']['transform'] = 'FAILED'
            return pipeline_status

        save_transformed_data(df_final)
        avg_sentiment = df_final['sentiment_compound'].mean()
        pipeline_status['steps']['transform'] = f'SUCCESS - Avg sentiment: {avg_sentiment:.3f}'

    except Exception as e:
        print(f"❌ Transformation failed: {e}")
        pipeline_status['overall_status'] = 'FAILED'
        pipeline_status['steps']['transform'] = f'FAILED - {str(e)}'
        return pipeline_status

    # STEP 4: LOAD
    print("\n" + "=" * 70)
    print("📍 STEP 4/4: LOAD TO DATABASE")
    print("=" * 70)

    try:
        success = load_to_duckdb(df_final)

        if not success:
            print("❌ PIPELINE FAILED: Load to database failed")
            pipeline_status['overall_status'] = 'WARNING'
            pipeline_status['steps']['load'] = 'FAILED'
        else:
            pipeline_status['steps']['load'] = f'SUCCESS - {len(df_final)} records loaded'

    except Exception as e:
        print(f"❌ Load failed: {e}")
        pipeline_status['overall_status'] = 'WARNING'
        pipeline_status['steps']['load'] = f'FAILED - {str(e)}'

    # PIPELINE SUMMARY
    pipeline_status['end_time'] = datetime.now()
    duration = (pipeline_status['end_time'] - pipeline_status['start_time']).total_seconds()

    print("\n" + "=" * 70)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("=" * 70)
    print(f"Status: {pipeline_status['overall_status']}")
    print(f"Duration: {duration:.1f} seconds")
    print(f"\nStep Results:")
    for step, status in pipeline_status['steps'].items():
        print(f"  {step.upper():12s}: {status}")
    print("=" * 70)

    if pipeline_status['overall_status'] == 'SUCCESS':
        print("✅ PIPELINE COMPLETED SUCCESSFULLY")
    elif pipeline_status['overall_status'] == 'WARNING':
        print("⚠️  PIPELINE COMPLETED WITH WARNINGS")
    else:
        print("❌ PIPELINE FAILED")

    print("=" * 70)

    return pipeline_status


if __name__ == "__main__":
    # Run the pipeline
    run_full_pipeline(days_back=7, page_size=50)
