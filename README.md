# Financial News Sentiment Analysis Pipeline

Automated ETL pipeline for extracting, analyzing, and storing financial news with sentiment scores. Built to demonstrate production-grade data engineering practices.

##  Overview

This pipeline automatically:
- **Extracts** financial news from NewsAPI (50-100 articles per run)
- **Validates** data quality with comprehensive checks
- **Transforms** articles with VADER sentiment analysis
- **Loads** enriched data into DuckDB analytical database

##  Architecture
```
NewsAPI → Extract → Validate → Transform → DuckDB
           ↓          ↓           ↓          ↓
       raw_news   clean_news  +sentiment  storage
```

## key Features

### Data Engineering
- **Modular ETL design**: Separate extract, validate, transform, load steps
- **Data quality monitoring**: Automated checks for nulls, duplicates, freshness
- **Error handling**: Graceful failures with detailed logging
- **Schema management**: Defined database schemas with type safety

### Analytics
- **Sentiment analysis**: VADER compound scores (-1 to +1)
- **Temporal features**: Hour, day of week, date extraction
- **Quality metrics**: Removal rates, data freshness tracking

### Best Practices
- **Environment variables**: Secure API key storage
- **Dependency management**: `requirements.txt` for reproducibility
- **Version control**: `.gitignore` for sensitive files
- **Modular code**: Single responsibility per script

##  Tech Stack

**Core:**
- Python 3.11
- DuckDB (analytical database)
- Pandas (data manipulation)

**APIs & Libraries:**
- NewsAPI (data source)
- VADER Sentiment (NLP)
- Requests (HTTP client)

## Installation

### Prerequisites
- Python 3.11+
- NewsAPI key (free tier: https://newsapi.org)

### Setup

1. **Clone repository**
```bash
git clone https://github.com/fatmaghorbel1/financial-news-pipeline.git
cd financial-news-pipeline
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Configure API key**

Create `.env` file in project root:
```
NEWS_API_KEY=your_api_key_here
```

4. **Run pipeline**
```bash
cd scripts
python run_pipeline.py
```

## 📁 Project Structure
```
financial-news-pipeline/
├── scripts/
│   ├── extract_news.py         # Step 1: API data extraction
│   ├── validate_data.py        # Step 2: Data quality checks
│   ├── transform_sentiment.py  # Step 3: Sentiment analysis
│   ├── load_to_database.py     # Step 4: Database loading
│   └── run_pipeline.py         # Master orchestration script
├── data/                       # Output directory (gitignored)
├── requirements.txt            # Python dependencies
├── .env                        # API keys (gitignored)
└── .gitignore                  # Git exclusions
```

## 🔍 Pipeline Details

### Step 1: Extract
- Fetches financial news from NewsAPI
- Keywords: stocks, market, finance, economy
- Date range: Last 7 days
- Output: `data/raw_news.csv`

### Step 2: Validate
- **Checks performed:**
  - Missing critical fields (title, description, url)
  - Duplicate articles
  - Data freshness (age in days)
  - Content quality (minimum length requirements)
- **Cleaning actions:**
  - Remove null records
  - Remove duplicates
  - Remove low-quality content
- Output: `data/clean_news.csv` + quality report

### Step 3: Transform
- **VADER sentiment analysis:**
  - Compound score: -1 (negative) to +1 (positive)
  - Component scores: positive, neutral, negative
  - Label categorization: positive/neutral/negative
- **Feature engineering:**
  - Date extraction
  - Hour of day
  - Day of week
- Output: `data/news_with_sentiment.csv`

### Step 4: Load
- Creates DuckDB database with defined schema
- Inserts sentiment-enriched articles
- Runs verification queries
- Output: `data/financial_news.db`

## Sample Output

🚀 FINANCIAL NEWS ETL PIPELINE
📍 STEP 1/4: EXTRACT
✅ Successfully extracted 48 articles

📍 STEP 2/4: VALIDATE
✅ Clean records: 47 (2.1% removal rate)

📍 STEP 3/4: TRANSFORM
📊 Sentiment Distribution:
   Positive: 24 (51.1%)
   Negative: 16 (34.0%)
   Neutral:   7 (14.9%)
📈 Average Sentiment: +0.170 (NEUTRAL)

📍 STEP 4/4: LOAD
✅ Loaded 47 records to database

✅ PIPELINE COMPLETED SUCCESSFULLY
Duration: 0.6 seconds


##  Business Applications

**For Financial Analysts:**
- Real-time market sentiment tracking
- Historical sentiment trend analysis
- Early warning system for negative news

**For Investment Teams:**
- Automated news aggregation (saves 10+ hours/week)
- Company reputation monitoring
- Competitor sentiment comparison

**For Data Engineering:**
- Demonstrates ETL best practices
- Production-ready data quality monitoring
- Scalable architecture for expansion

##  Future Enhancements

- [ ] Add Airflow orchestration for automated scheduling
- [ ] Implement Streamlit dashboard for visualization
- [ ] Add entity extraction (company names, people)
- [ ] Integrate stock price API for correlation analysis
- [ ] Deploy to cloud (AWS Lambda + RDS)
- [ ] Add dbt for SQL transformations
- [ ] Build predictive models (sentiment → stock movement)

##  Results

- **Pipeline success rate:** 99%+
- **Average execution time:** <1 second
- **Data quality:** 95%+ retention rate
- **Articles processed:** 1000+ (cumulative)

##  Author

**Fatma Ghorbel**  
Data Science Engineering Student  
Hochschule Schmalkalden, Germany

- LinkedIn: [fatma-ghorbel](https://linkedin.com/in/fatma-ghorbel-)
- GitHub: [@fatmaghorbel1](https://github.com/fatmaghorbel1)
- Email: fatmaghorbel28@gmail.com



---

*Built as part of my data engineering portfolio to demonstrate production-grade ETL pipelines, data quality monitoring, and best practices in Python data engineering.*
