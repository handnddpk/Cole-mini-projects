-- Database initialization script for Real Data Pipeline
-- Creates tables for storing processed data and analytics results

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS realdata_warehouse;

-- Use the database
\c realdata_warehouse;

-- Create extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Twitter daily aggregates table
CREATE TABLE IF NOT EXISTS twitter_daily (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    total_tweets INTEGER NOT NULL,
    unique_users INTEGER NOT NULL,
    avg_sentiment DECIMAL(5,4),
    total_retweets BIGINT,
    total_likes BIGINT,
    total_replies BIGINT,
    languages_used TEXT[],
    language_diversity INTEGER,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, source)
);

-- Twitter hashtag trends table
CREATE TABLE IF NOT EXISTS twitter_hashtags (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    hashtag VARCHAR(100) NOT NULL,
    mention_count INTEGER NOT NULL,
    rank INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, hashtag)
);

-- Reddit daily aggregates table
CREATE TABLE IF NOT EXISTS reddit_daily (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    total_posts INTEGER NOT NULL,
    unique_authors INTEGER NOT NULL,
    active_subreddits INTEGER NOT NULL,
    avg_sentiment DECIMAL(5,4),
    avg_score DECIMAL(8,2),
    avg_upvote_ratio DECIMAL(5,4),
    total_comments BIGINT,
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, source)
);

-- Reddit subreddit activity table
CREATE TABLE IF NOT EXISTS reddit_subreddits (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    subreddit VARCHAR(100) NOT NULL,
    post_count INTEGER NOT NULL,
    avg_score DECIMAL(8,2),
    avg_sentiment DECIMAL(5,4),
    total_comments BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, subreddit)
);

-- News daily aggregates table
CREATE TABLE IF NOT EXISTS news_daily (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    total_articles INTEGER NOT NULL,
    unique_sources INTEGER NOT NULL,
    unique_authors INTEGER NOT NULL,
    avg_sentiment DECIMAL(5,4),
    categories_covered TEXT[],
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, source)
);

-- News categories table
CREATE TABLE IF NOT EXISTS news_categories (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    category VARCHAR(100) NOT NULL,
    article_count INTEGER NOT NULL,
    avg_sentiment DECIMAL(5,4),
    source_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, category)
);

-- Stock daily OHLCV table
CREATE TABLE IF NOT EXISTS stocks_daily (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    sector VARCHAR(100),
    open_price DECIMAL(12,4),
    high_price DECIMAL(12,4),
    low_price DECIMAL(12,4),
    close_price DECIMAL(12,4),
    volume BIGINT,
    market_cap BIGINT,
    avg_pe_ratio DECIMAL(8,2),
    daily_return DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, symbol)
);

-- Stock sector performance table
CREATE TABLE IF NOT EXISTS stocks_sectors (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    sector VARCHAR(100) NOT NULL,
    stock_count INTEGER NOT NULL,
    avg_change DECIMAL(8,4),
    total_volume BIGINT,
    total_market_cap BIGINT,
    gainers INTEGER,
    losers INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, sector)
);

-- Market cap analysis table
CREATE TABLE IF NOT EXISTS stocks_market_cap (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    market_cap_category VARCHAR(50) NOT NULL,
    stock_count INTEGER NOT NULL,
    avg_performance DECIMAL(8,4),
    total_market_cap BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, market_cap_category)
);

-- Crypto daily aggregates table
CREATE TABLE IF NOT EXISTS crypto_daily (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    crypto_tier VARCHAR(50),
    open_price DECIMAL(18,8),
    high_price DECIMAL(18,8),
    low_price DECIMAL(18,8),
    close_price DECIMAL(18,8),
    avg_volume DECIMAL(18,2),
    market_cap BIGINT,
    final_rank INTEGER,
    daily_return DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, symbol)
);

-- Crypto tier performance table
CREATE TABLE IF NOT EXISTS crypto_tiers (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    crypto_tier VARCHAR(50) NOT NULL,
    crypto_count INTEGER NOT NULL,
    avg_change DECIMAL(8,4),
    total_market_cap BIGINT,
    avg_volume DECIMAL(18,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, crypto_tier)
);

-- Trading alerts table
CREATE TABLE IF NOT EXISTS trading_alerts (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    current_price DECIMAL(18,8),
    change_pct DECIMAL(8,4),
    current_volume BIGINT,
    trend_signal VARCHAR(20),
    asset_type VARCHAR(20),
    alert_timestamp TIMESTAMP NOT NULL,
    alert_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Market sector performance table
CREATE TABLE IF NOT EXISTS market_sector_performance (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    sector VARCHAR(100) NOT NULL,
    avg_sector_change DECIMAL(8,4),
    stock_count INTEGER,
    total_market_cap BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, sector)
);

-- Market breadth indicators table
CREATE TABLE IF NOT EXISTS market_market_breadth (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    advancing_stocks INTEGER,
    declining_stocks INTEGER,
    total_stocks INTEGER,
    market_avg_change DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date)
);

-- Daily correlations table
CREATE TABLE IF NOT EXISTS daily_correlations (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    correlation_type VARCHAR(100) NOT NULL,
    correlation_value DECIMAL(8,6),
    calculated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily anomalies table
CREATE TABLE IF NOT EXISTS daily_anomalies (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    anomaly_type VARCHAR(100) NOT NULL,
    observed_value DECIMAL(12,4),
    expected_value DECIMAL(12,4),
    description TEXT,
    detected_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ML detected anomalies table
CREATE TABLE IF NOT EXISTS ml_detected_anomalies (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    date DATE NOT NULL,
    total_tweets INTEGER,
    avg_sentiment DECIMAL(5,4),
    tweets_zscore DECIMAL(8,4),
    sentiment_zscore DECIMAL(8,4),
    anomaly_type VARCHAR(50),
    detected_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User clusters table
CREATE TABLE IF NOT EXISTS user_clusters (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    cluster_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id)
);

-- ML insights table
CREATE TABLE IF NOT EXISTS ml_insights (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    insight_type VARCHAR(100) NOT NULL,
    total_analyzed INTEGER,
    avg_predicted_sentiment DECIMAL(5,4),
    unique_users INTEGER,
    predicted_market_return DECIMAL(8,4),
    generated_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better performance
CREATE INDEX IF NOT EXISTS idx_twitter_daily_date ON twitter_daily(date);
CREATE INDEX IF NOT EXISTS idx_twitter_hashtags_date ON twitter_hashtags(date);
CREATE INDEX IF NOT EXISTS idx_reddit_daily_date ON reddit_daily(date);
CREATE INDEX IF NOT EXISTS idx_reddit_subreddits_date ON reddit_subreddits(date);
CREATE INDEX IF NOT EXISTS idx_news_daily_date ON news_daily(date);
CREATE INDEX IF NOT EXISTS idx_news_categories_date ON news_categories(date);
CREATE INDEX IF NOT EXISTS idx_stocks_daily_date_symbol ON stocks_daily(date, symbol);
CREATE INDEX IF NOT EXISTS idx_stocks_sectors_date ON stocks_sectors(date);
CREATE INDEX IF NOT EXISTS idx_crypto_daily_date_symbol ON crypto_daily(date, symbol);
CREATE INDEX IF NOT EXISTS idx_trading_alerts_timestamp ON trading_alerts(alert_timestamp);
CREATE INDEX IF NOT EXISTS idx_daily_correlations_date ON daily_correlations(date);
CREATE INDEX IF NOT EXISTS idx_daily_anomalies_date ON daily_anomalies(date);
CREATE INDEX IF NOT EXISTS idx_ml_insights_generated_at ON ml_insights(generated_at);

-- Grant permissions (adjust as needed)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
