CREATE TABLE feature_store (
    option_id INTEGER PRIMARY KEY,
    ticker TEXT,
    fetch_date TEXT,
    expiry TEXT,
    option_type TEXT,
    strike REAL,
    underlying_price REAL,
    days_to_expiry REAL,
    implied_volatility REAL,
    historical_volatility_20 REAL,
    delta REAL,
    gamma REAL,
    vega REAL,
    theta REAL,
    rho REAL,
    open_interest INTEGER,
    volume INTEGER,
    rfr REAL,
    llm_sentiment_score REAL,
    fomc_distance_days INTEGER,
    earnings_distance_days INTEGER
);

