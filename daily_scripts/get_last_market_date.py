from datetime import datetime, timedelta

def get_last_market_date() -> str:
    """
    Returns the last market close date as a string in 'YYYY-MM-DD' format.
    - If today is a weekend, it returns the last Friday.
    - Otherwise, it returns today's date.
    """
    today = datetime.today()
    if today.weekday() >= 5:  # Saturday (5) or Sunday (6)
        # Subtract days to get to Friday
        offset = today.weekday() - 4  # 1 for Saturday, 2 for Sunday
        last_market_date = today - timedelta(days=offset)
    else:
        last_market_date = today

    return last_market_date.strftime('%Y-%m-%d')

