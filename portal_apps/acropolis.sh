#!/bin/bash

PYTHON_PATH="/Library/Frameworks/Python.framework/Versions/3.12/bin/python3.12"
TRADE_SUGGEST_SCRIPT="gradio_trade_suggest_2.py"
DASHBOARD_SCRIPT="gradio_InfrastructureDashboard.py"
OPTIONS_PIE="gradio_ticker_count_pie.py"
BULL_PUT="gradio_bull_put_spread.py"


start() {
    echo "Starting Trade Suggestion App..."
    $PYTHON_PATH $TRADE_SUGGEST_SCRIPT &   # runs in the background
    TRADE_SUGGEST_PID=$!
    echo $TRADE_SUGGEST_PID > trade_suggest.pid
    echo "Trade Suggestion App running as PID $TRADE_SUGGEST_PID"

    echo "Starting Infrastructure Dashboard App..."
    $PYTHON_PATH $DASHBOARD_SCRIPT &      # runs in the background
    DASHBOARD_PID=$!
    echo $DASHBOARD_PID > dashboard.pid
    echo "Infrastructure Dashboard App running as PID $DASHBOARD_PID"

    echo "Starting Options Pie Chart App..."
    $PYTHON_PATH $OPTIONS_PIE &      # runs in the background
    OPTIONS_PID=$!
    echo $OPTIONS_PID > options.pid
    echo "Options Pie Chart App running as PID $DASHBOARD_PID"

    echo "Starting Bull Put Spread App..."
    $PYTHON_PATH $BULL_PUT &      # runs in the background
    BULL_PUT_PID=$!
    echo $BULL_PUT_PID > options.pid
    echo "Bull Put Spread App running as PID $BULL_PUT_PID"
}

stop() {
    if [ -f trade_suggest.pid ]; then
        kill $(cat trade_suggest.pid) && echo "Stopped Trade Suggestion App."
        rm trade_suggest.pid
    else
        echo "Trade Suggestion App not running."
    fi

    if [ -f dashboard.pid ]; then
        kill $(cat dashboard.pid) && echo "Stopped Infrastructure Dashboard App."
        rm dashboard.pid
    else
        echo "Infrastructure Dashboard App not running."
    fi

    if [ -f options.pid ]; then
        kill $(cat options.pid) && echo "Stopped Options Pie Chart App."
        rm dashboard.pid
    else
        echo "Options Pie Chart App not running."
    fi
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        start
        ;;
    *)
        echo "Usage: $0 {start|stop|restart}"
        exit 1
esac
