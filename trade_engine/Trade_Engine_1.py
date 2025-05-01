from dataclasses import dataclass
from typing import List
import datetime

@dataclass
class OptionContract:
    symbol: str
    underlying: str
    date: datetime.date
    expiry: datetime.date
    strike: float
    option_type: str  # 'call' or 'put'
    delta: float
    price: float

@dataclass
class Trade:
    contract: OptionContract
    entry_date: datetime.date
    exit_date: datetime.date = None
    entry_price: float = None
    exit_price: float = None

    def close(self, exit_price, exit_date):
        self.exit_price = exit_price
        self.exit_date = exit_date

    def pnl(self):
        if self.exit_price is None:
            return 0
        return (self.exit_price - self.entry_price) * 100 - 2  # $2 round-trip fee

class Portfolio:
    def __init__(self, starting_cash=100_000):
        self.cash = starting_cash
        self.positions: List[Trade] = []
        self.closed_trades: List[Trade] = []

    def open_position(self, contract, entry_date):
        trade = Trade(contract=contract, entry_date=entry_date, entry_price=contract.price)
        self.positions.append(trade)
        self.cash -= (contract.price * 100 + 1)  # cost + $1 commission

    def update_positions(self, current_date, price_lookup):
        for trade in self.positions[:]:
            holding_period = (current_date - trade.entry_date).days
            if holding_period >= 5:  # fixed 5-day hold
                exit_price = price_lookup(trade.contract.symbol, current_date)
                trade.close(exit_price=exit_price, exit_date=current_date)
                self.positions.remove(trade)
                self.closed_trades.append(trade)
                self.cash += (exit_price * 100 - 1)  # receive money minus $1 fee

    def summary(self):
        total_pnl = sum(t.pnl() for t in self.closed_trades)
        return {
            'closed_trades': len(self.closed_trades),
            'cash': self.cash,
            'total_pnl': total_pnl,
            'open_positions': len(self.positions)
        }

## Below is the test data

portfolio = Portfolio()

# Replace this with your real database lookup!
def dummy_price_lookup(symbol, date):
    return 5.0  # pretend price

# Example option pulled from your historical data:
contract = OptionContract(
    symbol='AAPL_20250101_150_C',
    underlying='AAPL',
    date=datetime.date(2025, 4, 1),
    expiry=datetime.date(2025, 5, 1),
    strike=150,
    option_type='call',
    delta=0.4,
    price=3.0
)

portfolio.open_position(contract, entry_date=datetime.date(2025, 4, 1))

# Advance 5 days
portfolio.update_positions(current_date=datetime.date(2025, 4, 6), price_lookup=dummy_price_lookup)

print(portfolio.summary())
