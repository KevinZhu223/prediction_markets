import httpx

raw = httpx.get('https://api.elections.kalshi.com/trade-api/v2/markets/KXMLBSPREAD-26APR051335BALPIT-PIT7/orderbook').json()
fp = raw.get('orderbook', {})
print("orderbook", fp)
fp2 = raw.get('orderbook_fp', {})
print("orderbook_fp", fp2)
