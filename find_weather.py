"""Search for ANY weather-related markets on Kalshi."""
import asyncio, sys
sys.path.insert(0, ".")
from exchanges.kalshi_wrapper import KalshiExchange

async def main():
    k = KalshiExchange()
    await k.connect()
    
    # Fetch a massive list to make sure we aren't missing them in pagination
    markets = await k.fetch_markets()
    print(f"Total markets fetched: {len(markets)}")
    
    weather_keywords = ["temperature", "precip", "weather", "degree", "rain", "snow", "inch", "Fahrenheit"]
    weather_found = []
    
    for m in markets:
        title = m.get("title", "").lower()
        ticker = m.get("ticker", "").lower()
        if any(kw in title or kw in ticker for kw in weather_keywords):
            weather_found.append(m)
            
    print(f"\nFound {len(weather_found)} weather-related markets:")
    for m in weather_found[:10]:
        print(f"  {m.get('ticker')}: {m.get('title')} (status={m.get('status')})")
        
    await k.disconnect()

asyncio.run(main())
