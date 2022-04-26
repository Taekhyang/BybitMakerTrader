## How it works
Bybit Maker Trader in USDT Perpetual Market
1. 50 limit order per each side from current price (short limit order to upper side / long limit order to down side)
2. If limit order is filled, user gets `0.025 %` from filled order amount.
3. If open positions are off balance (one side being less than 25 %)  :
1) clear all open positions (short, long)
2) start rebalancing to 50% 50%

## Logic
- Added base http api, base websocket API classes
- Subscribed kline data to get current price
- Used `PostOnly` option for placing order to ensure it does not pay any taker fee
- Sends ping every 15 mins for websocket connection
- API Request limit considered, it waits certain amount of time when API call is blocked -> up down 40 recommended (i think 35 is also fair enough)
- Get active orders using API to make sure it recognizes previous orders even before program startup

1. Place limit orders when the current price reaches (start_gap + order_gap), (down - middle - up) side
2. If order is filled or cancelled, it checks current price and then cancel or place short&long orders based on the current price
3. Check open positions to check if the positions are out of balance (less than 25 %)
4. If so, either clear all positions (method 1) or rebalance to 50 % : 50 % (method 2)
- rebalancing method 1: Program stops in case of clearing all positions
- rebalancing method 2: all active orders are canceled when the program starts clearing positions (idk, just.. bybit does that)
