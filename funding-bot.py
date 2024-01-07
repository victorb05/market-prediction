import ccxt
import time
import math
import schedule
import keys
from datetime import datetime, timedelta
from pprint import pprint

TARGET_USD_SIZE = 100  # Total possition size in USD for each trade/ticker
MARGIN_TYPE = 'CROSS' # Cross or Isolated
LEVERAGE = 10

MAX_TICKERS_TO_TRADE = 3
TIMEDELTA_BEFORE_FUNDING_RESET = 15 # Run this script X seconds before funding rate
FUNDING_RESET_TIMEFRAME = 8 # How often funding is being reset (in hours)
POSITION_LIFECYCLE = 60 # Position will be closed after that time (in minutes)
MIN_FUNDING_RATE_TO_SHORT = -1 # Funding rate must be lower than this value

binance = ccxt.binance({
    'apiKey': keys.BINANCE_API_KEY,
    'secret': keys.BINANCE_API_SECRET,   
    'options': {
        'defaultType': 'future'
    }
})

bybit = ccxt.bybit({
    'apiKey': keys.BYBIT_API_KEY,
    'secret': keys.BYBIT_API_SECRET
})

def find_tickers():
    try:
        funding_rates = []

        binance_funding_rates = binance.fetch_funding_rates()
        binance_funding_rates = [entry for entry in binance_funding_rates.items() if float(entry[1]['fundingRate']) < MIN_FUNDING_RATE_TO_SHORT / 100 * 0.8]
        binance_funding_rates = sorted(binance_funding_rates, key=lambda x: float(x[1]['fundingRate']))
        binance_funding_rates = binance_funding_rates[:MAX_TICKERS_TO_TRADE]

        bybit_funding_rates = bybit.fetch_funding_rates()

        for ticker in binance_funding_rates:
            bybit_ticker = next((entry for entry in bybit_funding_rates.items() if entry[0] == ticker[0]), None)
            
            binance_funding_rate = ticker[1]['fundingRate']
            bybitfunding_rate = bybit_ticker[1]['fundingRate']
            
            if (binance_funding_rate + bybitfunding_rate) / 2 < MIN_FUNDING_RATE_TO_SHORT / 100:
                funding_rates.append(ticker)

        return funding_rates
    
    except Exception as e:
        print(f"Error fetching funding rates: {e}")
        return []
    
def submit_batch_orders(orders): 
    orders = [binance.encode_uri_component(binance.json(order), safe=",") for order in orders]
    return binance.fapiPrivatePostBatchOrders({
        'batchOrders': '[' + ','.join(orders) + ']'
    })

def place_market_short_orders(tickers):
    orders = []
    sl_orders = []
    tp_orders = []
    fundingRates = {}

    for ticker in tickers:
        symbol = ticker[1]['info']['symbol']
        price = float(ticker[1]['info']['markPrice'])
        fundingRates[symbol] = float(ticker[1]['fundingRate'])
        market = binance.market(symbol)
        quantity = binance.amount_to_precision(symbol, TARGET_USD_SIZE / price)

        order = {
            'symbol': market['id'],
            'side': 'SELL', 
            'type': 'MARKET',
            'positionSide': 'SHORT',
            'quantity': quantity,
            'newOrderRespType': 'FULL',
            'leverage': str(LEVERAGE),
            'marginType': MARGIN_TYPE.upper()
        }

        orders.append(order)

    response = submit_batch_orders(orders)

    for item in response:
        print (f"Market order has been submited for {item['symbol']} at {convert_to_readable_time(item['updateTime'])} (funding rate is {round(fundingRates[item['symbol']] * 100, 2)}%)")

        info = binance.fetch_order(item['orderId'], item['symbol'])
        
        symbol = info['info']['symbol']
        avgPrice = info['info']['avgPrice']
        executedQty = binance.amount_to_precision(symbol, info['info']['executedQty'])
        stopPrice = binance.price_to_precision(symbol, float(avgPrice) * (1 + (fundingRates[symbol] * -1 * 2)))
        
        print (f"Order for {symbol} has been executed at ${avgPrice}")

        sl_order = {
            'symbol': symbol,
            'side': 'BUY', 
            'type': 'STOP_MARKET',
            'positionSide': 'SHORT',
            'stopPrice': stopPrice,
            'quantity': executedQty,
            'newOrderRespType': 'FULL',
            'leverage': str(LEVERAGE),
            'marginType': MARGIN_TYPE.upper(),
            'timeInForce': 'GTC'
        }

        sl_orders.append(sl_order)

        tp_order = {
            'symbol': symbol,
            'side': 'BUY', 
            'type': 'MARKET',
            'positionSide': 'SHORT',
            'quantity': executedQty,
            'newOrderRespType': 'FULL',
            'leverage': str(LEVERAGE),
            'marginType': MARGIN_TYPE.upper()
        }

        print (f"Stop loss order for {symbol} at ${stopPrice}")
        tp_orders.append(tp_order)

    sl_response = submit_batch_orders(sl_orders)

    sl_orders_ids = {}
    for item in sl_response:
        sl_orders_ids[item['symbol']] = item['orderId']
    
    close_possitions(tp_orders, sl_orders_ids)

def close_possitions(orders, sl_orders_ids):
    time.sleep(POSITION_LIFECYCLE * 60)

    response = submit_batch_orders(orders)
    for o in sl_orders_ids:
        try:
            binance.cancel_order(sl_orders_ids[o], o)
        except Exception as e:
            print (f'Error while canceling stop loss order: {e}')

    for r in response:
        print (f"Possition for {r['symbol']} has been closed")
    
    print ('===========================')

def format_time(time_obj):
    return time_obj.strftime('%H:%M:%S')

def convert_to_readable_time(unix_timestamp):
    time_in_seconds = int(unix_timestamp) / 1000  # Binance server time is in milliseconds
    formatted_time = datetime.utcfromtimestamp(time_in_seconds).strftime('%H:%M:%S')
    return formatted_time

def next_run_hour():
    time = datetime.utcnow()
    next = (1 + (math.floor(time.hour / FUNDING_RESET_TIMEFRAME))) * FUNDING_RESET_TIMEFRAME
    if next >= 24:
        return next - 24
    
    return next

def run():
    print (f'Run at {format_time(datetime.now())}')
    
    tickers = find_tickers()
    if len(tickers) > 0:
        place_market_short_orders(tickers)
    else:
        print(f"No tickers with the funding rate less than {MIN_FUNDING_RATE_TO_SHORT}%")

if __name__ == "__main__":
    binance.load_markets(True)
    
    interval = timedelta(hours=FUNDING_RESET_TIMEFRAME)
    hours_difference = round((datetime.now() - datetime.utcnow()).seconds / 3600)
    first_run_time = datetime.now().replace(hour=next_run_hour() + hours_difference, minute=0, second=0, microsecond=0) - timedelta(seconds=TIMEDELTA_BEFORE_FUNDING_RESET)

    for k in range(math.ceil(24 / FUNDING_RESET_TIMEFRAME)):
        schedule.every().day.at(format_time(first_run_time + k * interval)).do(run)

    while True:
        next_run = schedule.next_run()
        print(f"Next run time: {format_time(next_run)} (local time: {datetime.now().strftime('%H:%M:%S')})")
        
        tickers = find_tickers()

        if len(tickers) > 0:
            for ticker in tickers:
                symbol = ticker[1]['info']['symbol']
                fundingRate = float(ticker[1]['fundingRate'])
                print (f"{symbol} will be shorted on the next funding reset (funding rate is {round(fundingRate * 100, 2)}%)")
        else:
            print(f"No tickers found with the funding rate less than {MIN_FUNDING_RATE_TO_SHORT}%")

        print ('==============')
        schedule.run_pending()
        time.sleep(1)