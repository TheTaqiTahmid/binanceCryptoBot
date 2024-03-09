#!/usr/bin/python3
# Install "binance-futures-connector" python module using "pip install binance-futures-connector" for windows or "pip3 install binance-futures-connector" for linux before running the script. 
from binance.um_futures import UMFutures
import logging
from logging import FileHandler, Formatter, StreamHandler
import time
from multiprocessing.pool import ThreadPool
import pandas as pd
import datetime
import os.path
from binance.error import ClientError
import statistics
import sys

LOG_FORMAT = ("%(asctime)s [%(levelname)s]: %(message)s in %(pathname)s:%(lineno)d")
logging.getLogger().setLevel(logging.WARN)
logging.getLogger().addHandler(FileHandler("warning.log", mode='w'))

binance_logger = logging.getLogger("binance_logger")
binance_logger.setLevel(logging.INFO)
binance_logger_file_handler = FileHandler("debug.log", mode='w')
binance_logger_file_handler.setLevel(logging.INFO)
binance_logger_file_handler.setFormatter(Formatter(LOG_FORMAT))
binance_logger.addHandler(binance_logger_file_handler)
binance_logger_stream_handler = StreamHandler()
binance_logger_stream_handler.setLevel(logging.INFO)
binance_logger_stream_handler.setFormatter(Formatter(LOG_FORMAT))
binance_logger.addHandler(binance_logger_stream_handler)

api_key = ""
api_secret = ""

df_file_path = './'

coin_list = [   
            "DOGEUSDT",
            ]

um_futures_client = UMFutures(key=api_key, secret=api_secret) #base_url="https://testnet.binancefuture.com") 


def get_precision(symbol:str, um_futures_client:str) -> str:
   info = um_futures_client.exchange_info()
   for x in info['symbols']:
    if x['symbol'] == symbol:
        return x['quantityPrecision']
    

# this 'll get the stored csv file df or create a new one
def getOrCreateDf(filename):
    if os.path.exists(df_file_path+filename):
        return pd.read_csv(df_file_path+filename)
    else:
        df = pd.DataFrame(columns=["Symbol","DT","Capital after Trade_Primary",\
                                   "Capital after Trade_Secondary","Pnl till Now",\
                                   "Brokerage till Now", "Investment Value", \
                                   "Strategical SL / TG Status"])
        df.to_csv(df_file_path+filename,index=False)
        return df

def getOrCreateDf_open_posiiton(filename):
    if os.path.exists(df_file_path+filename):
        return pd.read_csv(df_file_path+filename)
    else:
        df = pd.DataFrame(columns=["Symbol","DT","primary_sl_order_id", \
                                   "primary_tp_order_id", \
                                    "secondary_sl_order_id", \
                                    "secondary_tp_order_id"])
        df.to_csv(df_file_path+filename,index=False)
        return df

def get_Compounded_Cap_buy(symbol:str) -> float:
    df = getOrCreateDf('Trade_Detail.csv')
    df = df.loc[df['Symbol'] == symbol]
    if len(df):
        df.sort_values(by=['DT'], inplace=True)
        row = df.iloc[-1]
        return float(row["Capital after Trade_Primary"])
    return 0

def get_Compounded_Cap_sell(symbol:str) -> float:
    df = getOrCreateDf('Trade_Detail.csv')
    df = df.loc[df['Symbol'] == symbol]
    if len(df):
        df.sort_values(by=['DT'], inplace=True)
        row = df.iloc[-1]
        return float(row["Capital after Trade_Secondary"])
    return 0

def get_PNL_till_now(symbol:str) -> float:
    df = getOrCreateDf('Trade_Detail.csv')
    df = df.loc[df['Symbol'] == symbol]
    if len(df):
        df.sort_values(by=['DT'], inplace=True)
        row = df.iloc[-1]
        return float(row["Pnl till Now"])
    return 0

def get_brokerage_till_now(symbol:str) -> float:
    df = getOrCreateDf('Trade_Detail.csv')
    df = df.loc[df['Symbol'] == symbol]
    if len(df):
        df.sort_values(by=['DT'], inplace=True)
        row = df.iloc[-1]
        return float(row["Brokerage till Now"])
    return 0

def get_investment(symbol:str, TP:int, percentage:float) -> float:
    df = getOrCreateDf('Trade_Detail.csv')
    df = df.loc[df['Symbol'] == symbol]
    if len(df):
        df.sort_values(by=['DT'], inplace=True)
        row = df.iloc[-1]
        return float(row["Investment Value"])
    return TP * percentage   

def get_flag(symbol:str) -> int:
    df = getOrCreateDf('Trade_Detail.csv')
    df = df.loc[df['Symbol'] == symbol]
    if len(df):
        df.sort_values(by=['DT'], inplace=True)
        row = df.iloc[-1]
        return int(row["Strategical SL / TG Status"])
    return 0

def get_order_id(symbol:str) -> str:
    df = getOrCreateDf_open_posiiton('open_position_order_id.csv')
    df = df.loc[df['Symbol'] == symbol]
    if len(df):
        df.sort_values(by=['DT'], inplace=True)
        row = df.iloc[-1]
        return (row["primary_sl_order_id"], row["primary_tp_order_id"], row["secondary_sl_order_id"], row["secondary_tp_order_id"])
    return ("", "", "", "")    

def update_file(symbol, dt, Cbuy, Csell, PNL_till_now, brokerage_till_now, I, flag):
    df = getOrCreateDf('Trade_Detail.csv')
    df = df.append(
        {
            'Symbol' : symbol, 
            'DT' : dt, 
            'Capital after Trade_Primary': Cbuy,
            'Capital after Trade_Secondary': Csell,
            'Pnl till Now':PNL_till_now, 
            'Brokerage till Now':brokerage_till_now,
            "Investment Value":I,
            'Strategical SL / TG Status':flag
        },
        ignore_index = True
    )
    df.to_csv('Trade_Detail.csv', index=False )

def update_open_position(symbol, dt, primary_sl, primary_tp, secondary_sl, secondary_tp):
    df = getOrCreateDf_open_posiiton("open_position_order_id.csv")
    df = df.append(
        {
            'Symbol' : symbol, 
            'DT' : dt, 
            "primary_sl_order_id": primary_sl,
            'primary_tp_order_id': primary_tp,
            'secondary_sl_order_id':secondary_sl, 
            'secondary_tp_order_id':secondary_tp,
        },
        ignore_index = True
    )
    df.to_csv('open_position_order_id.csv', index=False )    

def presize(num:float, precision:int):
    return float(round(num,precision))

def presize_price(price:float):
    if price >= 10:
        return presize(price, 2)
    elif price >= 1:    
        return presize(price, 3)
    else:
        return presize(price, 4)

def calculate_price_pnl_brokerage(orders:list, quantity:float, positionside:str, Brokerage_fee:float):
    orders.reverse()
    qty = 0
    pnl_fetched = []
    brokerage_fetched = []
    price_fetched = []
    for order in orders:
        if order["positionSide"] == positionside:
            price = float(order["price"])
            price_fetched.append(price)
            pnl_prim_market = float(order["realizedPnl"])
            brokerage_primary = (float(order["price"]) * float(order["qty"]) * Brokerage_fee)
            pnl_fetched.append(pnl_prim_market)
            brokerage_fetched.append(brokerage_primary)
            qty += float(order["qty"])
            if qty >= quantity:
                break
    return (statistics.fmean(price_fetched), sum(pnl_fetched), sum(brokerage_fetched))

def run_crpyto_bot(symbol:str) -> None:
    TP = 50                   # Take profit mark
    investment_percentage = 0.08               # Initial Investment put, should be 20% max. of TP
    L = 20                      # Leverage to choose
    Str_TG = 0.75               # Target on Investment according to the strategy
    Str_SL = -0.15              # Stop Loss on Investment according to the strategy
    Brokerage_fee = 0.0004
    Tg = 0.011                 # Target Level               
    Sl = -0.011                # Stop Loss Level
    precision = get_precision(symbol, um_futures_client)
    print("precision:",precision)
    count = 0
    brokerage_order_open = 0 # brokerage value for opening new orders. This value will be overriden when new order is placed. 
    flag = 0
    primary_entry_price, secondary_entry_price = 0, 0
    quantity_primary, quantity_secondary = 0, 0
    primary_sl_order_id, primary_tp_order_id, secondary_sl_order_id, secondary_tp_order_id = "", "", "", ""
    
    while True:        
        print("running", symbol)
        Compounded_Cap_buy = get_Compounded_Cap_buy(symbol) # The last Cbuy value from csv file
        Compounded_Cap_sell = get_Compounded_Cap_sell(symbol) # The last Csell value from csv file
        PNL_till_now = get_PNL_till_now(symbol)
        brokerage_till_now = get_brokerage_till_now(symbol)
        I = get_investment(symbol, TP, investment_percentage)
        
        if Compounded_Cap_buy == 0:
            Cbuy = round(I/2, 0) 
        else:
            Cbuy = Compounded_Cap_buy
        
        if Compounded_Cap_sell == 0:
            Csell = round(I/2, 0)
        else:
            Csell = Compounded_Cap_sell

        price_fetched = float(um_futures_client.ticker_price(symbol)["price"])
        Qbuy = presize(Cbuy * L / price_fetched, precision)
        Qsell = presize(Csell * L / price_fetched, precision)

        TG_buy = presize_price(price_fetched * (1 + Tg))  
        SL_buy = presize_price(price_fetched * (1 + Sl))

        TG_sell   =   presize_price(price_fetched * (1 - Tg)) 
        SL_sell    =   presize_price(price_fetched * (1 - Sl))
        primary_sl_order_id, primary_tp_order_id, secondary_sl_order_id, secondary_tp_order_id = get_order_id(symbol=symbol)
        CMP = price_fetched
        # print(f"Cbuy: {Cbuy}, CMP: {CMP}, primary entry price: {primary_entry_price}, quantity_primary: {quantity_primary}")
        
        # place orders  
        check_if_order_present = um_futures_client.get_position_risk(symbol=symbol)
        order_long = 0
        order_short = 0
        for order in check_if_order_present:
            if order["positionSide"] == "LONG":
                order_long = order["positionAmt"]
                price_long = order["entryPrice"]
            elif order["positionSide"] == "SHORT":
                order_short = str(abs(float(order["positionAmt"])))
                price_short = order["entryPrice"]
        
        # These below two if conditions will fetch the current price and order quantity if script is restarted
        if quantity_primary == 0:
            quantity_primary = float(order_long)
            primary_entry_price = float(price_long)
        if quantity_secondary == 0:
            quantity_secondary = float(order_short)
            secondary_entry_price = float(price_short)
        
        primary_asset = Cbuy + ((CMP - primary_entry_price) * quantity_primary)
        secondary_asset = Csell + ((secondary_entry_price - CMP) * quantity_secondary)
        print(f"primary asset: {primary_asset} and secondary asset: {secondary_asset}")
        running_asset = primary_asset + secondary_asset
        binance_logger.info(f"Current running asset for {symbol}: {running_asset}")

        # This if block is to monitor the strategic target and loss values:
        if flag == 0 and running_asset >= I * (1 + Str_TG):
            if count >= 1:
                binance_logger.warning(" Strategic target profit has ben achieved, closing positions...")
                primary_close_params = {
                        'symbol': symbol,
                        'side': 'SELL',
                        'positionSIDE': 'LONG',
                        'type': 'MARKET',
                        'quantity': order_long
                }
                secondary_close_params = {
                        'symbol': symbol,
                        'side': 'BUY',
                        'positionSIDE': 'SHORT',
                        'type': 'MARKET',
                        'quantity': order_short
                }
                primary_close_position = um_futures_client.new_order(**primary_close_params)
                secondary_close_position = um_futures_client.new_order(**secondary_close_params)
                binance_logger.info(primary_close_position)
                binance_logger.info(secondary_close_position)
                time.sleep(1)
                trade_orders = um_futures_client.get_account_trades(symbol=symbol, limit=10, incomeType='REALIZED_PNL')
                price_prim_market = calculate_price_pnl_brokerage(trade_orders, quantity_primary, "LONG", Brokerage_fee)[0]
                price_sec_market  = calculate_price_pnl_brokerage(trade_orders, quantity_secondary, "SHORT", Brokerage_fee)[0]
                PNL_till_now += PNL_till_now + (I * Str_TG)
                brokerage_prim_market = price_prim_market * quantity_primary * Brokerage_fee
                brokerage_sec_market = price_sec_market * quantity_secondary * Brokerage_fee  
                brokerage_till_now += brokerage_prim_market + brokerage_sec_market
                I = I * (1 + Str_TG) - brokerage_till_now
                Cbuy = round(I/2, 2)
                Csell = round(I/2, 2)
                flag = 1
                # Brokerage value will reset to zero after strategic hit.
                update_file(symbol=symbol, dt=datetime.datetime.now(), Cbuy=Cbuy, Csell=Csell, \
                            PNL_till_now=PNL_till_now, brokerage_till_now=0, I=I, flag=1)
                time.sleep(2)
                
        elif flag == 0 and running_asset <= I * (1 + Str_SL):
            if count >= 1:
                binance_logger.warning(" Strategic stop loss ben achieved, closing positions...")
                primary_close_params = {
                        'symbol': symbol,
                        'side': 'SELL',
                        'positionSIDE': 'LONG',
                        'type': 'MARKET',
                        'quantity': order_long
                }
                secondary_close_params = {
                        'symbol': symbol,
                        'side': 'BUY',
                        'positionSIDE': 'SHORT',
                        'type': 'MARKET',
                        'quantity': order_short
                }
                primary_close_position = um_futures_client.new_order(**primary_close_params)
                secondary_close_position = um_futures_client.new_order(**secondary_close_params)
                binance_logger.info(primary_close_position)
                binance_logger.info(secondary_close_position)
                time.sleep(1)
                trade_orders = um_futures_client.get_account_trades(symbol=symbol, limit=10, incomeType='REALIZED_PNL')
                price_prim_market = calculate_price_pnl_brokerage(trade_orders, quantity_primary, "LONG", Brokerage_fee)[0]
                price_sec_market  = calculate_price_pnl_brokerage(trade_orders, quantity_secondary, "SHORT", Brokerage_fee)[0]
                PNL_till_now += PNL_till_now + (I * Str_SL)
                brokerage_prim_market = price_prim_market * quantity_primary * Brokerage_fee
                brokerage_sec_market = price_sec_market * quantity_secondary * Brokerage_fee  
                brokerage_till_now += brokerage_prim_market + brokerage_sec_market
                I = max((TP * investment_percentage)/2, I*(1 + Str_SL)) - brokerage_till_now
                Cbuy = round(I/2, 2)
                Csell = round(I/2, 2)
                flag = 1
                # Brokerage value will reset to zero after strategic hit.
                update_file(symbol=symbol, dt=datetime.datetime.now(), Cbuy=Cbuy, Csell=Csell, \
                            PNL_till_now=PNL_till_now, brokerage_till_now=0, I=I, flag=1)
                time.sleep(2)

        # This elif block is to monitor normal TP and SL hit and placing new orders
        if float(order_long) == 0 and float(order_short) == 0:
            time.sleep(2)
            # This count is to distinguish first order from the rest of the future orders
            if count >= 1 and flag == 0:
                primary_sl_order_status = um_futures_client.query_order(symbol=symbol, orderId=primary_sl_order_id)
                primary_tp_order_status = um_futures_client.query_order(symbol=symbol, orderId=primary_tp_order_id)
                secondary_sl_order_status = um_futures_client.query_order(symbol=symbol, orderId=secondary_sl_order_id)
                secondary_tp_order_status = um_futures_client.query_order(symbol=symbol, orderId=secondary_tp_order_id)
                
                if primary_tp_order_status["status"] == "FILLED":
                    price_prim_close = float(primary_tp_order_status["avgPrice"])
                    price_sec_close = float(secondary_sl_order_status["avgPrice"])
                    PNL_till_now += (Cbuy * Tg * L) + (Csell * Sl * L)  
                    Cbuy = Cbuy * (1 + (Tg * L))
                    Csell = Csell * (1 + (Sl * L))
                    Qbuy = presize(Cbuy * L / price_fetched, precision)
                    Qsell = presize(Csell * L / price_fetched, precision)
                    brokerage_prim_close = price_prim_close * quantity_primary * Brokerage_fee
                    brokerage_sec_close = price_sec_close * quantity_secondary * Brokerage_fee
                    brokerage_till_now += brokerage_prim_close + brokerage_sec_close + brokerage_order_open
                    update_file(symbol=symbol, dt=datetime.datetime.now(), Cbuy=Cbuy, Csell=Csell, \
                            PNL_till_now=PNL_till_now, brokerage_till_now=brokerage_till_now, I=I, flag=0)
                    time.sleep(1)

                elif primary_sl_order_status["status"] == "FILLED":
                    price_prim_close = float(primary_sl_order_status["avgPrice"])
                    price_sec_close = float(secondary_tp_order_status["avgPrice"])
                    PNL_till_now += (Cbuy * Sl * L) + (Csell * Tg * L)  
                    Cbuy = Cbuy * (1 + (Sl * L))
                    Csell = Csell * (1 + (Tg * L))
                    Qbuy = presize(Cbuy * L / price_fetched, precision)
                    Qsell = presize(Csell * L / price_fetched, precision)
                    brokerage_prim_close = price_prim_close * quantity_primary * Brokerage_fee
                    brokerage_sec_close = price_sec_close * quantity_secondary * Brokerage_fee
                    brokerage_till_now += brokerage_prim_close + brokerage_sec_close + brokerage_order_open
                    update_file(symbol=symbol, dt=datetime.datetime.now(), Cbuy=Cbuy, Csell=Csell, \
                            PNL_till_now=PNL_till_now, brokerage_till_now=brokerage_till_now, I=I, flag=0)
                    time.sleep(1)
                
                else:
                    orders = um_futures_client.get_account_trades(symbol=symbol, limit=10, incomeType='REALIZED_PNL')
                    price_prim_close = calculate_price_pnl_brokerage(orders, quantity_primary, "LONG", Brokerage_fee)[0]
                    price_sec_close  = calculate_price_pnl_brokerage(orders, quantity_secondary, "SHORT", Brokerage_fee)[0]
                    brokerage_prim_close = price_prim_close * quantity_primary * Brokerage_fee
                    brokerage_sec_close = price_sec_close * quantity_secondary * Brokerage_fee
                    brokerage_till_now += brokerage_prim_close + brokerage_sec_close + brokerage_order_open
                    update_file(symbol=symbol, dt=datetime.datetime.now(), Cbuy=Cbuy, Csell=Csell, \
                            PNL_till_now=PNL_till_now, brokerage_till_now=brokerage_till_now, I=I, flag=0)
                    time.sleep(1)

            count += 1
            print("placing orders")
            leverage = um_futures_client.change_leverage(symbol=symbol, leverage=L)
            # primary orders            
            primary_params = [
                # buy
                {
                    'symbol': symbol,
                    'side': 'BUY',
                    'positionSIDE': 'LONG',
                    'type': 'MARKET',
                    'quantity': Qbuy,
                },
                # sl
                {
                    'symbol': symbol,
                    'side': 'SELL',
                    'positionSIDE': 'LONG',
                    'type': 'STOP_MARKET',
                    'timeInForce': 'GTE_GTC',
                    'quantity': Qbuy,
                    'stopPrice': SL_buy,
                    'workingType': 'MARK_PRICE',
                    'closePosition': True
                },
                # tp
                {
                    'symbol': symbol,
                    'side': 'SELL',
                    'positionSIDE': 'LONG',
                    'type': 'TAKE_PROFIT_MARKET',
                    'timeInForce': 'GTE_GTC',
                    'quantity': Qbuy,
                    'stopPrice': TG_buy,
                    'workingType': 'MARK_PRICE',
                    'closePosition': True
                },
                
            ]
            secondary_params = [
                # buy
                {
                    'symbol': symbol,
                    'side': 'SELL',
                    'positionSIDE': 'SHORT',
                    'type': 'MARKET',
                    'quantity': Qsell,
                },
                # sl
                {
                    'symbol': symbol,
                    'side': 'BUY',
                    'positionSIDE': 'SHORT',
                    'type': 'STOP_MARKET',
                    'timeInForce': 'GTE_GTC',
                    'quantity': Qsell,
                    'stopPrice': SL_sell,
                    'workingType': 'MARK_PRICE',
                    'closePosition': True
                },
                # tp
                {
                    'symbol': symbol,
                    'side': 'BUY',
                    'positionSIDE': 'SHORT',
                    'type': 'TAKE_PROFIT_MARKET',
                    'timeInForce': 'GTE_GTC',
                    'quantity': Qsell,
                    'stopPrice': TG_sell,
                    'workingType': 'MARK_PRICE',
                    'closePosition': True
                },
                
            ]
            try:
                binance_logger.info("placing p order market buy", primary_params[0])
                primary_mark_response = um_futures_client.new_order(**primary_params[0])
                binance_logger.info(primary_mark_response)
                binance_logger.info("placing p order buy sl", primary_params[1])
                primary_sl_response = um_futures_client.new_order(**primary_params[1])
                primary_sl_order_id = primary_sl_response["orderId"]
                binance_logger.info(primary_sl_response)
                binance_logger.info("placing p order buy tp", primary_params[2])
                primary_tp_response = um_futures_client.new_order(**primary_params[2])
                primary_tp_order_id = primary_tp_response["orderId"]
                binance_logger.info(primary_tp_response)
                binance_logger.info("placing s order market buy", secondary_params[0])
                secondary_mark_response = um_futures_client.new_order(**secondary_params[0])
                binance_logger.info(secondary_mark_response)
                binance_logger.info("placing s order buy sp", secondary_params[1])
                secondary_sl_response = um_futures_client.new_order(**secondary_params[1])
                secondary_sl_order_id = secondary_sl_response["orderId"]
                binance_logger.info(secondary_sl_response)
                binance_logger.info("placing s order buy tp", secondary_params[2])
                secondary_tp_response = um_futures_client.new_order(**secondary_params[2])
                secondary_tp_order_id = secondary_tp_response["orderId"]
                binance_logger.info(secondary_tp_response)
                update_open_position(symbol=symbol, dt=datetime.datetime.now(), \
                                    primary_sl=primary_sl_order_id, \
                                    primary_tp=primary_tp_order_id, \
                                    secondary_sl=secondary_sl_order_id, \
                                    secondary_tp=secondary_tp_order_id
                                    )

                entry_point = um_futures_client.get_position_risk(symbol=symbol)
                for entry in entry_point:
                    if entry["positionSide"] == "LONG":
                        primary_entry_price = float(entry['entryPrice'])
                        print(f"primary entry price: {primary_entry_price}")
                    elif entry["positionSide"] == "SHORT":
                        secondary_entry_price = float(entry['entryPrice'])
                        print(f"secondary entry price: {secondary_entry_price}")
                brokerage_prim_open = (primary_entry_price * Qbuy * Brokerage_fee)
                brokerage_sec_open = (secondary_entry_price * Qsell * Brokerage_fee)
                brokerage_order_open = brokerage_prim_open + brokerage_sec_open
                flag = 0
                quantity_primary = Qbuy # Primary buy order quantity
                print(f"quantity primary: {quantity_primary}")
                quantity_secondary = Qsell # Secondary buy order quantity
                print(f"quantity secondary: {quantity_secondary}")
            except ClientError as error:
                binance_logger.error(
                    "Found error. status: {}, error code: {}, error message: {}".format(
                        error.status_code, error.error_code, error.error_message
                    )
                )
                binance_logger.error(error)
                sys.exit()
        
        # This else block is to update the running position status
        else:
            count += 1
            binance_logger.info(f"Current position info on {symbol}: order_long: {order_long} order_short: {order_short} price_fetched: {price_fetched}")
        time.sleep(7)            

def main():
    with ThreadPool(processes= len(coin_list) + 1) as pool:
        for coin in coin_list:
            res = pool.apply_async(run_crpyto_bot,args=(coin, ))
            print(res.get())
        pool.close()
        pool.join()

if __name__ == "__main__":
    main()



