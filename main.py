from kiteconnect import KiteConnect
from selenium import webdriver
from pyotp import TOTP
from kiteconnect import KiteTicker
import time
import pandas as pd
import datetime as dt
import logging

# Historical data
duration = 5
f_final = {}
alltoken = []

# Token Mappping
sexchange = {}
symbolToInstrumentMap = {} # Find token from symbol
tokenToInstrumentMap = {} # Find symbol from token

# Order placement
gorderid = {}

def tokenLookup(instrument_df, symbol_list, exchange):
    global sexchange, symbolToInstrumentMap, tokenToInstrumentMap
    token_list = []
    for symbol in symbol_list:
        instrumentToken = instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]
        token_list.append(int(instrumentToken))
        # print(instrumentToken)
        # print(symbol)
        symbolToInstrumentMap[symbol] = str(instrumentToken)

        if exchange == 'CDS':
            sexchange[symbol] = kite.EXCHANGE_CDS
        elif exchange == 'MCX':
            sexchange[symbol] = kite.EXCHANGE_MCX
        elif exchange == 'NSE':
            sexchange[symbol] = kite.EXCHANGE_NSE

        tokenToInstrumentMap[str(instrumentToken)] = symbol
    print(token_list)
    print(symbolToInstrumentMap)
    print(tokenToInstrumentMap)

    return(token_list)

def getInstrumentDataBySymbol(tradingSymbol):
    return symbolToInstrumentMap[tradingSymbol]

def getInstrumentDataByToken(instrumentToken):
    return tokenToInstrumentMap[instrumentToken]

def auto_login():

    token_path = "api_key.txt"
    key_secret = open("api_key.txt", 'r').read().split()
    kite = KiteConnect(api_key=key_secret[0])

    service = webdriver.chrome.service.Service('./chromedriver')
    service.start()
    options = webdriver.ChromeOptions()
    #options.add_argument('--headless')
    options = options.to_capabilities()
    driver = webdriver.Remote(service.service_url, options)
    driver.get(kite.login_url())
    driver.implicitly_wait(10)
    username = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[1]/input')
    password = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input')
    username.send_keys(key_secret[2])
    password.send_keys(key_secret[3])
    driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[4]/button').click()
    pin = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/div/input')
    
    # For Google Authenticator Login
    totp = TOTP(key_secret[4])
    token = totp.now()
    pin.send_keys(token)
    driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[3]/button').click()
    time.sleep(10)

    request_token = driver.current_url.split('request_token')[1][:32]
    with open('request_token.txt', 'w') as the_file:
        the_file.write(request_token)
    driver.quit()

    request_token = open('request_token.txt','r').read()
    key_secret = open('api_key.txt', 'r').read().split()
    kite = KiteConnect(api_key=key_secret[0])
    data = kite.generate_session(request_token=request_token, api_secret=key_secret[1])

    with open('access_token.txt', 'w') as file:
        file.write(data["access_token"])

    print("Auto Login Completed")

def on_ticks(ws, ticks):
    global df
    for tick in ticks:
        instrument = str(tick['instrument_token'])
        timestamp = tick['exchange_timestamp']
        ltp = tick['last_price']
        print(f"{instrument} : {ltp} : {timestamp}")

def on_connect(ws, response):
    ws.subscribe(alltoken)
    ws.set_mode(ws.MODE_FULL, alltoken)

def kitePlaceOrder(name, tradingSymbol, price, qty, direction, exchange, slprice):
    # logging.info("Going to place order %s %f %d %s, tradingSymbol, price, qty, direction")
    # print("Going to place order {0} Qty: {1} Direction: {2}".format(tradingSymbol, qty, direction))
    global status, zorderid, etime
    # print("Order placed for Trading symbol", tradingSymbol)
    print("Kite: Going to place order {0} Qty: {1} Direction: {2}".format(tradingSymbol, qty, direction))

    try:
        orderId = kite.place_order(
            variety = kite.VARIETY_REGULAR,
            exchange=exchange,
            tradingsymbol=tradingSymbol,
            transaction_type=kite.TRANSACTION_TYPE_BUY if direction == "Buy" else kite.TRANSACTION_TYPE_SELL,
            quantity=qty,
            price=price,
            product=kite.PRODUCT_MIS,
            order_type=kite.ORDER_TYPE_MARKET
        )
        print(orderId)
        # logging.info("Order placed successfully, orderId = %s", orderId)
        print("Kite place order {0} Qty: {1} Direction: {2} Successful OrderID: {3}".format(tradingSymbol, qty, direction, orderId))
        return orderId
    
    except Exception as e:
        # logging.info("Order placement failed: %s", e)
        print("Kite Order Failed {0}".format(e))

def checkZerodhaSLOrder(orderid):
    received=kite.orders()
    for individual_order in received:
        if int(individual_order['order_id']) == int(orderid):
            # print("SL Order Details", individual_order)
            status = (individual_order['status'] == "TRIGGER PENDING")
            print("Trigger Pending: Kite Order status for Order ID {0} is {1}".format(orderid, status))
            return status
        
def checkZerodhaOrder(orderid):
    received=kite.orders()
    for individual_order in received:
        if int(individual_order['order_id']) == int(orderid):
            # print("Order Details", individual_order)
            status = (individual_order['status'] == "COMPLETE")
            print("Kite Order status for Order ID {0} is {1}".format(orderid, status))
            return status

def kitePlaceSLOrder(oid, name, tradingSymbol, triggerPrice, qty, direction, exchange):
    # logging.info("Going to place SL order %s %f %d %s, tradingSymbol, triggerPrice, qty, direction")
    global status, zorderid, etime, zid
    flag = 0
    print("Kite: Going to place SL order {0} Qty: {1} Direction: {2}".format(tradingSymbol, qty, direction))
    print("OrderID is {0}", format(oid))
    time.sleep(4)
    sflag = 0
    
    while True:
        if checkZerodhaOrder(oid):
            try:
                orderId = kite.place_order(
                    variety = kite.VARIETY_REGULAR,
                    exchange=exchange,
                    tradingsymbol=tradingSymbol,
                    transaction_type=kite.TRANSACTION_TYPE_BUY if direction == "Buy" else kite.TRANSACTION_TYPE_SELL,
                    quantity=qty,
                    trigger_price=triggerPrice,
                    product=kite.PRODUCT_MIS,
                    order_type=kite.ORDER_TYPE_SLM
                )
                print("SL Order placed successfully {0} with orderId {1}".format(name, orderId))
            
            except Exception as e:
                print("Kite SL Order Failed {0}".format(e))
        
        time.sleep(2)
        sflag = sflag + 1
        if sflag == 2:
            print("Kite Buy/Sell order not placed for order id {0}".format(oid))
            break

def getOHLCdatakite(cdu):
    global f_final
    if cdu == 1:
        interval = 'minute'
    else:
        interval = str(cdu) + 'minute'

    for ticker in alltoken:
        vname = str(ticker) + "_" + str(cdu) + "min"
        col_names = ['timestamp', 'open', 'high', 'low', 'close']
        f_final[str(vname)] = pd.DataFrame(kite.historical_data(ticker, dt.date.today()-dt.timedelta(duration), dt.date.today(),interval), columns = col_names)
        f_final[vname].set_index(['timestamp'], inplace = True)
        print("Name is", vname)
        print(f_final[str(vname)])

if __name__ == "__main__":

    itickers = ["NIFTY 50", "BANK NIFTY"]
    otickers = ["NIFTY23MARFUT", "AXISBANK23MARFUT"]
    mtickers = ["CRUDEOIL27MARFUT"]
    ctickers = ["USDINR27MARFUT"]

    auto_login()
    access_token = open("access_token.txt", "r").read()
    key_secret = open("api_key.txt", 'r').read().split()
    kite = KiteConnect(api_key=key_secret[0])
    # Create Kite trading object
    kite.set_access_token(access_token)
    print("Kite Session Generated!")

    while(True):
        try:
            instrument_dump = kite.instruments("NSE")
            instrument_df = pd.DataFrame(instrument_dump)
            instrument_df.to_csv("NSE_Instruments.csv", index = False)
            tokens_nse = tokenLookup(instrument_df, itickers, "NSE")
            print(tokens_nse)
            break;

        except Exception as e:
            print("NSE Token Dump Error")

    while(True):
        try:
            instrument_dump = kite.instruments("MCX")
            instrument_df = pd.DataFrame(instrument_dump)
            instrument_df.to_csv("MCX_Instruments.csv", index = False)
            tokens_mcx = tokenLookup(instrument_df, itickers, "MCX")
            print(tokens_mcx)
            break;

        except Exception as e:
            print("MCX Token Dump Error")

    while(True):
        try:
            instrument_dump = kite.instruments("CDS")
            instrument_df = pd.DataFrame(instrument_dump)
            instrument_df.to_csv("CDS_Instruments.csv", index = False)
            tokens_cds = tokenLookup(instrument_df, itickers, "CDS")
            print(tokens_cds)
            break;

        except Exception as e:
            print("CDS Token Dump Error")

    while(True):
        try:
            instrument_dump = kite.instruments("NFO")
            instrument_df = pd.DataFrame(instrument_dump)
            instrument_df.to_csv("NFO_Instruments.csv", index = False)
            tokens_nfo = tokenLookup(instrument_df, itickers, "NFO")
            print(tokens_nfo)
            break;

        except Exception as e:
            print("NFO Token Dump Error")

    for name in tokens_nse:
        alltoken.append(name)
    
    for name in tokens_mcx:
        alltoken.append(name)

    for name in tokens_cds:
        alltoken.append(name)

    for name in tokens_nfo:
        alltoken.append(name)

    print("All symbols {0}".format(alltoken))

    for i in [1,2,3]: # 1,2,3 minute interval
        getOHLCdatakite(i)

    for name in alltoken:
        tsymbol = getInstrumentDataByToken(str(name))
        gorderid[name] = kitePlaceOrder(name, tsymbol, 0, 1, "Buy", sexchange[tsymbol], 0)
        time.sleep(1)
        ltp = kite.ltp(str(name))[str(name)]['last_price']
        print("SL is", str(ltp-10))
        kitePlaceSLOrder(gorderid[name], name, tsymbol, ltp-10, 1, "Sell", sexchange[tsymbol])

    kws = KiteTicker(key_secret[0], kite.access_token)
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.connect(threaded=True)