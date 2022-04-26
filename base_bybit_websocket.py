import websocket
import threading
import traceback
from time import sleep
import json
import logging
import urllib
import math
import time
import hmac
import sys
import datetime

from debugger import debugger


# This is a simple adapters for connecting to Bybit's websocket API.
# You could use methods whose names begin with “subscribe”, and get result by "get_data" method.
# All the provided websocket APIs are implemented, includes public and private topic.
# Private methods are only available after authorization, but public methods do not.
# If you have any questions during use, please contact us vim the e-mail "support@bybit.com".


ORDERBOOK_DEPTH = {
            '25': 'orderBookL2_25.{pair}',
            '200': 'orderBook_200.100ms.{pair}'
        }

LATEST_DATA_TOPICS = ['trade', 'candle']

AUTH_AFTER_CONNECTION = True


class BybitWebsocket(object):
    # User can ues MAX_DATA_CAPACITY to control memory usage.
    MAX_DATA_CAPACITY = 200
    PRIVATE_TOPIC = ['position', 'execution', 'order']

    def __init__(self, wsURL, api_key, api_secret):
        """Initialize"""
        self.logger = debugger
        self.logger.debug("Initializing WebSocket.")

        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        self.api_key = api_key
        self.api_secret = api_secret
        self.auth_after_connection = AUTH_AFTER_CONNECTION

        self.data = {}
        self.exited = False
        self.auth = False

        self.authed = threading.Event()

        # We can subscribe right in the connection querystring, so let's build that.
        # Subscribe to all pertinent endpoints
        self.logger.info("Connecting to %s" % wsURL)
        self.__connect(wsURL)

    def exit(self):
        """Call this to exit - will close websocket."""
        self.exited = True
        self.ws.close()

    def __make_auth_url(self, ws_url):
        # Generate expires.
        expires = int((time.time() + 10) * 1000)

        # Generate signature.
        signature = str(hmac.new(
            bytes(self.api_secret, "utf-8"),
            bytes("GET/realtime{}".format(expires), "utf-8"), digestmod="sha256"
        ).hexdigest())

        param = "api_key={api_key}&expires={expires}&signature={signature}".format(
            api_key=self.api_key,
            expires=expires,
            signature=signature
        )

        url = ws_url + "?" + param
        return url

    def __connect(self, ws_url):
        """Connect to the websocket in a thread."""
        self.logger.debug("Starting thread")

        if not self.auth_after_connection:
            if self.api_key and self.api_secret:
                ws_url = self.__make_auth_url(ws_url)

        self.ws = websocket.WebSocketApp(ws_url,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         keep_running=True)

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        self.logger.debug("Started thread")

        # Wait for connect before continuing
        retry_times = 50
        self.logger.debug("waiting for connection... auth:{}".format(self.auth))
        while (not self.ws.sock or not self.ws.sock.connected) and retry_times:
            sleep(0.1)
            retry_times -= 1
        if retry_times == 0 and (not self.ws.sock or not self.ws.sock.connected):
            self.logger.error("Couldn't connect to WebSocket! Exiting.")
            self.logger.debug('sock: {}'.format(self.ws.sock))
            self.exit()
            raise websocket.WebSocketTimeoutException('Error！Couldn not connect to WebSocket!.')
        self.logger.debug("Connected! auth:{}".format(self.auth))

        if self.auth_after_connection:
            if self.api_key and self.api_secret:
                self.__do_auth()

    def generate_signature(self, expires):
        """Generate a request signature."""
        _val = 'GET/realtime' + expires
        return str(hmac.new(bytes(self.api_secret, "utf-8"), bytes(_val, "utf-8"), digestmod="sha256").hexdigest())

    def __do_auth(self):
        """
            auth after websocket
        """
        debugger.debug("doing auth...")

        # originally it was 1 sec, but changed to 10 due to "request expired" error
        expires = str(int(time.time() + 10)) + "000"
        signature = self.generate_signature(expires)

        auth = {"op": "auth", "args": [self.api_key, expires, signature]}
        args = json.dumps(auth)

        self.ws.send(args)
        debugger.debug("auth sent...")

    def __on_message(self, message):
        """Handler for parsing WS messages."""
        message = json.loads(message)
        if 'success' in message and message["success"]:
            if 'request' in message and message["request"]["op"] == 'auth':
                self.auth = True
                self.authed.set()
                self.logger.info("Authentication success.")
            if 'ret_msg' in message and message["ret_msg"] == 'pong':
                self.data["pong"].append("PING success")
                self.logger.debug("received pong")

        if 'topic' in message:
            # latest specific data is updated to a var
            topic_name = message['topic'].split('.')[0]
            if topic_name in LATEST_DATA_TOPICS:
                self.data[message['topic']] = message['data'][0]
            else:
                self.data[message["topic"]].append(message["data"])
                if len(self.data[message["topic"]]) > BybitWebsocket.MAX_DATA_CAPACITY:
                    self.data[message["topic"]] = self.data[message["topic"]][BybitWebsocket.MAX_DATA_CAPACITY//2:]

    def __on_error(self, error):
        """Called on fatal websocket errors. We exit on these."""
        if not self.exited:
            self.logger.exception("Error : %s. auth: {}".format(self.auth) % error)
            # raise websocket.WebSocketException(error)

    def __on_open(self):
        """Called when the WS opens."""
        self.logger.debug("Websocket Opened.")

    def __on_close(self):
        """Called on websocket close."""
        self.logger.info('Websocket Closed. auth:{}'.format(self.auth))

    def ping(self):
        self.ws.send('{"op":"ping"}')
        debugger.debug('sent ping')
        if 'pong' not in self.data:
            self.data['pong'] = []

    def subscribe_kline(self, symbol: str, interval: str):
        topic = 'candle.{interval}.{symbol}'.format(interval=interval, symbol=symbol)

        param = dict()
        param['op'] = 'subscribe'
        param['args'] = [topic]
        self.ws.send(json.dumps(param))
        if topic not in self.data:
            self.data[topic] = dict()

    def subscribe_trade(self, symbol):
        topic = 'trade.{symbol}'.format(symbol=symbol)
        stream = dict(op='subscribe', args=[topic])
        json_stream = json.dumps(stream)
        self.ws.send(json_stream)
        if topic not in self.data:
            self.data[topic] = dict()

    def subscribe_insurance(self):
        self.ws.send('{"op":"subscribe","args":["insurance"]}')
        if 'insurance.BTC' not in self.data:
            self.data['insurance.BTC'] = []
            self.data['insurance.XRP'] = []
            self.data['insurance.EOS'] = []
            self.data['insurance.ETH'] = []

    def subscribe_orderBookL2(self, symbol, depth='25'):
        if depth == '25':
            topic = 'orderBookL2_25.{symbol}'
        elif depth == '200':
            topic = 'orderBook_200.100ms.{symbol}'

        param = {}
        param['op'] = 'subscribe'
        param['args'] = [topic.format(symbol=symbol)]
        self.ws.send(json.dumps(param))
        if topic.format(symbol=symbol) not in self.data:
            self.data[topic.format(symbol=symbol)] = []
    
    def subscribe_instrument_info(self, symbol):
        param = {}
        param['op'] = 'subscribe'
        param['args'] = ['instrument_info.100ms.' + symbol]
        self.ws.send(json.dumps(param))
        if 'instrument_info.100ms.' + symbol not in self.data:
            self.data['instrument_info.100ms.' + symbol] = []

    def subscribe_position(self):
        self.ws.send('{"op":"subscribe","args":["position"]}')
        if 'position' not in self.data:
            self.data['position'] = []

    def subscribe_execution(self):
        self.ws.send('{"op":"subscribe","args":["execution"]}')
        if 'execution' not in self.data:
            self.data['execution'] = []

    def subscribe_order(self):
        self.authed.wait(300)
        self.ws.send('{"op":"subscribe","args":["order"]}')
        if 'order' not in self.data:
            self.data['order'] = []
    
    def subscribe_orderbook(self, symbol, depth='25'):
        if depth not in ORDERBOOK_DEPTH.keys():
            self.logger.debug('Value Error, expected between `25` or `200` : got [{}]'.format(depth))
            raise ValueError

        if depth == '25':
            self.subscribe_orderBookL2(symbol=symbol)
        elif depth == '200':
            self.subscribe_orderBookL2(symbol=symbol, depth='200')

    def _get_data(self, topic):
        try:
            if isinstance(self.data[topic], list):
                if topic not in self.data:
                    self.logger.info(" The topic %s is not subscribed." % topic)
                    return list()
                if topic.split('.')[0] in BybitWebsocket.PRIVATE_TOPIC and not self.auth:
                    self.logger.error("Authentication failed. Please check your api_key and api_secret. Topic: %s" % topic)
                    return list()
                else:
                    if len(self.data[topic]) == 0:
                        # self.logger.info(" The topic %s is empty." % topic)
                        return []
                    # while len(self.data[topic]) == 0 :
                    #     sleep(0.1)
                    return self.data[topic].pop(0)  # FIFO

            elif isinstance(self.data[topic], dict):
                if topic not in self.data:
                    self.logger.info(" The topic %s is not subscribed." % topic)
                    return dict()
                if not self.data[topic]:
                    return dict()
                return self.data[topic]
        except Exception:
            debugger.exception("FATAL: unknown error: _get_data()")
            return list()

    def get_orderbook(self, symbol, depth='25'):
        if depth not in ORDERBOOK_DEPTH.keys():
            self.logger.debug('Value Error, expected between `25` or `200` : got [{}]'.format(depth))
            raise ValueError

        topic = ORDERBOOK_DEPTH[depth].format(symbol=symbol)
        data = self._get_data(topic)
        return data
    
    def get_kline(self, symbol, interval):
        topic = 'candle.{interval}.{symbol}'.format(interval=interval, symbol=symbol)
        data = self._get_data(topic)
        return data

    def get_trade(self, symbol):
        topic = 'trade.{symbol}'.format(symbol=symbol)
        data = self._get_data(topic)
        return data

    def get_current_price_from_kline(self, symbol, interval):
        data = self.get_kline(symbol, interval)
        if data:
            close_price = data[0]['close']
            timestamp_e6 = data[0]['timestamp']
            timestamp_s = timestamp_e6 / (1000 * 1000)
            timestamp = datetime.datetime.fromtimestamp(timestamp_s, datetime.timezone(datetime.timedelta(hours=9)))

            kst_timestamp = datetime.datetime.strftime(timestamp, '%Y-%m-%d %H:%M:%S')
            debugger.debug('Bybit ws public api ::: kline current price - [{}], timestamp - [{}]'.format(close_price, kst_timestamp))
            return close_price
        return None

    def get_current_price_from_trade(self, symbol):
        data = self.get_trade(symbol)
        if data:
            trade_price = float(data['price'])
            trade_time_ms = int(data['trade_time_ms']) / 1000
            timestamp_kst = datetime.datetime.fromtimestamp(trade_time_ms, datetime.timezone(datetime.timedelta(hours=9)))

            debugger.debug(
                'Bybit ws public api ::: trade current price - [{}], timestamp - [{}]'.format(trade_price, timestamp_kst))
            return trade_price
        return None

    def get_instrument_info(self, symbol):
        topic = 'instrument_info.100ms.{symbol}'.format(symbol=symbol)
        data = self._get_data(topic)
        return data

    def get_last_price(self, symbol):
        data = self.get_instrument_info(symbol)
        if data:
            last_price = data.get('last_price', None)
            if not last_price:
                update = data['update'][0]
                last_price = update.get('last_price', None)
                if last_price:
                    last_price = float(last_price)
            return last_price
        return None

    def get_order_status(self):
        try:
            topic = 'order'
            data = self._get_data(topic)
            return data
        except Exception:
            debugger.exception("fatal")
            return []
