import datetime
import certifi

import bybit
import time
import threading
import win32api
import ntplib

from abc import abstractmethod
from debugger import debugger


class OrderSide(object):
    BUY = 'Buy'
    SELL = 'Sell'


class OrderType(object):
    LIMIT = 'Limit'
    MARKET = 'Market'


class TimeInForce(object):
    GOOD_TILL_CANCEL = 'GoodTillCancel'
    POST_ONLY = 'PostOnly'


class OrderStatus(object):
    FILLED = 'Filled'
    PARTIALLY_FILLED = 'PartiallyFilled'
    CANCELLED = 'Cancelled' 
    PENDING_CANCEL = 'PendingCancel'
    REJECTED = 'Rejected'
    NEW = 'New'
    CREATED = 'Created'


REQUEST_BLOCK_WAIT_TIME = 3


class BaseBybit(object):
    def __init__(self, api_key, api_secret, is_test=False):
        self.client = None
        self._api_key = api_key
        self._api_secret = api_secret
        self.active_orders_dict = dict()
        self.is_test = is_test
        self.lock = threading.Lock()

        self._get_client()

    def _get_client(self):
        debugger.debug('{}'.format(certifi.where()))
        self.client = bybit.bybit(test=self.is_test, api_key=self._api_key, api_secret=self._api_secret)

    def get_server_time(self):
        """get server time and correct network delay """
        try:
            start_time = time.time()
            server_time = float(self.client.Common.Common_getTime().result()['time_now'])
            delay = time.time() - start_time
            return server_time + delay / 2
        except Exception:
            debugger.debug("Failed to get the bybit server time")
            return None

    def _sync_time(self):
        server_time = self.get_server_time()
        if server_time is None:
            return

        try:
            debugger.info('시스템 시간 동기화를 진행합니다...')
            time_tuple = datetime.datetime.utcfromtimestamp(server_time).timetuple()
            day_of_week = datetime.datetime.utcfromtimestamp(server_time).isocalendar()[2]
            t = time_tuple[:2] + (day_of_week,) + time_tuple[2:-3] + (int(server_time * 1000 % 1000),)
            win32api.SetSystemTime(*t)
            debugger.info('시간동기화 완료. 현재시간:{}'.format(time.time()))
            try:
                c = ntplib.NTPClient()
                response = c.request('time.google.com', version=3)
                debugger.debug('time.google.com: {}'.format(response.tx_time))
            except Exception:
                pass
        except Exception:
            debugger.debug("failed to sync time")

    def update_active_orders(self, symbol):
        with self.lock:
            self.active_orders_dict = self._get_active_orders(symbol)

    @abstractmethod
    def _get_active_orders(self, symbol):
        pass

    def get_all_order_ids(self, symbol, update=True):
        if update:
            self.update_active_orders(symbol)

        if not isinstance(self.active_orders_dict, dict):
            debugger.debug('get_all_order_ids() failed, failed to get `self.active_orders_dict`')
            return None  
        
        order_ids = self.active_orders_dict.keys()
        if order_ids:
            return list(order_ids)
        return list()

    def get_order_history(self, order_id, symbol, update=True):
        if update:
            self.update_active_orders(symbol)

        if not isinstance(self.active_orders_dict, dict):
            debugger.debug('get_order_history() failed, failed to get `self.active_orders_dict`')
            return None  

        order_history = self.active_orders_dict.get(order_id, None)
        if order_history:
            order_side, order_price = order_history
            return order_side, order_price

        debugger.debug('get_order_history() failed, order_id[{}] not exist in `self._order_id_dict`'.format(order_id))
        return None
    
    def get_all_order_history(self, symbol, update=True):
        if update:
            self.update_active_orders(symbol)

        if not isinstance(self.active_orders_dict, dict):
            debugger.debug('get_all_order_history() failed, failed to get `self.active_orders_dict`')
            return None

        return self.active_orders_dict

    def get_active_limit_order_counts(self, symbol, update=True):
        buy_cnt = 0
        sell_cnt = 0

        if update:
            self.update_active_orders(symbol)

        if not isinstance(self.active_orders_dict, dict):
            debugger.debug('get_active_limit_order_counts() failed, failed to get `self.active_orders_dict`')
            return None, None

        for order_side, _ in self.active_orders_dict.values():
            if order_side == OrderSide.BUY:
                buy_cnt += 1
            elif order_side == OrderSide.SELL:
                sell_cnt += 1

        debugger.debug('current limit order counts - long [{}], short [{}]'.format(buy_cnt, sell_cnt))
        return buy_cnt, sell_cnt


class USDTPerpetualAPI(BaseBybit):
    def __init__(self, api_key, api_secret, is_test=False):
        super().__init__(api_key, api_secret, is_test=is_test)

        # for order api (create, cancel, cancel-all) api rate limit is 100 per minute
        self._order_rate_limit_status = None
        self._limit_status_lock = threading.Lock()

    def _set_order_rate_limit_status(self, rate_limit_status):
        with self._limit_status_lock:
            self._order_rate_limit_status = rate_limit_status

    def get_left_api_rate_count(self):
        with self._limit_status_lock:
            left_api_rate_count = self._order_rate_limit_status
            debugger.debug('order api count left - [{}]'.format(left_api_rate_count))
            return left_api_rate_count

    def place_order(
        self, symbol, price,
        qty: float, side: str, order_type: str,
        time_in_force=TimeInForce.POST_ONLY, reduce_only=False, close_on_trigger=False
        ):
        """ 
            - Default order_type is `Limit` order, order_type could be `Limit` or `Market`
            - `price` should be None if order_type is `Market`
            - With `reduce_only` True, it reduces size of the opposite position while increasing input position
        """

        order_side_verbose = '숏 매수' if side == OrderSide.SELL else '롱 매수'
        if reduce_only:
            order_side_verbose = '롱 매도' if side == OrderSide.SELL else '숏 매도'
 
        qty = float('{:.8f}'.format(qty))  # order qty is allowed up to decimal 8 points with no trailing zeros
        if isinstance(price, str):
            price = price.rstrip('0')

        if order_type == OrderType.LIMIT:
            order_type_verbose = '지정가'
            order = self.client.LinearOrder.LinearOrder_new(
                side=side,
                symbol=symbol,
                order_type=order_type,
                price=price,
                qty=qty,
                time_in_force=time_in_force,
                reduce_only=reduce_only,
                close_on_trigger=close_on_trigger
            ).result()[0]
        else:
            time_in_force = TimeInForce.GOOD_TILL_CANCEL
            order_type_verbose = '시장가'
            order = self.client.LinearOrder.LinearOrder_new(
                side=side,
                symbol=symbol,
                order_type=order_type,
                qty=qty,
                time_in_force=time_in_force,
                reduce_only=reduce_only,
                close_on_trigger=close_on_trigger
            ).result()[0]

        result = order.get('result', None)
        error_code = order.get('ret_code', None)
        rate_limit_status = order.get('rate_limit_status', None)

        if rate_limit_status:
            self._set_order_rate_limit_status(rate_limit_status)

        if result or error_code == 0:
            order_id = result['order_id']
            debugger.info('{} {} 주문 성공 - symbol[{}], qty[{}], price[{}], order_id[{}]'.format(
                    order_side_verbose, order_type_verbose, symbol, qty, price, order_id))
            return order_id

        if error_code == 130021:
            debugger.info('주문 실패 - 투자금이 부족합니다, 사이트에서 투자 금액을 확보해주세요')
        elif error_code == 10006:
            debugger.info('주문 실패 - 1분에 요청할 수 있는 주문 횟수를 초과했습니다, 잠시만 기다려주세요')
            time.sleep(REQUEST_BLOCK_WAIT_TIME)
        elif error_code == 35015:
            debugger.info('주문 실패 - 주문할 수 있는 단위가 범위를 벗어났습니다')
        elif error_code == 130125:
            debugger.info('주문 실패 - 현재 포지션이 0 입니다')
        elif error_code == 10002:
            debugger.info('주문 실패 - 서버와 시간오차가 너무 큽니다.')
            self._sync_time()

        debugger.debug('{} {} order failure - symbol[{}], qty[{}], price[{}], reason[{}]'.format(
            side, order_type, symbol, qty, price, order))
        return False

    def cancel_order(self, symbol, order_id):
        """
            if cancel_order returns `None`, that means the order is already executed
            it is different from request failure which returns `False`
        """

        order_history = self.get_order_history(order_id, symbol)
        if not order_history:
            return None
        
        order_side, order_price = order_history

        order = self.client.LinearOrder.LinearOrder_cancel(symbol=symbol, order_id=order_id).result()[0]
        result = order.get('result', None)
        error_code = order.get('ret_code', None)
        rate_limit_status = order.get('rate_limit_status', None)

        if rate_limit_status:
            self._set_order_rate_limit_status(rate_limit_status)

        if result or error_code == 0:
            matched_order_id = result.get('order_id', None)
            if matched_order_id:
                debugger.info('주문취소 성공 - symbol[{}], order_side[{}], order_id[{}], price[{}]'.format(symbol, order_side, order_id, order_price))
                return True
        
        elif error_code == 10006:
            debugger.info('1분에 요청할 수 있는 주문 횟수를 초과했습니다, 잠시만 기다려주세요')
            time.sleep(REQUEST_BLOCK_WAIT_TIME)

        debugger.info('주문취소 실패 - symbol[{}], order_id[{}]'.format(symbol, order_id))
        debugger.debug('failed to cancel order, response - {}'.format(order))

        # max 100 request per 60
        time.sleep(60 / 100)
        return False
    
    def cancel_all_orders(self, symbol):
        order = self.client.LinearOrder.LinearOrder_cancelAll(symbol=symbol).result()[0]
        cancelled_order_list = order.get('result', None)

        error_code = order.get('ret_code', None)
        rate_limit_status = order.get('rate_limit_status', None)

        if rate_limit_status:
            self._set_order_rate_limit_status(rate_limit_status)

        if not cancelled_order_list or error_code != 0:
            debugger.info('모든 주문 일괄취소 실패')
            debugger.debug('failed to cancel all orders, response - {}'.format(order))
            return False

        debugger.info('모든 주문 일괄취소 성공')
        return True
    
    def get_position_sizes(self, symbol):
        position = self.client.LinearPositions.LinearPositions_myPosition(symbol=symbol).result()[0]
        position_list = position.get('result', None)

        if position_list:
            buy_position_size = 0
            sell_position_size = 0

            for position in position_list:
                position_side = position['side']
                position_size = position['size']

                if position_side == OrderSide.BUY:
                    buy_position_size = position_size
                elif position_side == OrderSide.SELL:
                    sell_position_size = position_size
            
            debugger.debug('{} current position sizes - BUY[{}], SELL[{}]'.format(symbol, buy_position_size, sell_position_size))
            return buy_position_size, sell_position_size
        
        debugger.debug('failed to get current position sizes, response - {}'.format(position))

        error_code = position.get('ret_code', None)
        if error_code == 10002:
            self._sync_time()
        return None, None

    def _get_active_orders(self, symbol, verbose=False):
        active_orders = self.client.LinearOrder.LinearOrder_query(symbol=symbol).result()[0]

        active_orders_dict = dict()
        order_details = active_orders.get('result', None)
        if order_details:
            if verbose:
                debugger.debug('got active orders successfully, `order_details` length - [{}]'.format(len(order_details)))
            for order_detail in order_details:
                order_id = order_detail['order_id']
                order_side = order_detail['side']
                price = order_detail['price']
                order_status = order_detail['order_status']
                
                active_order_types = [OrderStatus.NEW, OrderStatus.CREATED, OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING_CANCEL]
                if order_status not in active_order_types:
                    debugger.debug('order status is not in {}, instead, got {}'.format(active_order_types, order_status))
                    continue

                order_history = order_side, price
                active_orders_dict.setdefault(order_id, order_history)
                
        elif order_details is None:
            debugger.debug('failed to get current active orders, response- [{}]'.format(active_orders))
            return False
        # debugger.debug(active_orders_dict)
        return active_orders_dict

    def get_current_price(self, symbol):
        """
            get latest execution price for the symbol (current price)
        """
        data = self.client.LinearMarket.LinearMarket_trading(symbol=symbol, limit='1').result()[0]
        result = data.get('result', None)
        if result:
            current_price = result[0]['price']
            trade_time_ms = result[0]['trade_time_ms']
            trade_time_s = trade_time_ms / 1000

            timestamp = datetime.datetime.fromtimestamp(trade_time_s, datetime.timezone(datetime.timedelta(hours=9)))
            kst_timestamp = datetime.datetime.strftime(timestamp, '%Y-%m-%d %H:%M:%S')

            debugger.debug('Bybit public http api ::: current price - [{}], timestamp - [{}]'.format(current_price, kst_timestamp))
            return current_price
        return None
