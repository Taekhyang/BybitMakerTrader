import configparser
import functools
import queue
import threading
import certifi
import shutil
import multiprocessing
import os
import sys
import time


from concurrent import futures

from base_bybit_websocket import BybitWebsocket
from base_bybit import USDTPerpetualAPI, OrderSide, OrderStatus, OrderType, TimeInForce

from debugger import debugger
from util import *


config = configparser.ConfigParser()
config.read('config.ini')

# api auth
BYBIT_API_KEY = config['bybit']['api_key']
BYBIT_API_SECRET = config['bybit']['api_secret']

# max limit for user setting value 
MAX_LIMIT_ORDER_COUNT_PER_EACH_SIDE = 20
MAX_REBALANCING_BALANCE_PERCENT_LIMIT = 45

# user settings
ORDER_OPTION = int(config['bybit']['order_option'])

SYMBOL = config['bybit']['symbol']
ORDER_GAP = float(config['bybit']['order_gap'])
START_PRICE = float(config['bybit']['start_price'])
LIMIT_ORDER_QUANTITY = float(config['bybit']['limit_order_quantity'])
SLEEP_AFTER_ORDER = float(config['bybit']['sleep_after_order'])

LIMIT_ORDER_COUNT_PER_EACH_SIDE = int(config['bybit']['limit_order_count_per_each_side'])
if LIMIT_ORDER_COUNT_PER_EACH_SIDE > MAX_LIMIT_ORDER_COUNT_PER_EACH_SIDE:
    debugger.info('limit_order_count_per_each_side 설정값이 [{}] 을 초과할 수 없습니다, 현재 설정값 - [{}]'.format(
        MAX_LIMIT_ORDER_COUNT_PER_EACH_SIDE, LIMIT_ORDER_COUNT_PER_EACH_SIDE))
    os.system("PAUSE")

# CANCEL_START_API_RATE = float(config['bybit']['cancel_start_api_rate'])
CANCEL_START_ACTIVE_ORDER_COUNT = float(config['bybit']['cancel_start_active_order_count'])
CANCEL_AMOUNT_AT_ONCE = int(config['bybit']['cancel_amount_at_once'])

REBALANCE_QUANTITY = float(config['bybit']['rebalance_quantity'])
MIN_POSITION_BALANCE_QUANTITY = float(config['bybit']['minimum_position_balance_quantity'])
MAX_POSITION_BALANCE_QUANTITY = float(config['bybit']['maximum_position_balance_quantity'])

REBALANCE_OPTION = int(config['bybit']['rebalance_option'])
TESTNET = True if int(config['bybit']['testnet']) == 1 else False

# thread pool max workers
MAX_WORKERS = 4


# bybit websocket url (real/testnet)
if not TESTNET:
    BYBIT_WEBSOCKET_PUBLIC_URL = "wss://stream.bybit.com/realtime_public"
    BYBIT_WEBSOCKET_PRIVATE_URL = "wss://stream.bybit.com/realtime_private"
else:
    BYBIT_WEBSOCKET_PUBLIC_URL = "wss://stream-testnet.bybit.com/realtime_public"
    BYBIT_WEBSOCKET_PRIVATE_URL = "wss://stream-testnet.bybit.com/realtime_private"


PING_INTERVAL = 60 * 0.3
WS_MAX_WAIT_TIME = 10
ACTIVE_ORDER_COUNT_INTERVAL = 10  # get current active order count every 10 secs


class OrderOption(object):
    BOTH_SIDE = 0
    ONLY_SHORT = 1
    ONLY_LONG = 2


class RebalancingMethod(object):
    """ option_1. Clear all long & short position and quit program
        option_2. Set long & short balance to 50:50 in the next limit price
        option_3. process option_2 and cancel all orders and quit program
    """
    OPTION_1 = 1
    OPTION_2 = 2
    OPTION_3 = 3
    OPTION_4 = 4


class OpenPositionExecuteType(object):
    EXECUTE_OPTION = 'execute_option'
    EXECUTE_REBALANCE_AND_QUIT = 'execute_rebalance_and_quit'

    class DoubleReverseOrderType(object):
        SHORT_DOUBLE = 'short_double'
        LONG_DOUBLE = 'long_double'
        RETURN_TO_ORIGINAL_ORDER = 'return_to_original_order'


class ProcessType(object):
    PLACE_ORDER = 'place_order'
    CANCEL_ORDER = 'cancel_order'

    class OrderType(object):
        OPEN = 'open'
        CLEAR = 'clear'


class Order(object):
    def __init__(self, symbol, side, order_type, price, qty, reduce_only=False, order_time=None, order_id=None):
        self.symbol = symbol
        self.side = side
        self.order_type = order_type
        self.price = price
        self.qty = qty
        self.reduce_only = reduce_only
        self.order_time = order_time
        self.order_id = order_id

    def __eq__(self, other):
        return (self.symbol, self.side, self.order_type, self.price, self.qty, self.reduce_only) == \
            (other.symbol, other.side, other.order_type, other.price, other.qty, other.reduce_only)


class BybitMakerTrader(threading.Thread):
    def __init__(self, canceled_order_q, order_id_to_cancel_q, stopped, order_lock):
        super().__init__()
        self.ws_public = BybitWebsocket(BYBIT_WEBSOCKET_PUBLIC_URL, None, None)
        self.ws_private = BybitWebsocket(BYBIT_WEBSOCKET_PRIVATE_URL, BYBIT_API_KEY, BYBIT_API_SECRET)

        self.base_bybit = USDTPerpetualAPI(BYBIT_API_KEY, BYBIT_API_SECRET, is_test=TESTNET)

        self.pool = futures.ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.single_pool = futures.ThreadPoolExecutor(max_workers=1)
        self.kline_interval = '1'
        self.ping_timer = time.time()
        self.ws_wait_timer = None
        self.total_active_order_count = None
        self.initial_order_price_set = set()
        
        self.stopped = stopped
        self.order_lock = order_lock

        self.order_processing_price_lock = threading.Lock()
        self.order_processing_price_dict = dict()
        self.order_processing_price_dict.setdefault(ProcessType.PLACE_ORDER, dict(clear=set(), open=set()))
        self.order_processing_price_dict.setdefault(ProcessType.CANCEL_ORDER, set())

        self.one_before_last_filled_price = None
        self.last_filled_price = None

        self.active_order_count_timer = None

        self.canceled_order_q = canceled_order_q
        self.order_id_to_cancel_q = order_id_to_cancel_q
        self.should_not_reorder = list()

    def set_ping_timer(self):
        self.ping_timer = time.time()

    def stop(self):
        self.stopped.set()
        debugger.debug('set bybit thread event, ready to close the thread')

        try:
            self.ws_public.exit()
            self.ws_private.exit()
            debugger.debug('exited websocket connection')
        except Exception:
            debugger.exception('unexpected error while exiting websocket connection')

    def initiate_maker_orders(self):
        """ start when (current price >= gap + start price)
        """
        while duplicate_access_evt.is_set():
            current_price = self.get_current_price()
            if current_price >= START_PRICE:
                # middle price should be near current price
                debugger.info('현재가[{}]가 설정 시작값[{}] 이상으로 프로그램을 시작합니다'.format(current_price, START_PRICE))
                middle_price = self.get_middle_price(current_price)

                self.place_limit_order_on_middle(SYMBOL, middle_price, initial_order=True)
                self.place_limit_orders_on_range(SYMBOL, middle_price, initial_order=True)
                break

            debugger.info('현재가가 시작가에 아직 도달하지 않았습니다 - 현재가[{}], 시작가[{}]'.format(current_price, START_PRICE))
    
    def get_current_price(self):
        while duplicate_access_evt.is_set():
            try:
                current_price = self.ws_public.get_current_price_from_trade(SYMBOL)
            except Exception:
                self.ws_public.subscribe_trade(SYMBOL)
                current_price = False
            if not current_price:
                current_price = self.base_bybit.get_current_price(SYMBOL)
                if current_price is None:
                    debugger.info('get_current_price()::: 현재가를 가져오는데 실패했습니다')
                    continue

            return current_price

    def get_middle_price(self, current_price):
        """
            to get middle price which is in line with order gap
            ex) if gap is 10 and current price is 51223, then middle price should be 51220
        """
        remainder = (current_price - START_PRICE) % ORDER_GAP
        if remainder < ORDER_GAP / 2:
            middle_price = current_price - remainder
        else:
            middle_price = current_price + (ORDER_GAP - remainder)
        return float('{:.8f}'.format(middle_price))

    @_retry_on_fail(tries_count=10, delay=0.6)
    def _place_order(self, symbol, price, qty, order_side, order_type, time_in_force=TimeInForce.POST_ONLY, reduce_only=False, close_on_trigger=False, last_try=False):
        try:
            open_or_clear = ProcessType.OrderType.CLEAR if reduce_only else ProcessType.OrderType.OPEN
            with self.order_processing_price_lock:
                processing_price_set = self.order_processing_price_dict[ProcessType.PLACE_ORDER][open_or_clear]

            res = self.base_bybit.place_order(symbol, price, qty, order_side, order_type, time_in_force=time_in_force, reduce_only=reduce_only, close_on_trigger=close_on_trigger)
            # remove price from dict if success
            if res:
                with self.order_processing_price_lock:
                    if price in processing_price_set:
                        processing_price_set.remove(price)
                return True
            
            # remove if it gets last_try=True from `_retry_on_fail`
            if last_try:
                debugger.debug('failed with last try, remove price from processing_price_set')
                with self.order_processing_price_lock:
                    if price in processing_price_set:
                        processing_price_set.remove(price)
        except OSError:
            try:
                shutil.copyfile('./cacert.pem', certifi.where())
            except Exception:
                debugger.exception('failed to restore cert file')
        except Exception:
            debugger.exception('unexpected error occured in `_place_order()`')
            with self.order_processing_price_lock:
                if price in processing_price_set:
                    processing_price_set.remove(price)
        return False

    @_retry_on_fail(tries_count=5, delay=3)
    def cancel_order(self, symbol, order_id, last_try=False):
        try:
            with self.order_processing_price_lock:
                processing_order_id_set = self.order_processing_price_dict[ProcessType.CANCEL_ORDER]

            res = self.base_bybit.cancel_order(symbol, order_id)

            # remove price from dict if success
            if res or res is None:
                with self.order_processing_price_lock:
                    if order_id in processing_order_id_set:
                        processing_order_id_set.remove(order_id)
                    return True
            
            if last_try:
                debugger.debug('failed with last try, remove price from processing_price_set')
                with self.order_processing_price_lock:
                    if order_id in processing_order_id_set:
                        processing_order_id_set.remove(order_id)

        except Exception:
            debugger.exception('unexpected error occured in `_cancel_order()')
            with self.order_processing_price_lock:
                if order_id in processing_order_id_set:
                    processing_order_id_set.remove(order_id)
        return False

    @_retry_on_fail(tries_count=100, delay=3)
    def _cancel_all_orders(self, symbol, last_try=False):
        try:
            res = self.base_bybit.cancel_all_orders(symbol)
            if res:
                return True
        except Exception:
            debugger.exception('unexpected error occured in `_cancel_all_orders()')
        return False
    
    @_retry_on_fail(tries_count=10, delay=0.6)
    def _get_all_order_ids(self, symbol, update=True, last_try=False):
        try:
            order_ids = self.base_bybit.get_all_order_ids(symbol, update=update)
            if order_ids is not None:
                return order_ids
        except OSError:
            try:
                shutil.copyfile('./cacert.pem', certifi.where())
            except Exception:
                debugger.exception('failed to restore cert file')
        except Exception:
            debugger.exception('unexpected error occured in `_get_all_order_ids()')
        return None
    
    @_retry_on_fail(tries_count=5, delay=0.6)
    def _get_position_sizes(self, symbol, last_try=False):
        try:
            buy_position_size, sell_position_size = self.base_bybit.get_position_sizes(symbol)
        except OSError:
            try:
                shutil.copyfile('./cacert.pem', certifi.where())
            except Exception:
                debugger.exception('failed to restore cert file')
            buy_position_size, sell_position_size = None, None
        except Exception:
            debugger.exception('unexpected error occured in `_get_position_sizes()')
            buy_position_size, sell_position_size = None, None
        return buy_position_size, sell_position_size
    
    def is_place_order_being_processed(self, side, price):
        with self.order_processing_price_lock:
            processing_price_set = self.order_processing_price_dict[ProcessType.PLACE_ORDER][side]
            if price in processing_price_set:
                debugger.debug('price - [{}][{}] is already being processed'.format(side, price))
                return True
            return False

    def is_cancel_order_being_processed(self, order_id):
        with self.order_processing_price_lock:
            processing_order_id_set = self.order_processing_price_dict[ProcessType.CANCEL_ORDER]
            if order_id in processing_order_id_set:
                debugger.debug('order_id - [{}] is already being processed'.format(order_id))
                return True
            return False
    
    def add_to_processing_price_set(self, order_type, price):
        with self.order_processing_price_lock:
            processing_price_set = self.order_processing_price_dict[ProcessType.PLACE_ORDER][order_type]
            processing_price_set.add(price)
    
    def add_to_processing_order_id_set(self, order_id):
        with self.order_processing_price_lock:
            processing_order_id_set = self.order_processing_price_dict[ProcessType.CANCEL_ORDER]
            processing_order_id_set.add(order_id)

    def add_initial_order_price(self, price: float):
        self.initial_order_price_set.add(price)

    def remove_initial_order_price(self, price: float):
        try:
            self.initial_order_price_set.remove(price)
        except Exception:
            debugger.debug('initial price - [{}] already removed'.format(price))

    def place_limit_order_on_middle(self, symbol, base_price, initial_order=False):
        # place short, long order at the same time for `middle_price`
        # because `PostOnly` option will prevent from "market order" execution
        # one of them will be executed as "limit order"
        # 최초 주문시에 가격 기록해서, 캔슬되는것들만 반대방향으로 걸기

        debugger.info('단일 지정가 [{}] 기준 주문 시작'.format(base_price))

        original_order_price_set = set()

        # # 마지막 직전에 체결된 주문의 가격을 기준으로 주문을 걸어야하고
        # # 마지막에 체결된 주문 가격에 주문이 들어가면 안됨
        if self.last_filled_price:
            original_order_price_set.add(float(self.last_filled_price))

        if initial_order:
            self.add_initial_order_price(float(base_price))

        middle_price_str = '{:.8f}'.format(base_price)

        if float(base_price) not in original_order_price_set:
            # limit order (either long or short)
            if self.last_filled_price:
                current_price = self.last_filled_price
            else:
                current_price = self.get_current_price()

            if current_price > base_price:
                # open long
                if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_LONG]:
                    debugger.info('현재가 [{}] > 중간 기준가 [{}], 롱 매수'.format(current_price, middle_price_str))

                    is_being_processed = self.is_place_order_being_processed(ProcessType.OrderType.OPEN,
                                                                             middle_price_str)
                    if not is_being_processed:
                        self.add_to_processing_price_set(ProcessType.OrderType.OPEN, middle_price_str)
                        self.pool.submit(self._place_order, symbol, middle_price_str, LIMIT_ORDER_QUANTITY,
                                         OrderSide.BUY, OrderType.LIMIT)
                        debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))

                # clear short
                if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_SHORT]:
                    debugger.info('현재가 [{}] > 중간 기준가 [{}], 숏 매도'.format(current_price, middle_price_str))

                    is_being_processed = self.is_place_order_being_processed(ProcessType.OrderType.CLEAR,
                                                                             middle_price_str)
                    if not is_being_processed:
                        self.add_to_processing_price_set(ProcessType.OrderType.CLEAR, middle_price_str)
                        self.pool.submit(self._place_order, symbol, middle_price_str, LIMIT_ORDER_QUANTITY,
                                         OrderSide.BUY, OrderType.LIMIT, reduce_only=True)
                        debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))

                original_order_price_set.add(base_price)

            elif current_price < base_price:
                # open short
                if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_SHORT]:
                    debugger.info('현재가 [{}] < 중간 기준가 [{}], 숏 매수'.format(current_price, middle_price_str))

                    is_being_processed = self.is_place_order_being_processed(ProcessType.OrderType.OPEN,
                                                                             middle_price_str)
                    if not is_being_processed:
                        self.add_to_processing_price_set(ProcessType.OrderType.OPEN, middle_price_str)
                        self.pool.submit(self._place_order, symbol, middle_price_str, LIMIT_ORDER_QUANTITY,
                                         OrderSide.SELL, OrderType.LIMIT)
                        debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))

                # clear long
                if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_LONG]:
                    debugger.info('현재가 [{}] < 중간 기준가 [{}], 롱 매도'.format(current_price, middle_price_str))

                    is_being_processed = self.is_place_order_being_processed(ProcessType.OrderType.CLEAR,
                                                                             middle_price_str)
                    if not is_being_processed:
                        self.add_to_processing_price_set(ProcessType.OrderType.CLEAR, middle_price_str)
                        self.pool.submit(self._place_order, symbol, middle_price_str, LIMIT_ORDER_QUANTITY,
                                         OrderSide.SELL, OrderType.LIMIT, reduce_only=True)
                        debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))

                original_order_price_set.add(middle_price_str)

            elif current_price == base_price:
                debugger.info('현재가 [{}] = 중간 기준가 [{}], 해당 가격대에 지정가를 걸 수 없습니다'.format(current_price, base_price))

    def place_limit_orders_on_range(self, symbol, base_price, initial_order=False):
        debugger.info('범위 지정가 [{}] 기준 주문 시작'.format(base_price))

        original_order_price_set = set()

        # # 마지막 직전에 체결된 주문의 가격을 기준으로 주문을 걸어야하고
        # # 마지막에 체결된 주문 가격에 주문이 들어가면 안됨
        if self.last_filled_price:
            original_order_price_set.add(float(self.last_filled_price))

        order_ids = self._get_all_order_ids(symbol, update=True)
        if order_ids is None:
            debugger.info('현재 활성화된 주문정보를 가져오는데 실패했습니다')
            return False

        for order_id in order_ids:
            # because order history(`self.base_bybit.active_orders_dict`) is updated in `get_all_order_ids()` above
            # it doesn't need to be updated for every iteration
            order_history = self.base_bybit.get_order_history(order_id, symbol, update=False)
            if not order_history:
                continue

            _, order_price = order_history
            original_order_price_set.add(order_price)

        # get short price range based on middle_price
        # to prevent ip block on exceeding rate limit, it should sleep a bit
        for i in range(1, LIMIT_ORDER_COUNT_PER_EACH_SIDE + 1):
            short_price_str = '{:.8f}'.format(base_price + (ORDER_GAP * i))
            # 최초 주문시에 가격 기록해서, 캔슬되는것들만 반대방향으로 걸기
            if initial_order:
                self.add_initial_order_price(float(short_price_str))

            if float(short_price_str) not in original_order_price_set:
                # short open order
                if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_SHORT]:
                    is_being_processed = self.is_place_order_being_processed(ProcessType.OrderType.OPEN,
                                                                             short_price_str)
                    if not is_being_processed:
                        self.add_to_processing_price_set(ProcessType.OrderType.OPEN, short_price_str)
                        self.pool.submit(self._place_order, symbol, short_price_str, LIMIT_ORDER_QUANTITY,
                                         OrderSide.SELL, OrderType.LIMIT)
                        debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
                        time.sleep(SLEEP_AFTER_ORDER)

                # long clear order, `reduce_only` 는 주문을 넣는 side 의 반대쪽 size 를 줄이는것이므로 롱은 SELL 로 청산, 숏은 BUY 로 청산한다
                if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_LONG]:
                    is_being_processed = self.is_place_order_being_processed(ProcessType.OrderType.CLEAR,
                                                                             short_price_str)
                    if not is_being_processed:
                        self.add_to_processing_price_set(ProcessType.OrderType.CLEAR, short_price_str)
                        self.pool.submit(self._place_order, symbol, short_price_str, LIMIT_ORDER_QUANTITY,
                                         OrderSide.SELL, OrderType.LIMIT, reduce_only=True)
                        debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
                        time.sleep(SLEEP_AFTER_ORDER)

            long_price_str = '{:.8f}'.format(base_price - (ORDER_GAP * i))
            # 최초 주문시에 가격 기록해서, 캔슬되는것들만 반대방향으로 걸기
            if initial_order:
                self.add_initial_order_price(float(long_price_str))

            if float(long_price_str) not in original_order_price_set:
                # long open order
                if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_LONG]:
                    is_being_processed = self.is_place_order_being_processed(ProcessType.OrderType.OPEN, long_price_str)
                    if not is_being_processed:
                        self.add_to_processing_price_set(ProcessType.OrderType.OPEN, long_price_str)
                        self.pool.submit(self._place_order, symbol, long_price_str, LIMIT_ORDER_QUANTITY, OrderSide.BUY,
                                         OrderType.LIMIT)
                        debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
                        time.sleep(SLEEP_AFTER_ORDER)

                # short clear order
                if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_SHORT]:
                    is_being_processed = self.is_place_order_being_processed(ProcessType.OrderType.CLEAR,
                                                                             long_price_str)
                    if not is_being_processed:
                        self.add_to_processing_price_set(ProcessType.OrderType.CLEAR, long_price_str)
                        self.pool.submit(self._place_order, symbol, long_price_str, LIMIT_ORDER_QUANTITY, OrderSide.BUY,
                                         OrderType.LIMIT, reduce_only=True)
                        debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
                        time.sleep(SLEEP_AFTER_ORDER)

    def place_limit_orders_on_end(self, symbol, base_price, on_end):
        debugger.info('끝단 지정가 [{}] 기준 주문 시작'.format(base_price))

        original_order_price_set = set()

        # # 마지막 직전에 체결된 주문의 가격을 기준으로 주문을 걸어야하고
        # # 마지막에 체결된 주문 가격에 주문이 들어가면 안됨
        if self.last_filled_price:
            debugger.info('지정가 주문시 마지막 체결가 - [{}] 를 제외합니다'.format(self.last_filled_price))
            original_order_price_set.add(float(self.last_filled_price))

        order_ids = self._get_all_order_ids(symbol, update=True)
        if order_ids is None:
            debugger.info('현재 활성화된 주문정보를 가져오는데 실패했습니다')
            return False

        for order_id in order_ids:
            # because order history(`self.base_bybit.active_orders_dict`) is updated in `get_all_order_ids()` above
            # it doesn't need to be updated for every iteration
            order_history = self.base_bybit.get_order_history(order_id, symbol, update=False)
            if not order_history:
                continue

            _, order_price = order_history
            original_order_price_set.add(order_price)

        if on_end == OrderSide.SELL:
            short_price_str = '{:.8f}'.format(base_price + (ORDER_GAP * LIMIT_ORDER_COUNT_PER_EACH_SIDE))
            if float(short_price_str) not in original_order_price_set:
                # short open order
                if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_SHORT]:
                    is_being_processed = self.is_place_order_being_processed(ProcessType.OrderType.OPEN,
                                                                             short_price_str)
                    if not is_being_processed:
                        self.add_to_processing_price_set(ProcessType.OrderType.OPEN, short_price_str)
                        self.pool.submit(self._place_order, symbol, short_price_str, LIMIT_ORDER_QUANTITY,
                                         OrderSide.SELL, OrderType.LIMIT)
                        debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
                        time.sleep(SLEEP_AFTER_ORDER)

                # long clear order, `reduce_only` 는 주문을 넣는 side 의 반대쪽 size 를 줄이는것이므로 롱은 SELL 로 청산, 숏은 BUY 로 청산한다
                if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_LONG]:
                    is_being_processed = self.is_place_order_being_processed(ProcessType.OrderType.CLEAR,
                                                                             short_price_str)
                    if not is_being_processed:
                        self.add_to_processing_price_set(ProcessType.OrderType.CLEAR, short_price_str)
                        self.pool.submit(self._place_order, symbol, short_price_str, LIMIT_ORDER_QUANTITY,
                                         OrderSide.SELL, OrderType.LIMIT, reduce_only=True)
                        debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
                        time.sleep(SLEEP_AFTER_ORDER)

        elif on_end == OrderSide.BUY:
            long_price_str = '{:.8f}'.format(base_price - (ORDER_GAP * LIMIT_ORDER_COUNT_PER_EACH_SIDE))
            if float(long_price_str) not in original_order_price_set:
                # long open order
                if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_LONG]:
                    is_being_processed = self.is_place_order_being_processed(ProcessType.OrderType.OPEN, long_price_str)
                    if not is_being_processed:
                        self.add_to_processing_price_set(ProcessType.OrderType.OPEN, long_price_str)
                        self.pool.submit(self._place_order, symbol, long_price_str, LIMIT_ORDER_QUANTITY,
                                         OrderSide.BUY, OrderType.LIMIT)
                        debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
                        time.sleep(SLEEP_AFTER_ORDER)

                # short clear order
                if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_SHORT]:
                    is_being_processed = self.is_place_order_being_processed(ProcessType.OrderType.CLEAR,
                                                                             long_price_str)
                    if not is_being_processed:
                        self.add_to_processing_price_set(ProcessType.OrderType.CLEAR, long_price_str)
                        self.pool.submit(self._place_order, symbol, long_price_str, LIMIT_ORDER_QUANTITY,
                                         OrderSide.BUY, OrderType.LIMIT, reduce_only=True)
                        debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
                        time.sleep(SLEEP_AFTER_ORDER)

    def cancel_all_orders(self):
        res = self._cancel_all_orders(SYMBOL)
        return res

    def check_open_position_sizes(self):
        """
            Check open position sizes and return `OpenPositionExecuteType`, `both_sizes`
            
            OpenPositionExecuteType: signal for what to execute between "option execution" and "rebalance and quit" execution
            both_sizes: return both buy_position_size and sell_position_size  
        """
        buy_position_size, sell_position_size = self._get_position_sizes(SYMBOL)
        both_sizes = buy_position_size, sell_position_size

        if buy_position_size is None and sell_position_size is None:
            debugger.info('현재 포지션을 가져오는데 실패했습니다')
            return False, both_sizes

        if buy_position_size == 0 and sell_position_size == 0:
            debugger.info('현재 포지션이 없습니다 {}'.format(both_sizes))
            return False, both_sizes

        if ORDER_OPTION == OrderOption.ONLY_LONG:
            debugger.info('현재 롱 포지션 - [{}]'.format(buy_position_size))
            if buy_position_size < MIN_POSITION_BALANCE_QUANTITY or buy_position_size > MAX_POSITION_BALANCE_QUANTITY:
                debugger.info('현재 롱 포지션이 설정 값을 벗어났습니다, 현재 롱 포지션 - [{}] | 설정값 - ([{}] ~ [{}])'.format(
                    buy_position_size, MIN_POSITION_BALANCE_QUANTITY, MAX_POSITION_BALANCE_QUANTITY
                ))
                return OpenPositionExecuteType.EXECUTE_OPTION, both_sizes

        elif ORDER_OPTION == OrderOption.ONLY_SHORT:
            debugger.info('현재 숏 포지션- [{}]'.format(sell_position_size))
            if sell_position_size < MIN_POSITION_BALANCE_QUANTITY or sell_position_size > MAX_POSITION_BALANCE_QUANTITY:
                debugger.info('현재 숏 포지션이 설정 값을 벗어났습니다, 현재 숏 포지션 - [{}] | 설정값 - ([{}] ~ [{}])'.format(
                    sell_position_size, MIN_POSITION_BALANCE_QUANTITY, MAX_POSITION_BALANCE_QUANTITY
                ))
                return OpenPositionExecuteType.EXECUTE_OPTION, both_sizes

        elif ORDER_OPTION == OrderOption.BOTH_SIDE:
            debugger.info('현재 롱 포지션 - [{}], 숏 포지션- [{}]'.format(buy_position_size, sell_position_size))
            if buy_position_size < MIN_POSITION_BALANCE_QUANTITY or buy_position_size > MAX_POSITION_BALANCE_QUANTITY:
                debugger.info('현재 롱 포지션이 설정 값을 벗어났습니다, 현재 롱 포지션 - [{}] | 설정값 - ([{}] ~ [{}])'.format(
                    buy_position_size, MIN_POSITION_BALANCE_QUANTITY, MAX_POSITION_BALANCE_QUANTITY
                ))
                return OpenPositionExecuteType.EXECUTE_OPTION, both_sizes

            if sell_position_size < MIN_POSITION_BALANCE_QUANTITY or sell_position_size > MAX_POSITION_BALANCE_QUANTITY:
                debugger.info('현재 숏 포지션이 설정 값을 벗어났습니다, 현재 숏 포지션 - [{}] | 설정값 - ([{}] ~ [{}])'.format(
                    sell_position_size, MIN_POSITION_BALANCE_QUANTITY, MAX_POSITION_BALANCE_QUANTITY
                ))
                return OpenPositionExecuteType.EXECUTE_OPTION, both_sizes
        return False, both_sizes
    
    def rebalance_open_positions(self, buy_position_size, sell_position_size):
        """
            RebalancingMethod.option_2
            Rebalance open positions(long & short) to REBALANCE_QUANTITY
        """
        debugger.info('리밸런싱 시작, 리밸런싱 수량 - [{}]'.format(REBALANCE_QUANTITY))

        tasks_in_pool = list()

        if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_LONG]:
            if buy_position_size > REBALANCE_QUANTITY:
                rebalance_size = buy_position_size - REBALANCE_QUANTITY
                clear_long_task = self.pool.submit(self._place_order, SYMBOL, None, rebalance_size, OrderSide.SELL, OrderType.MARKET, time_in_force=TimeInForce.GOOD_TILL_CANCEL, reduce_only=True)
                debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
                tasks_in_pool.append(clear_long_task)
            elif buy_position_size < REBALANCE_QUANTITY:
                rebalance_size = REBALANCE_QUANTITY - buy_position_size
                open_long_task = self.pool.submit(self._place_order, SYMBOL, None, rebalance_size, OrderSide.BUY, OrderType.MARKET, time_in_force=TimeInForce.GOOD_TILL_CANCEL)
                debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
                tasks_in_pool.append(open_long_task)

        if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_SHORT]:
            if sell_position_size > REBALANCE_QUANTITY:
                rebalance_size = sell_position_size - REBALANCE_QUANTITY
                clear_short_task = self.pool.submit(self._place_order, SYMBOL, None, rebalance_size, OrderSide.BUY, OrderType.MARKET, time_in_force=TimeInForce.GOOD_TILL_CANCEL, reduce_only=True)
                debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
                tasks_in_pool.append(clear_short_task)
            elif sell_position_size < REBALANCE_QUANTITY:
                rebalance_size = REBALANCE_QUANTITY - sell_position_size
                open_short_task = self.pool.submit(self._place_order, SYMBOL, None, rebalance_size, OrderSide.SELL, OrderType.MARKET, time_in_force=TimeInForce.GOOD_TILL_CANCEL)
                debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
                tasks_in_pool.append(open_short_task)
        
        is_tasks_all_success = self.check_done_tasks(tasks_in_pool, self.rebalance_open_positions.__name__)
        return is_tasks_all_success

    def clear_open_positions(self, buy_position_size, sell_position_size):
        """
            RebalancingMethod.option_1
            Clear all open positions (both long & short)
        """
        debugger.info('리밸런싱 시작 - [포지션 모두청산]')

        tasks_in_pool = list()
        if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_LONG]:
            clear_long_task = self.pool.submit(self._place_order, SYMBOL, None, buy_position_size, OrderSide.SELL, OrderType.MARKET, time_in_force=TimeInForce.GOOD_TILL_CANCEL, reduce_only=True)
            debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
            tasks_in_pool.append(clear_long_task)

        if ORDER_OPTION in [OrderOption.BOTH_SIDE, OrderOption.ONLY_SHORT]:
            clear_short_task = self.pool.submit(self._place_order, SYMBOL, None, sell_position_size, OrderSide.BUY, OrderType.MARKET, time_in_force=TimeInForce.GOOD_TILL_CANCEL, reduce_only=True)
            debugger.debug('main pool pending: {} jobs'.format(self.pool._work_queue.qsize()))
            tasks_in_pool.append(clear_short_task)

        is_tasks_all_success = self.check_done_tasks(tasks_in_pool, self.clear_open_positions.__name__)
        return is_tasks_all_success
    
    def check_done_tasks(self, tasks_in_pool, func_name):
        # wait for tasks to be finished
        if tasks_in_pool:
            task_count = len(tasks_in_pool)
            debugger.debug('wait for futures in function - [{}], task count - [{}]'.format(func_name, task_count))

            done_tasks = [done_task.result() for done_task in futures.as_completed(tasks_in_pool)]
            if not all(done_tasks):
                debugger.debug('there was an failed API request in function - [{}]'.format(func_name))
                return False

            debugger.debug('all tasks have been finished in function - [{}], task count - [{}]'.format(func_name, task_count))
        return True
    
    def set_ws_wait_timer(self):
        self.ws_wait_timer = time.time()
    
    def unset_ws_wait_timer(self):
        self.ws_wait_timer = None

    def set_active_order_count_timer(self):
        self.active_order_count_timer = time.time()

    def _place_end_orders(self):
        if self.last_filled_price > self.one_before_last_filled_price:
            self.single_pool.submit(self.place_limit_orders_on_end, SYMBOL, self.last_filled_price, OrderSide.SELL)
        else:
            self.single_pool.submit(self.place_limit_orders_on_end, SYMBOL, self.last_filled_price, OrderSide.BUY)

    def _subscribe_topics(self):
        self.ws_public.subscribe_trade(SYMBOL)  # to subscribe current price
        self.ws_private.subscribe_order()

    def run(self):
        self._subscribe_topics()
        self.initiate_maker_orders()

        while duplicate_access_evt.is_set():
            try:
                self._run()
            except OSError:
                try:
                    shutil.copyfile('./cacert.pem', certifi.where())
                except Exception:
                    debugger.exception('failed to restore cert file')
            except Exception as e:
                debugger.exception('unexpected exception in BybitMakerTrader, error message - [{}]'.format(e))

            self.ws_public.exit()
            self.ws_private.exit()

            self.ws_public = BybitWebsocket(BYBIT_WEBSOCKET_PUBLIC_URL, None, None)
            self.ws_private = BybitWebsocket(BYBIT_WEBSOCKET_PRIVATE_URL, BYBIT_API_KEY, BYBIT_API_SECRET)
            self._subscribe_topics()

        debugger.debug('Done. dup:{} stopped:{}'.format(duplicate_access_evt.is_set(), self.stopped.is_set()))

    def _run(self):
        if self._rebalance():
            return

        while not self.stopped.is_set() and duplicate_access_evt.is_set():
            if not self.ws_public.ws.sock or not self.ws_public.ws.sock.connected:
                debugger.debug('ws_public connection closed, try to reconnect')
                self.ws_public = BybitWebsocket(BYBIT_WEBSOCKET_PUBLIC_URL, None, None)
                debugger.debug('ws_public reconnected')
                self.ws_public.subscribe_trade(SYMBOL)  # to subscribe current price
                debugger.debug('subscribed to {}'.format(SYMBOL))
                continue

            if not self.ws_private.ws.sock or not self.ws_private.ws.sock.connected:
                debugger.debug('ws_private connection closed, try to reconnect')
                self.ws_private = BybitWebsocket(BYBIT_WEBSOCKET_PRIVATE_URL, BYBIT_API_KEY, BYBIT_API_SECRET)
                debugger.debug('ws_public reconnected')
                self.ws_private.subscribe_order()
                debugger.debug('subscribed to order')
                continue

            if time.time() > self.ping_timer + PING_INTERVAL:
                self.ws_public.ping()
                self.ws_private.ping()
                self.set_ping_timer()

            order_list = self.ws_private.get_order_status()
            if order_list:
                for order in order_list:
                    order_time = order['update_time']  # 주문체결시각 ex) '2021-10-21T15:40:29.888691Z'
                    kst_order_time_ = datetime_parser(order_time) + datetime.timedelta(hours=9)
                    kst_order_time = datetime.datetime.strftime(kst_order_time_, '%Y-%m-%d %H:%M:%S.%fZ')

                    symbol = order['symbol']
                    order_status = order['order_status']
                    order_id = order['order_id']
                    order_side = order['side']
                    order_price = order['price']
                    order_qty = order['qty']
                    order_type = order['order_type']
                    reduce_only = order['reduce_only']
                    debugger.info(
                        '주문체결현황 - order_id[{}], order_status[{}], order_type[{}], order_side[{}], order_price[{}], order_qty[{}], reduce_only[{}], order_time[{}]'.format(
                            order_id,
                            order_status,
                            order_type,
                            order_side,
                            order_price,
                            order_qty,
                            reduce_only,
                            kst_order_time
                        )
                    )

                    if order_status == OrderStatus.FILLED and order_type == OrderType.LIMIT:
                        debugger.debug('order lock: {}'.format(self.order_lock.locked()))
                        locked = self.order_lock.acquire(timeout=60)
                        if not locked:
                            debugger.error("order lock is busy")
                            self.ws_private.data['order'].insert(0, order_list[order_list.index(order):])
                            self.order_lock.release()
                            return

                        self.last_filled_price = order_price

                        # 지정가 체결시 직전 체결가로 주문
                        if self.one_before_last_filled_price:
                            self.place_limit_order_on_middle(SYMBOL, self.one_before_last_filled_price)

                            # 현재 체결가가 직전체결가보다 높으면 위로 (숏으로 끝단에 걸고) 아니면 반대로
                            self._place_end_orders()

                        # 마지막 체결가는 바로직전 체결가로 세팅
                        self.one_before_last_filled_price = order_price
                        self.order_lock.release()
                        debugger.debug('order lock: {}'.format(self.order_lock.locked()))

                    elif order_status == OrderStatus.NEW:
                        if order_price in self.initial_order_price_set:
                            self.remove_initial_order_price(order_price)

                        open_or_clear = ProcessType.OrderType.CLEAR if reduce_only else ProcessType.OrderType.OPEN
                        with self.order_processing_price_lock:
                            processing_price_set = self.order_processing_price_dict[ProcessType.PLACE_ORDER][open_or_clear]

                            if order_price in processing_price_set:
                                processing_price_set.remove(order_price)

                    elif order_status == OrderStatus.CANCELLED:
                        if order_id in self.should_not_reorder:
                            try:
                                self.should_not_reorder.remove(order_id)
                            except Exception as e:
                                debugger.error('order_id - [{}]::: exception occurred while removing element from should_not_reorder, error - [{}]'.format(order_id, e))
                            continue

                        if order_price in self.initial_order_price_set:
                            debugger.info('최초주문에 실패하여 반대방향으로 다시 주문을 넣습니다 - order_price[{}], order_side[{}], reduce_only[{}]'.format(order_price, order_side, reduce_only))
                            reversed_order_side = OrderSide.BUY if order_side == OrderSide.SELL else OrderSide.SELL
                            reversed_reduce_only = True if not reduce_only else False

                            self._place_order(SYMBOL, order_price, order_qty, reversed_order_side, OrderType.LIMIT, reduce_only=reversed_reduce_only)
                            continue

                        order_obj = Order(
                            symbol,
                            order_side,
                            order_type,
                            order_price,
                            order_qty,
                            reduce_only,
                            kst_order_time,
                            order_id
                        )
                        q_payload = (order_obj, self.last_filled_price)
                        self.canceled_order_q.put(q_payload)

            elif not order_list:
                continue

            if self._rebalance():
                return

            if not self.active_order_count_timer or time.time() > self.active_order_count_timer + ACTIVE_ORDER_COUNT_INTERVAL:
                buy_cnt, sell_cnt = self.base_bybit.get_active_limit_order_counts(SYMBOL, update=True)
                if buy_cnt and sell_cnt:
                    self.total_active_order_count = buy_cnt + sell_cnt
                    debugger.info('현재 지정가 주문 - 롱 [{}]개, 숏 [{}]개'.format(buy_cnt, sell_cnt))

                    # 지정가가 N 개 이상일 때 마지막 끝단에서 M 개 만큼 취소하기 위해, 취소할 주문번호를 취소 스레드로 보냄
                    if self.total_active_order_count > CANCEL_START_ACTIVE_ORDER_COUNT:
                        debugger.info(
                            '현재 걸려있는 지정가의 개수가 [{}] 개를 초과하여 현재가로부터 가장 먼 [{}] 개 주문만큼 취소합니다'.format(
                                CANCEL_START_ACTIVE_ORDER_COUNT, CANCEL_AMOUNT_AT_ONCE)
                        )

                        order_ids_to_cancel = self.get_order_ids_to_cancel(self.last_filled_price)
                        self.should_not_reorder.extend(order_ids_to_cancel)

                        for order_id in order_ids_to_cancel:
                            self.order_id_to_cancel_q.put(order_id)

                    self.set_active_order_count_timer()

    def get_order_ids_to_cancel(self, middle_price):
        order_ids = self.base_bybit.get_all_order_ids(SYMBOL)
        order_tuple_list = list()

        for order_id in order_ids:
            order_history = self.base_bybit.get_order_history(order_id, SYMBOL, update=False)
            if not order_history:
                continue

            _, order_price = order_history
            order_tuple = (order_price, order_id)
            order_tuple_list.append(order_tuple)

        # cancel half of CANCEL_AMOUNT_AT_ONCE from the end of each side
        sorted_order_tuple_list = sorted(order_tuple_list, key=lambda order: abs(order[0] - middle_price), reverse=True)

        order_tuples_to_cancel = sorted_order_tuple_list[:CANCEL_AMOUNT_AT_ONCE]
        order_ids_to_cancel = [order_id for _, order_id in order_tuples_to_cancel]
        return order_ids_to_cancel

    def _rebalance(self):
        open_position_execute_type, both_sizes = self.check_open_position_sizes()
        if open_position_execute_type == OpenPositionExecuteType.EXECUTE_OPTION:
            buy_position_size, sell_position_size = both_sizes

            if REBALANCE_OPTION == RebalancingMethod.OPTION_1:
                clear_success = self.clear_open_positions(buy_position_size, sell_position_size)
                if clear_success:
                    debugger.info('옵션 1번 수행 완료: 포지션 모두 청산, 프로그램을 종료합니다')
                    return True

            elif REBALANCE_OPTION == RebalancingMethod.OPTION_2:
                self.rebalance_open_positions(buy_position_size, sell_position_size)
                debugger.info('옵션 2번 수행완료: 물량 {} 으로 리밸런싱 성공'.format(REBALANCE_QUANTITY))

            elif REBALANCE_OPTION == RebalancingMethod.OPTION_3:
                rebalance_success = self.rebalance_open_positions(buy_position_size, sell_position_size)
                cancel_success = self.cancel_all_orders()

                if rebalance_success and cancel_success:
                    debugger.info(
                        '옵션 3번 수행 완료: 물량 {} 으로 리밸런싱 성공 및 지정가 주문 모두 취소, 프로그램을 종료합니다'.format(REBALANCE_QUANTITY))
                    return True

            elif REBALANCE_OPTION == RebalancingMethod.OPTION_4:
                cancel_success = self.cancel_all_orders()
                if cancel_success:
                    debugger.info('옵션 4번 수행 완료: 지정가 주문 모두 취소, 프로그램을 종료합니다')
                    return True


class CanceledOrderHandler(threading.Thread):
    def __init__(self, canceled_orders_queue: queue.Queue, bybit_maker_trader, stopped, order_lock):
        super(CanceledOrderHandler, self).__init__()
        self.canceled_orders_list = list()
        self.cancel_orders_queue = canceled_orders_queue
        self.base_bybit = USDTPerpetualAPI(BYBIT_API_KEY, BYBIT_API_SECRET, is_test=TESTNET)

        self.bybit_maker_trade = bybit_maker_trader
        # self.pool = futures.ThreadPoolExecutor(MAX_WORKERS)

        self.stopped = stopped
        self.order_lock = order_lock

    def stop(self):
        debugger.debug('Stop canceled order handler')
        self.stopped.set()

    def run(self):
        while not self.stopped.is_set() and duplicate_access_evt.is_set():
            try:
                self._run()
            except Exception as e:
                debugger.exception('unexpected error in CanceledOrderHandler thread- [{}]'.format(e))

        debugger.debug('stopped: {} dup access: {}'.format(self.stopped, duplicate_access_evt.is_set()))

    def _run(self):
        while not self.stopped.is_set() and duplicate_access_evt.is_set():
            try:
                order_obj, last_filled_price = self.cancel_orders_queue.get(timeout=0.3)
                if order_obj not in self.canceled_orders_list:
                    debugger.debug(
                        'got cancelled order info::: order_price - [{}], order_qty - [{}], order_side - [{}], order_type - [{}], reduce_only - [{}], canceled_time - [{}]'.format(
                            order_obj.price, order_obj.qty, order_obj.side, order_obj.order_type, order_obj.reduce_only,
                            order_obj.order_time
                        ))
                    self.canceled_orders_list.append(order_obj)

            except queue.Empty:
                pass

            if self.canceled_orders_list:
                debugger.debug('canceled order count - [{}]'.format(len(self.canceled_orders_list)))
                current_price = self.bybit_maker_trade.get_current_price()

                # 취소재주문 스레드에서도 이미 지정가 걸려있는 가격에 주문을 넣으면 안되기때문에 기주문 체크 필요
                original_order_price_set = set()
                order_ids = self.base_bybit.get_all_order_ids(SYMBOL, update=True)
                if order_ids is None:
                    debugger.info('현재 활성화된 주문정보를 가져오는데 실패했습니다')
                    continue

                for order_id in order_ids:
                    # because order history(`self.base_bybit.active_orders_dict`) is updated in `get_all_order_ids()` above
                    # it doesn't need to be updated for every iteration
                    order_history = self.base_bybit.get_order_history(order_id, SYMBOL, update=False)
                    if not order_history:
                        continue

                    _, order_price = order_history
                    original_order_price_set.add(order_price)

                canceled_order_price_list = list()
                for order in sorted(self.canceled_orders_list, key=lambda order: order.price):
                    # for log
                    canceled_order_info = (order.side, order.price, order.reduce_only)
                    canceled_order_price_list.append(canceled_order_info)

                    if order.side == OrderSide.SELL and current_price <= order.price:
                        debugger.debug('order lock: {}'.format(self.order_lock.locked()))
                        with self.order_lock:
                            if float(self.bybit_maker_trade.one_before_last_filled_price) not in original_order_price_set:
                                debugger.info(
                                    '현재가 - [{}], 이전 취소 주문을 재주문 합니다, retry_price - [{}], order_price - [{}], order_qty - [{}], order_side - [{}], order_type - [{}], reduce_only - [{}], canceled_time - [{}]'.format(
                                        current_price, self.bybit_maker_trade.one_before_last_filled_price, order.price,
                                        order.qty, order.side, order.order_type, order.reduce_only, order.order_time
                                ))
                                self.bybit_maker_trade.last_filled_price = order.price
                                self.bybit_maker_trade.place_limit_order_on_middle(
                                    SYMBOL, self.bybit_maker_trade.one_before_last_filled_price
                                )
                                # 마지막 체결가는 바로직전 체결가로 세팅
                                self.bybit_maker_trade.one_before_last_filled_price = order.price
                                self.canceled_orders_list.remove(order)
                        debugger.debug('order lock: {}'.format(self.order_lock.locked()))

                for order in sorted(self.canceled_orders_list, key=lambda order: order.price, reverse=True):
                    # for log
                    canceled_order_info = (order.side, order.price, order.reduce_only)
                    canceled_order_price_list.append(canceled_order_info)

                    if order.side == OrderSide.BUY and current_price >= order.price:
                        debugger.debug('order lock: {}'.format(self.order_lock.locked()))
                        with self.order_lock:
                            if float(self.bybit_maker_trade.one_before_last_filled_price) not in original_order_price_set:
                                debugger.info(
                                    '현재가 - [{}], 이전 취소 주문을 재주문 합니다, retry_price - [{}], order_price - [{}], order_qty - [{}], order_side - [{}], order_type - [{}], reduce_only - [{}], canceled_time - [{}]'.format(
                                        current_price, self.bybit_maker_trade.one_before_last_filled_price, order.price,
                                        order.qty, order.side, order.order_type, order.reduce_only, order.order_time
                                    ))
                                self.bybit_maker_trade.last_filled_price = order.price
                                self.bybit_maker_trade.place_limit_order_on_middle(
                                    SYMBOL, self.bybit_maker_trade.one_before_last_filled_price
                                )
                                # 마지막 체결가는 바로직전 체결가로 세팅
                                self.bybit_maker_trade.one_before_last_filled_price = order.price
                                self.canceled_orders_list.remove(order)
                        debugger.debug('order lock: {}'.format(self.order_lock.locked()))


class CancelExistingOrderHandler(threading.Thread):
    def __init__(self, order_id_q: queue.Queue, bybit_maker_trader, stopped):
        super(CancelExistingOrderHandler, self).__init__()
        self.order_id_q = order_id_q
        self.base_bybit = USDTPerpetualAPI(BYBIT_API_KEY, BYBIT_API_SECRET, is_test=TESTNET)
        self.bybit_maker_trader = bybit_maker_trader

        self.pool = futures.ThreadPoolExecutor(MAX_WORKERS)

        self.stopped = stopped

    def stop(self):
        self.stopped.set()

    def run(self):
        while not self.stopped.is_set() and duplicate_access_evt.is_set():
            try:
                self._run()
            except Exception as e:
                debugger.exception('unexpected error in CanceledOrderHandler thread- [{}]'.format(e))

    def _run(self):
        while not self.stopped.is_set() and duplicate_access_evt.is_set():
            try:
                order_id_to_cancel = self.order_id_q.get(timeout=0.3)
            except queue.Empty:
                continue

            debugger.debug('CancelExistingOrderHandler got order_id - [{}]'.format(order_id_to_cancel))
            self.pool.submit(
                self.bybit_maker_trader.cancel_order,
                SYMBOL,
                order_id_to_cancel
            )
            debugger.debug('cancel pool pending: {} jobs'.format(self.pool._work_queue.qsize()))


if __name__ == '__main__':
    multiprocessing.freeze_support()
    id_ = user_check('user', 'user', 'BybitMakerTrader')
    try:
        try:
            sync_time(_raise=True)
        except Exception:
            res = input('관리자 권한이 없어 시간 동기화에 실패했습니다. 계속 하시겠습니까?(y/N)')
            if res.lower() != 'y':
                sys.exit()

        try:
            copy_cert(_raise=True)
        except Exception:
            res = input('관리자 권한이 없어 시스템 파일 복사에 실패했습니다. 계속 하시겠습니까?(y/N)')
            if res.lower() != 'y':
                sys.exit()

        canceled_orders_q = queue.Queue()
        order_id_to_cancel_q = queue.Queue()
        stopped = threading.Event()

        order_lock = threading.Lock()

        maker_trader = BybitMakerTrader(canceled_orders_q, order_id_to_cancel_q, stopped, order_lock)
        canceled_order_handler = CanceledOrderHandler(canceled_orders_q, maker_trader, stopped, order_lock)
        cancel_existing_order_handler = CancelExistingOrderHandler(order_id_to_cancel_q, maker_trader, stopped)

        canceled_order_handler.start()
        cancel_existing_order_handler.start()
        maker_trader.start()

        cancel_existing_order_handler.join()
        canceled_order_handler.join()
        maker_trader.join()

    except Exception:
        debugger.exception("FATAL")
    finally:
        debugger.info('프로그램을 종료합니다')

        close_program(id_)
        debugger.debug("DONE")
        os.system("PAUSE")
