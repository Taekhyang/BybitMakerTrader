import functools
import ntplib
import shutil
import certifi
import re
import datetime
import os
import time

from debugger import debugger


def _retry_on_fail(tries_count=3, delay=1):
    """
        decorator factory for getting additional option parameters `tries_count`, `delay`
        apart from original function's parameters
    """

    def decorating(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(1, tries_count + 1):
                last_try = True if i == tries_count else False
                res = func(*args, **kwargs, last_try=last_try)  # res could be <tuple> or <single value including(None, False)>
                if isinstance(res, tuple):
                    # if tuple element is None, then it means it failed
                    if res[0]:
                        return res

                elif res is not False and res is not None:
                    return res

                debugger.debug('failed function - [{}] retry again, try count - [{}]'.format(func.__name__, i))
                time.sleep(delay)
            else:
                return res

        return wrapper
    return decorating


def sync_time(_raise=False):
    try:
        debugger.info('시스템 시간 동기화를 진행합니다...')
        os.system('w32tm /resync')
        debugger.info('시간동기화 완료. 현재시간:{}'.format(time.time()))
        try:
            c = ntplib.NTPClient()
            response = c.request('time.google.com', version=3)
            debugger.debug('time.google.com: {}'.format(response.tx_time))
        except Exception:
            pass
    except Exception:
        debugger.debug("failed to sync time")
        if _raise:
            raise


def copy_cert(_raise=False):
    try:
        debugger.info("cert 파일을 복사합니다...")
        shutil.copyfile(certifi.where(), './cacert.pem')
        debugger.info("cert 파일 복사 완료")
    except Exception:
        debugger.debug("failed to copy cert")
        if _raise:
            raise


def datetime_parser(str):
    res = re.search(r'([\d]{4})-([\d]{2})-([\d]{2})T([\d]{2}):([\d]{2}):([\d]{2}).([\d]{1,6})[\d]*Z', str)
    return datetime.datetime(*[int(t) for t in res.groups()])


def duplicate_access_evt():
    pass


def user_check():
    pass


def close_program():
    pass
