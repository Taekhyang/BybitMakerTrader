import logging
import datetime
import os
import sys

from logging.handlers import RotatingFileHandler


debugger = logging.getLogger('Debugger')
debugger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(lineno)d - %(message)s")

try:
    now = str(datetime.datetime.now()).replace(':', '')
    if not os.path.isdir('./logs'):
        os.mkdir('./logs')
    os.mkdir('./logs/log-{}'.format(now))
    f_hdlr = RotatingFileHandler('./logs/log-{}/Debugger.log'.format(now), encoding='UTF-8', maxBytes=10 * 1024 * 1024,
                                 backupCount=50)
except:
    f_hdlr = RotatingFileHandler('Debugger.log', encoding='UTF-8', maxBytes=10 * 1024 * 1024, backupCount=50)
f_hdlr.setFormatter(formatter)
f_hdlr.setLevel(logging.DEBUG)

debugger.addHandler(f_hdlr)

formatter = logging.Formatter("[%(asctime)s] %(message)s")
s_hdlr = logging.StreamHandler()
s_hdlr.setFormatter(formatter)
if 'pydevd' in sys.modules:
    s_hdlr.setLevel(logging.DEBUG)
else:
    s_hdlr.setLevel(logging.INFO)

debugger.addHandler(s_hdlr)


def unhandled_exception(exctype, value, tb):
    debugger.exception("FATAL", exc_info=(exctype, value, tb))


sys.excepthook = unhandled_exception
 