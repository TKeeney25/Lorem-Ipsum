import datetime
import logging
from logging.handlers import RotatingFileHandler
import os
import json

LOG_DIR = r'./logs'
LOG_FILE = LOG_DIR + '/app.log'

DATA_DIR = r'./data'
SETTINGS_FILE = DATA_DIR + '/settings.json'
_DEFAULT_SETTINGS = {"api_key": "",
                     "log_type": "DEBUG",
                     "max_ms_calls": 50000,
                     "max_yh_calls": 50000,
                     "gmail_secret": "",
                     "from": "",
                     "send_debug": [""],
                     "send_start": [""],
                     "send_complete": [""],
                     "last_month_ran": -1,
                     "headers": [""]
                     }

STATE_READY = 'READY'
STATE_MUTUAL_FUND = 'MUTUAL_FUND'
STATE_ETF = 'ETF'
STATE_FINISHED = 'FINISHED'

PROGRESS_FILE = DATA_DIR + '/progress.json'
_DEFAULT_PROGRESS = {"screen_state": "READY",
                     "offset": 0,
                     "floor": -1,
                     "yh_api_calls": 0,
                     "ms_api_calls": 0
                     }

VALID_FUNDS_FILE = DATA_DIR + '/valid_funds.sql'
OUTPUT_FUNDS_FILE = DATA_DIR + '/output_funds.sql'

TICKERS_FILE = DATA_DIR + '/tickers.csv'

_MAKE_DIRS = [LOG_DIR, DATA_DIR]
_MAKE_FILES = [(SETTINGS_FILE, _DEFAULT_SETTINGS),
               (PROGRESS_FILE, _DEFAULT_PROGRESS),
               (VALID_FUNDS_FILE, None),
               (OUTPUT_FUNDS_FILE, None),
               (TICKERS_FILE, None)]

for make_dir in _MAKE_DIRS:
    if not os.path.exists(make_dir):
        os.mkdir(make_dir)
for make_file in _MAKE_FILES:
    file, defaults = make_file
    if not os.path.exists(file):
        with open(file, 'a+') as new_file:
            if defaults is None:
                continue
            json.dump(defaults, new_file, indent=4)

with open(SETTINGS_FILE) as settings_file:
    settings = json.load(settings_file)

with open(PROGRESS_FILE) as progress_file:
    progress = json.load(open(PROGRESS_FILE))

with open(VALID_FUNDS_FILE) as valid_funds_file:
    valid_funds = valid_funds_file.read()

with open(OUTPUT_FUNDS_FILE) as output_funds_file:
    output_funds = output_funds_file.read()


def dump_progress():
    with open(PROGRESS_FILE, 'w') as progress_file:
        json.dump(progress, progress_file, indent=4)

def dump_settings():
    with open(SETTINGS_FILE, 'w') as settings_file:
        json.dump(settings, settings_file, indent=4)


def reset_progress():
    global progress
    progress = _DEFAULT_PROGRESS
    dump_progress()


logFormatter = logging.Formatter(fmt='%(asctime)s:%(threadName)s:%(levelname)s: %(message)s')

logger = logging.getLogger()
logger.setLevel(logging.getLevelNamesMapping()[settings['log_type']])

handler = RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024, backupCount=50)
handler.setFormatter(logFormatter)
logger.addHandler(handler)

if settings['last_month_ran'] != datetime.datetime.today().month:
    reset_progress()
    settings['last_month_ran'] = datetime.datetime.today().month
    dump_settings()

if __name__ == '__main__':
    pass
