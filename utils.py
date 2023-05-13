import logging
import threading
from logging.handlers import RotatingFileHandler
import os
import json

MULTITHREADING = True

LOG_DIR = r'./logs'
LOG_FILE = LOG_DIR + '/app.log'

DATA_DIR = r'./data'
SETTINGS_FILE = DATA_DIR + '/settings.json'
_DEFAULT_SETTINGS = {"api_key": "", "log_type": "DEBUG"}

STATE_READY = 'READY'
STATE_MUTUAL_FUND = 'MUTUAL_FUND'
STATE_ETF = 'ETF'
STATE_FINISHED = 'FINISHED'

PROGRESS_FILE = DATA_DIR + '/progress.json'
_DEFAULT_PROGRESS = {"screen_state": "READY", "offset": 0, "floor": -1}

_MAKE_DIRS = [LOG_DIR, DATA_DIR]
_MAKE_FILES = [(SETTINGS_FILE, _DEFAULT_SETTINGS), (PROGRESS_FILE, _DEFAULT_PROGRESS)]

for make_dir in _MAKE_DIRS:
    if not os.path.exists(make_dir):
        os.mkdir(make_dir)
for make_file in _MAKE_FILES:
    file, defaults = make_file
    if not os.path.exists(file):
        with open(file, 'a+') as new_file:
            json.dump(defaults, new_file)

settings = json.load(open(SETTINGS_FILE))

progress = json.load(open(PROGRESS_FILE))


def progress_revert_to_default():
    global progress
    progress = _DEFAULT_PROGRESS
    dump_progress()


def dump_progress():
    json.dump(progress, open(PROGRESS_FILE, 'w'))


logFormatter = logging.Formatter(fmt='%(asctime)s:%(threadName)s:%(levelname)s: %(message)s')

logger = logging.getLogger()
logger.setLevel(logging.getLevelNamesMapping()[settings['log_type']])

handler = RotatingFileHandler(LOG_FILE, maxBytes=1000000, backupCount=100)
handler.setFormatter(logFormatter)
logger.addHandler(handler)

if __name__ == '__main__':
    logger.debug('THIS IS A TEST')
    pass

test = 0