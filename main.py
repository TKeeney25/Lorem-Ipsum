import json
import random
from time import sleep

import requests

import screen
from utils import logger
import threading
from queue import Queue
from structures import ScreenerResponse, MSFinanceResponse, YHFinanceResponse, PerformanceIdResponse

MAX_WORKERS = 5
# db_read_yh_event = threading.Event()
# db_read_yh_data = None

db_write_queue = Queue()

yh_queue = Queue()

screen_incomplete = True

MAX_TOTAL: int = 5000
DEFAULT_JUMP: int = 250
DEFAULT_FLOOR: int = -1
MAX_RESULTS: int = 50
DEFAULT_DELAYS = [1, 5, 10, 60, 300, 600]

yh_screen_dynamic_total = 0
yh_screen_dynamic_total_lock = threading.Lock()
yh_screen_dynamic_total_update_event = threading.Event()
yh_screen_total_funds_acquired_lock = threading.Lock()
yh_screen_total_funds_acquired = 0


def get_screen(quote_type: str, offset: int, payload: json) -> dict:
    url = "https://yh-finance.p.rapidapi.com/screeners/list"
    querystring = {"quoteType": quote_type, "sortField": "intradayprice", "region": "US", "size": "50",
                   "offset": offset,
                   "sortType": "ASC"}
    for delay in DEFAULT_DELAYS:

        # for testing
        with open('./tests/defaults/screen_data.json') as screen_data:
            json_response = json.load(screen_data)
            if payload[0]['operands'][0]['operator'] == 'gt':
                json_response['finance']['result'][0]['total'] = 10001
            else:
                json_response['finance']['result'][0]['total'] = int((payload[0]['operands'][0]['operands'][2] -
                                                                  payload[0]['operands'][0]['operands'][1]) * 40)
            if offset+50 >= json_response['finance']['result'][0]['total']:
                json_response['finance']['result'][0]['count'] = json_response['finance']['result'][0]['total'] - offset
            return json_response
        # end testing

        response = requests.request("POST", url, json=payload, headers=screen.YH_HEADERS, params=querystring)
        try:
            json_response = json.loads(response.text)  # TODO add bad response handling
            if json_response:
                return json_response
        except Exception as e:
            logger.exception(e)
        sleep(delay)


def screen_fund(screen_type: str, offset: int, floor: int, roof=None, updater=True) -> {}:
    global yh_screen_dynamic_total, yh_screen_total_funds_acquired

    if roof is None:
        payload = screen.gt_payload(floor)
    else:
        payload = screen.btwn_payload(floor, roof)

    screen_data = get_screen(screen_type, offset, payload)
    screener_response = ScreenerResponse(screen_data)
    screener_response_dict = screener_response.to_dict()

    yh_screen_total_funds_acquired_lock.acquire()
    yh_screen_total_funds_acquired += screener_response_dict['count']
    yh_screen_total_funds_acquired_lock.release()

    if updater:
        yh_screen_dynamic_total_lock.acquire()
        yh_screen_dynamic_total = screener_response_dict['total']
        yh_screen_dynamic_total_update_event.set()
        yh_screen_dynamic_total_lock.release()

    return screener_response


def yh_worker():
    while True:
        method, args = yh_queue.get()
        if method is None:
            yh_queue.task_done()
            return
        db_write_queue.put(method(*args))
        yh_queue.task_done()


def drop_roof(floor, roof, rate_of_approach=4):
    return roof - (roof - floor) / rate_of_approach


def screen_master():
    global screen_incomplete, yh_screen_dynamic_total, yh_screen_total_funds_acquired
    screen_threads = []
    for i in range(0, MAX_WORKERS):
        screen_threads.append(
            threading.Thread(target=yh_worker, name=f'screen_worker_{i}'))

    for thread in screen_threads:
        thread.start()

    for screen_type in ['MUTUAL_FUND', 'ETF']:
        current_floor = DEFAULT_FLOOR
        current_roof = DEFAULT_FLOOR + DEFAULT_JUMP

        screen_result = screen_fund(screen_type, 0, DEFAULT_FLOOR).to_dict()
        yh_screen_total = screen_result['total']

        yh_screen_dynamic_total_lock.acquire()
        yh_screen_total_funds_acquired_lock.acquire()
        yh_screen_dynamic_total = yh_screen_total
        yh_screen_total_funds_acquired = 0
        yh_screen_total_funds_acquired_lock.release()
        yh_screen_dynamic_total_lock.release()


        while yh_screen_total_funds_acquired < yh_screen_total:
            yh_screen_dynamic_total_update_event.set()
            for bottom_offset in range(0, MAX_TOTAL, 50 * MAX_WORKERS):

                yh_screen_dynamic_total_update_event.wait()
                yh_screen_dynamic_total_update_event.clear()
                yh_screen_dynamic_total_lock.acquire()
                print(f'{bottom_offset}/{yh_screen_dynamic_total}, {yh_screen_total_funds_acquired}/{yh_screen_total}')
                logger.debug(f'{bottom_offset}/{yh_screen_dynamic_total}, {yh_screen_total_funds_acquired}/{yh_screen_total}')
                if yh_screen_dynamic_total > MAX_TOTAL:
                    current_roof = drop_roof(current_floor, current_roof)

                if bottom_offset > yh_screen_dynamic_total:
                    yh_screen_dynamic_total_lock.release()
                    break
                else:
                    yh_screen_dynamic_total_lock.release()

                for offset_mod in range(0, 50 * MAX_WORKERS, 50):
                    logger.debug(
                        f'yh_queue.put:screen_fund:{screen_type}:{bottom_offset + offset_mod}:{current_floor}:{current_roof}:{offset_mod == 0}')
                    yh_queue.put((screen_fund, (
                        screen_type, bottom_offset + offset_mod, current_floor, current_roof, offset_mod == 0
                    )))
            current_floor = current_roof
            current_roof += DEFAULT_JUMP

    for i in range(0, MAX_WORKERS):
        yh_queue.put((None, None))

    yh_queue.join()
    for thread in screen_threads:
        thread.join()


def manage_db():
    global db_read_yh_data
    asdf = 0
    try:
        while True:
            # if not db_read_yh_event.is_set():
            #    db_read_yh_data = asdf
            #    db_read_yh_event.set()

            while not db_write_queue.empty():
                write_queue_value = db_write_queue.get()
                if write_queue_value is None:
                    db_write_queue.task_done()
                    return
                # TODO here
                if type(write_queue_value) is ScreenerResponse:
                    # logger.debug(write_queue_value.to_dict())
                    # print(write_queue_value.to_dict())
                    pass
                #
                db_write_queue.task_done()
            asdf += 1
    except Exception as e:
        print(e)


def todo():
    pass


def busy_work():
    sleep(2 * random.random())
    yh_queue.put((busy_work, ()))
    out = f'DT: {yh_screen_dynamic_total_lock.locked()}, FA: {yh_screen_total_funds_acquired_lock.locked()}, UE: {yh_screen_dynamic_total_update_event.is_set()}'
    # logger.debug(out)
    # print(out)
    return -1


def main():
    screen_thread = threading.Thread(target=screen_master, name='screen_master')
    db_thread = threading.Thread(target=manage_db, name='db_master')
    ms_thread = threading.Thread(target=todo, name='ms_master')
    perf_id_thread = threading.Thread(target=todo, name='perf_id_master')
    yh_thread = threading.Thread(target=todo, name='yh_master')

    db_thread.start()
    screen_thread.start()
    screen_thread.join()
    db_write_queue.put(None)
    db_thread.join()
    return

    db_thread.start()
    screen_thread.start()
    ms_thread.start()
    screen_thread.join()
    yh_thread.start()
    ms_thread.join()
    perf_id_thread.start()
    perf_id_thread.join()
    ms_thread.start()
    ms_thread.join()
    yh_thread.join()
    db_write_queue.put(None)
    db_thread.join()

    # region todo
    ms_threads = [(threading.Thread(target=yh_worker, name='ms_master'))]
    for i in range(0, MAX_WORKERS):
        ms_threads.append(threading.Thread(target=yh_worker, name=f'ms_worker_{i}'))

    yh_threads = [(threading.Thread(target=yh_worker, name='yh_master'))]

    for i in range(0, MAX_WORKERS):
        yh_threads.append(threading.Thread(target=yh_worker, name=f'yh_worker_{i}'))
    # endregion


if __name__ == '__main__':
    main()

# TODO think about concurrency with the threads. Once they receive an instruction it must be executed or the failure must be well documented.
# TODO make the consumers able to accept all calls for like sources.
# TODO as with the previous todo simplify the thread starting and joining.
