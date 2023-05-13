import json
import random
from time import sleep
import database
from http_requests import get_screen, gt_payload, btwn_payload, get_yh_info, get_perf_id, get_ms_info
import utils
from utils import logger
import threading
from queue import Queue, PriorityQueue
from structures import ScreenerResponse, MSFinanceResponse, YHFinanceResponse, PerformanceIdResponse

MAX_WORKERS = 5

db_read_yh_event = threading.Event()
db_read_yh_data = None

db_read_perf_event = threading.Event()
db_read_perf_data = None

db_read_ms_event = threading.Event()
db_read_ms_data = None

db_write_queue = Queue()

yh_queue = PriorityQueue()
yh_failures = 0
yh_failures_lock = threading.Lock()

ms_queue = PriorityQueue()
ms_failures = 0
ms_failures_lock = threading.Lock()

SCREEN_PRIORITY = 0
YH_PRIORITY = 0.5
PERF_ID_PRIORITY = 0
MS_PRIORITY = 0.5

screen_incomplete = True

MAX_TOTAL: int = 5000
DEFAULT_JUMP: int = 250
DEFAULT_FLOOR: int = -1
MAX_RESULTS: int = 50
DEFAULT_DELAYS = [0, 1, 5, 10, 60, 300, 600]

yh_screen_dynamic_total = 0
yh_screen_dynamic_total_lock = threading.Lock()
yh_screen_dynamic_total_update_event = threading.Event()


def screen_fund(screen_type: str, offset: int, floor: int, roof=None, updater=True) -> {}:
    global yh_screen_dynamic_total

    if roof is None:
        payload = gt_payload(floor)
    else:
        payload = btwn_payload(floor, roof)

    screen_data = get_screen(screen_type, offset, payload)
    screener_response = ScreenerResponse(screen_data)

    if updater:
        yh_screen_dynamic_total_lock.acquire()
        yh_screen_dynamic_total = screener_response.to_dict()['total']
        yh_screen_dynamic_total_update_event.set()
        yh_screen_dynamic_total_lock.release()

    return screener_response


# TODO find good solution to duplication
def yh_worker():
    global yh_failures
    while True:
        priority, content = yh_queue.get()
        method, args = content
        if method is None:
            yh_queue.task_done()
            return
        try:
            sleep(DEFAULT_DELAYS[yh_failures // MAX_WORKERS])
        except IndexError:
            print('Too many errors occurred in yh_worker. Aborting.')
            logger.exception('Too many errors occurred in yh_worker. Aborting.')
        method_return = method(*args)
        if method_return is None:
            yh_failures_lock.acquire()
            yh_failures += 1
            logger.error(f'Bad return in yh_worker. Failures: {yh_failures}. Priority: {priority}. Content: {content}.')
            yh_failures_lock.release()
            yh_queue.put((priority + 1, content))
        else:
            yh_failures = 0  # Race conditions do not matter
            db_write_queue.put(method_return)
        yh_queue.task_done()


def ms_worker():
    global ms_failures
    while True:
        priority, content = ms_queue.get()
        method, args = content
        if method is None:
            ms_queue.task_done()
            return
        try:
            sleep(DEFAULT_DELAYS[ms_failures // MAX_WORKERS])
        except IndexError:
            print('Too many errors occurred in ms_worker. Aborting.')
            logger.exception('Too many errors occurred in ms_worker. Aborting.')
        method_return = method(*args)
        if method_return is None:
            ms_failures_lock.acquire()
            ms_failures += 1
            logger.error(f'Bad return in ms_worker. Failures: {ms_failures}. Priority: {priority}. Content: {content}.')
            ms_failures_lock.release()
            ms_queue.put((priority + 1, content))
        else:
            ms_failures = 0  # Race conditions do not matter
            db_write_queue.put(method_return)
        ms_queue.task_done()


def drop_roof(floor, roof, rate_of_approach=4):
    return roof - (roof - floor) / rate_of_approach


def screen_master():
    global screen_incomplete, yh_screen_dynamic_total
    if utils.progress['screen_state'] == utils.STATE_FINISHED:
        return

    for screen_state in [utils.STATE_MUTUAL_FUND, utils.STATE_ETF]:
        if utils.progress['screen_state'] == screen_state:
            current_floor = utils.progress['floor']
            starting_offset = utils.progress['offset']
        elif screen_state == utils.STATE_MUTUAL_FUND and utils.progress['screen_state'] == utils.STATE_ETF:
            continue
        else:
            current_floor = DEFAULT_FLOOR
            starting_offset = 0

        utils.progress['screen_state'] = screen_state

        remaining_total = MAX_TOTAL + 1
        while remaining_total > MAX_TOTAL:
            current_roof = current_floor + DEFAULT_JUMP
            screen_result = screen_fund(screen_state, starting_offset, current_floor)
            screen_result_dict = screen_result.to_dict()
            remaining_total = screen_result_dict['total']
            db_write_queue.put(screen_result)

            yh_screen_dynamic_total_lock.acquire()
            yh_screen_dynamic_total = remaining_total
            yh_screen_dynamic_total_lock.release()

            utils.progress['floor'] = current_floor

            yh_screen_dynamic_total_update_event.set()
            for bottom_offset in range(starting_offset, MAX_TOTAL, 50 * MAX_WORKERS):
                utils.progress['offset'] = bottom_offset
                utils.dump_progress()

                yh_screen_dynamic_total_update_event.wait()
                yh_screen_dynamic_total_update_event.clear()
                yh_screen_dynamic_total_lock.acquire()
                yh_screen_dynamic_total = 249  # TODO only leave in place for testing
                print(f'{bottom_offset}/{yh_screen_dynamic_total}:{remaining_total}')
                logger.debug(
                    f'{bottom_offset}/{yh_screen_dynamic_total}:{remaining_total}')
                if yh_screen_dynamic_total > MAX_TOTAL:
                    current_roof = drop_roof(current_floor, current_roof)

                if bottom_offset > yh_screen_dynamic_total:
                    yh_screen_dynamic_total_lock.release()
                    break
                else:
                    yh_screen_dynamic_total_lock.release()

                for offset_mod in range(0, 50 * MAX_WORKERS, 50):
                    logger.debug(
                        f'yh_queue.put:screen_fund:{screen_state}:{bottom_offset + offset_mod}:{current_floor}:{current_roof}:{offset_mod == 0}')
                    yh_queue.put((SCREEN_PRIORITY, (screen_fund, (
                        screen_state, bottom_offset + offset_mod, current_floor, current_roof, offset_mod == 0
                    ))))
            current_floor = current_roof
            starting_offset = 0
    utils.progress['screen_state'] = utils.STATE_FINISHED
    utils.dump_progress()


# TODO find good solution to duplication
def fetch_yh_fund(fund):
    return YHFinanceResponse(get_yh_info(fund))


def fetch_perf_id(fund):
    return PerformanceIdResponse(get_perf_id(fund))


def fetch_ms_fund(fund):
    return MSFinanceResponse(get_ms_info(fund))


def yh_master():
    already_queued = set()
    while True:
        db_read_yh_event.wait()
        valid_funds = db_read_yh_data
        db_read_yh_event.clear()
        for fund in valid_funds:
            if fund in already_queued:
                continue
            already_queued.add(fund)
            yh_queue.put((YH_PRIORITY, (fetch_yh_fund, (fund,))))


def perf_id_master():
    already_queued = set()
    while True:
        db_read_perf_event.wait()
        valid_funds = db_read_perf_data
        db_read_perf_event.clear()
        for fund in valid_funds:
            if fund in already_queued:
                continue
            already_queued.add(fund)
            ms_queue.put((PERF_ID_PRIORITY, (fetch_perf_id, (fund,))))


def ms_master():
    already_queued = set()
    while True:
        db_read_ms_event.wait()
        valid_funds = db_read_ms_data
        db_read_ms_event.clear()
        for fund in valid_funds:
            if fund in already_queued:
                continue
            already_queued.add(fund)
            ms_queue.put((MS_PRIORITY, (fetch_ms_fund, (fund,))))


def manage_db():
    global db_read_yh_data, db_read_ms_data, db_read_perf_data
    db = database.db_start()
    db.drop_tables()  # TODO remove when not testing
    db.create_tables()
    try:
        while True:
            if not db_read_yh_event.is_set():
                db_read_yh_data = db.valid_for_yh_finance_view()
                db_read_yh_event.set()
            if not db_read_perf_event.is_set():
                db_read_perf_data = db.valid_for_perf_id_view()
                db_read_perf_event.set()
            if not db_read_ms_event.is_set():
                db_read_ms_data = db.valid_for_ms_finance_view()
                db_read_ms_event.set()
            while not db_write_queue.empty():
                write_queue_value = db_write_queue.get()
                if write_queue_value is None:
                    db_write_queue.task_done()
                    db.close_connections()
                    return
                if isinstance(write_queue_value, ScreenerResponse):
                    db.add_from_screener(write_queue_value.to_dict()['quotes'])
                elif isinstance(write_queue_value, YHFinanceResponse):
                    db.update_from_yh_finance(write_queue_value.to_dict())
                elif isinstance(write_queue_value, PerformanceIdResponse):
                    db.update_performance_id(write_queue_value.to_dict())
                elif isinstance(write_queue_value, MSFinanceResponse):
                    db.update_from_ms_finance(write_queue_value.to_dict())
                db_write_queue.task_done()
    except Exception as e:
        print(e)


def todo():
    pass


def join_workers(queue: PriorityQueue, workers: []) -> None:
    for _ in workers:
        queue.put((100, (None, None)))
    for worker in workers:
        worker.join()


def main():
    screen_thread = threading.Thread(target=screen_master, name='screen_master')
    db_thread = threading.Thread(target=manage_db, name='db_master')
    ms_thread = threading.Thread(target=ms_master, name='ms_master')
    perf_id_thread = threading.Thread(target=perf_id_master, name='perf_id_master')
    yh_thread = threading.Thread(target=yh_master, name='yh_master')

    yh_workers = []
    for i in range(0, MAX_WORKERS):
        yh_workers.append(threading.Thread(target=yh_worker, name=f'yh_worker_{i}'))

    ms_workers = []
    for i in range(0, MAX_WORKERS):
        ms_workers.append(threading.Thread(target=ms_worker, name=f'ms_worker_{i}'))

    db_thread.start()
    for worker in yh_workers:
        worker.start()
    for worker in ms_workers:
        worker.start()
    screen_thread.start()
    yh_thread.start()
    perf_id_thread.start()
    ms_thread.start()
    screen_thread.join()
    perf_id_thread.join()
    yh_thread.join()
    ms_thread.join()
    join_workers(yh_queue, yh_workers)
    join_workers(ms_queue, ms_workers)
    db_write_queue.put(None)
    db_thread.join()
    return


if __name__ == '__main__':
    main()

# TODO find a way to speed limit the threads to 5 requests/second