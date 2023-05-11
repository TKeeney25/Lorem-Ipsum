from time import sleep

import utils
import threading
from queue import Queue
from structures import ScreenerResponse, MSFinanceResponse, YHFinanceResponse, PerformanceIdResponse

MAX_WORKERS = 5
db_read_yh_event = threading.Event()
db_read_yh_data = None

db_write_queue = Queue()

screen_queue = Queue()
screen_incomplete = True
DEFAULT_FLOOR = -1
DEFAULT_JUMP = 250


def screen_fund(screen_type: str, offset=0, floor=DEFAULT_FLOOR, roof=DEFAULT_FLOOR + DEFAULT_JUMP) -> {}:
    ScreenerResponse('') # TODO
    return {"total_tickers": 100}


def screen_worker():
    while True:
        queue_value = screen_queue.get()
        if queue_value is None:
            screen_queue.task_done()
            return
        db_write_queue.put(queue_value)
        screen_queue.task_done()


def screen():
    global screen_incomplete
    screen_threads = []
    for i in range(0, MAX_WORKERS):
        screen_threads.append(
            threading.Thread(target=screen_worker, name=f'screen_worker_{i}'))

    for thread in screen_threads:
        thread.start()

    # TODO execution code
    screen_queue.put(('ETF', 0, -1, 249))
    for i in range(0, 100):
        pass
    #

    for i in range(0, MAX_WORKERS):
        screen_queue.put(None)

    screen_queue.join()
    for thread in screen_threads:
        thread.join()


def manage_db():
    global db_read_yh_data
    asdf = 0
    try:
        while True:
            if not db_read_yh_event.is_set():
                db_read_yh_data = asdf
                db_read_yh_event.set()

            while not db_write_queue.empty():
                write_queue_value = db_write_queue.get()
                if write_queue_value is None:
                    db_write_queue.task_done()
                    return
                # TODO here
                print(write_queue_value)
                #
                db_write_queue.task_done()
            asdf += 1
    except Exception as e:
        print(e)


def todo():
    pass


def main():
    screen_thread = threading.Thread(target=screen, name='screen_master')
    db_thread = threading.Thread(target=manage_db, name='db_master')
    ms_thread = threading.Thread(target=todo, name='ms_master')
    perf_id_thread = threading.Thread(target=todo, name='perf_id_master')
    yh_thread = threading.Thread(target=todo, name='yh_master')

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
    ms_threads = [(threading.Thread(target=screen_worker, name='ms_master'))]
    for i in range(0, MAX_WORKERS):
        ms_threads.append(threading.Thread(target=screen_worker, name=f'ms_worker_{i}'))

    yh_threads = [(threading.Thread(target=screen_worker, name='yh_master'))]

    for i in range(0, MAX_WORKERS):
        yh_threads.append(threading.Thread(target=screen_worker, name=f'yh_worker_{i}'))
    # endregion


if __name__ == '__main__':
    main()

# TODO think about concurrency with the threads. Once they receive an instruction it must be executed or the failure must be well documented.
# TODO make the consumers able to accept all calls for like sources.
# TODO as with the previous todo simplify the thread starting and joining.
