import threading
from queue import Queue, PriorityQueue
from time import sleep

import database
import utils
from utils import logger
from http_requests import get_screen, gt_payload, btwn_payload, get_yh_info, get_perf_id, get_ms_info
from structures import ScreenerResponse, MSFinanceResponse, YHFinanceResponse, PerformanceIdResponse

MAX_WORKERS = 5

db_write_queue = Queue()

yh_queue = PriorityQueue()
ms_queue = PriorityQueue()

SCREEN_PRIORITY = 0
YH_PRIORITY = 0.5
PERF_ID_PRIORITY = 0
MS_PRIORITY = 0.5
DEATH_PRIORITY = 10

MAX_TOTAL: int = 5000
DEFAULT_JUMP: int = 250
DEFAULT_FLOOR: int = -1
MAX_RESULTS: int = 50
DEFAULT_DELAYS = [0, 1, 5, 10, 60, 300, 600]

yh_screen_dynamic_total = 0
yh_screen_dynamic_total_lock = threading.Lock()
yh_screen_dynamic_total_update_event = threading.Event()

program_ended = False


class DataTree:
    def __init__(self, parent=None, data_source=''):
        self.parent: [DataTree, None] = parent
        if parent is not None:
            parent.children.append(self)
        self.children = []
        self.data_source = data_source
        self.data = []
        self.event = threading.Event()
        self.incomplete = True


def update_api_calls(access_controllers):
    for controller in access_controllers:
        with controller.api_calls_remaining_lock:
            controller.api_calls_remaining = 4
            controller.api_calls_remaining_condition.notify(4)
    if not program_ended:
        threading.Timer(1, update_api_calls, (access_controllers,)).start()


def screen_fund(screen_type: str, offset: int, floor: int, roof=None, updater=True) -> {}:
    global yh_screen_dynamic_total

    if roof is None:
        payload = gt_payload(floor)
    else:
        payload = btwn_payload(floor, roof)

    screen_data = get_screen(screen_type, offset, payload)
    if screen_data is None:
        return None
    screener_response = ScreenerResponse(screen_data)

    if updater:
        yh_screen_dynamic_total_lock.acquire()
        yh_screen_dynamic_total = screener_response.to_dict()['total']
        yh_screen_dynamic_total_update_event.set()
        yh_screen_dynamic_total_lock.release()

    return screener_response


class ApiAccessController:
    def __init__(self):
        self.api_calls_remaining = 0
        self.api_calls_remaining_lock = threading.Lock()
        self.api_calls_remaining_condition = threading.Condition(self.api_calls_remaining_lock)
        self.failures = 0
        self.failures_lock = threading.Lock()


def worker_thread(name, work_queue: PriorityQueue, db_queue: Queue, api_access_controller: ApiAccessController):
    while True:
        with api_access_controller.api_calls_remaining_lock:
            while api_access_controller.api_calls_remaining <= 0:
                api_access_controller.api_calls_remaining_condition.wait()
            api_access_controller.api_calls_remaining -= 1
        priority, content = work_queue.get()
        method, args = content
        if method is None:
            work_queue.task_done()
            return
        try:
            sleep(DEFAULT_DELAYS[api_access_controller.failures // MAX_WORKERS])
        except IndexError:
            print(f'Too many errors occurred in {name}. Aborting.')
            logger.exception(f'Too many errors occurred in {name}. Aborting.')
        method_return = method(*args)
        if method_return is None:
            with api_access_controller.failures_lock:
                api_access_controller.failures += 1
                logger.error(
                    f'''Bad return in {name}.
                        Failures: {api_access_controller.failures}.
                        Priority: {priority}.
                        Content: {content}.''')
                work_queue.put((priority + 1, content))
        else:
            api_access_controller.failures = 0  # Race conditions do not matter
            db_queue.put(method_return)
        work_queue.task_done()


def drop_roof(floor, roof, rate_of_approach=4):
    return roof - (roof - floor) / rate_of_approach


def screen_master(data_tree: DataTree):
    global yh_screen_dynamic_total
    if utils.progress['screen_state'] == utils.STATE_FINISHED:
        data_tree.incomplete = False
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
            remaining_total = 249  # TODO only leave in place for testing

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
                if yh_screen_dynamic_total > MAX_TOTAL:
                    current_roof = drop_roof(current_floor, current_roof)

                if bottom_offset > yh_screen_dynamic_total:
                    yh_screen_dynamic_total_lock.release()
                    break
                else:
                    yh_screen_dynamic_total_lock.release()

                for offset_mod in range(0, 50 * MAX_WORKERS, 50):
                    logger.debug(
                        f'''yh_queue.put:screen_fund:{screen_state}:{bottom_offset + offset_mod}:{current_floor}
                        :{current_roof}:{offset_mod == 0}''')
                    yh_queue.put((SCREEN_PRIORITY, (screen_fund, (
                        screen_state, bottom_offset + offset_mod, current_floor, current_roof, offset_mod == 0
                    ))))
            current_floor = current_roof
            starting_offset = 0
    utils.progress['screen_state'] = utils.STATE_FINISHED
    utils.dump_progress()
    data_tree.incomplete = False


class BadFund:
    def __init__(self, symbol):
        self.symbol = symbol

    def __str__(self):
        return self.symbol


def fetch_yh_fund(fund):
    data = get_yh_info(fund)
    if data is None:
        return None
    try:
        return YHFinanceResponse(data)
    except KeyError:
        logger.debug(data)
        return BadFund(fund)


def fetch_perf_id(fund):
    data = get_perf_id(fund)
    if data is None:
        return None
    result = None
    for entry in data['results']:
        if entry['ticker'] == fund:
            result = entry
            break
    if result is None:
        return BadFund(fund)
    return PerformanceIdResponse(result)


def fetch_ms_fund(fund):
    data = get_ms_info(fund)
    if data is None:
        return None
    return MSFinanceResponse(data[0])


def master_thread(queue: PriorityQueue, priority: float, data_source: DataTree, worker):
    already_queued = set()
    final_check = True
    while data_source.parent.incomplete or final_check:
        final_check = data_source.parent.incomplete
        data_source.event.wait()
        data = data_source.data
        data_source.event.clear()
        for entry in data:
            if entry in already_queued:
                continue
            already_queued.add(entry)
            queue.put((priority, (worker, (entry,))))
    if len(data_source.children) == 0:
        for _ in range(0, MAX_WORKERS):
            queue.put((DEATH_PRIORITY, (None, None)))
        queue.join()
    data_source.incomplete = False


def manage_db(data_trees):
    db = database.DB()
    db.create_tables()
    while True:
        write_queue_value = None
        try:
            for tree in data_trees:
                if not tree.event.is_set():
                    if tree.data_source == 'yh':
                        tree.data = db.valid_for_yh_finance_view()
                    elif tree.data_source == 'perf':
                        tree.data = db.valid_for_perf_id_view()
                    elif tree.data_source == 'ms':
                        tree.data = db.valid_for_ms_finance_view()
                    tree.event.set()
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
                elif isinstance(write_queue_value, BadFund):
                    logger.error(f'Bad Fund: {write_queue_value.symbol}')
                    db.delete_fund(write_queue_value.symbol)
                db_write_queue.task_done()
        except Exception as e:
            print(e)
            logger.exception(f'Value: {write_queue_value}, Exception: {e}')
            sleep(1)


def debug_aid(*args):
    print_str = ''
    for arg in args:
        print_str += f'|{arg.name}, {arg.is_alive()}|'
    print(print_str)
    if not program_ended:
        threading.Timer(5, debug_aid, args).start()


def main():
    # debug_aid(db_thread, *threads)
    global program_ended
    screen_data_tree = DataTree()
    yh_data_tree = DataTree(screen_data_tree, 'yh')
    perf_id_data_tree = DataTree(screen_data_tree, 'perf')
    ms_data_tree = DataTree(perf_id_data_tree, 'ms')

    yh_access_control = ApiAccessController()
    ms_access_control = ApiAccessController()

    db_thread = threading.Thread(target=manage_db, name='db_master',
                                 args=([yh_data_tree, perf_id_data_tree, ms_data_tree],))

    threads = [
        threading.Thread(target=screen_master, name='screen_master', args=(screen_data_tree,)),
        threading.Thread(target=master_thread, name='ms_master',
                         args=(ms_queue, MS_PRIORITY, ms_data_tree, fetch_ms_fund)),
        threading.Thread(target=master_thread, name='perf_id_master',
                         args=(ms_queue, PERF_ID_PRIORITY, perf_id_data_tree, fetch_perf_id)),
        threading.Thread(target=master_thread, name='yh_master',
                         args=(yh_queue, YH_PRIORITY, yh_data_tree, fetch_yh_fund)),
    ]
    for i in range(0, MAX_WORKERS):
        threads += [
            threading.Thread(target=worker_thread,
                             name=f'yh_worker_{i}',
                             args=(f'yh_worker_{i}', yh_queue, db_write_queue, yh_access_control)),
            threading.Thread(target=worker_thread,
                             name=f'ms_worker_{i}',
                             args=(f'ms_worker_{i}', ms_queue, db_write_queue, ms_access_control))
        ]

    update_api_calls([yh_access_control, ms_access_control])
    db_thread.start()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    db_write_queue.put(None)
    db_thread.join()
    program_ended = True
    return


if __name__ == '__main__':
    main()
