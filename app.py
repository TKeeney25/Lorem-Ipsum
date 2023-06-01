import csv
import threading
from dataclasses import field, dataclass
from queue import Queue, PriorityQueue
from time import sleep
import argparse
from typing import Any

import requests

import database
import mail
import morningstar_scraper
import utils
from utils import logger
from http_requests import get_screen, gt_payload, btwn_payload, get_yh_info, get_perf_id, get_ms_info
from structures import ScreenerResponse, MSFinanceResponse, YHFinanceResponse, PerformanceIdResponse, \
    MSFundTrailingReturnsResponse, MSStockTrailingReturnsResponse

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

STATUS_LATENCY = 10

yh_screen_dynamic_total = 0
yh_screen_dynamic_total_lock = threading.Lock()
yh_screen_dynamic_total_update_event = threading.Event()

yh_api_calls_lock = threading.Lock()
ms_api_calls_lock = threading.Lock()

kill_event = threading.Event()

unchecked_exceptions = []

program_ended = False


class DataTree:
    def __init__(self, parent=None, data_source='', **kwargs):
        self.parent: [DataTree, None] = parent
        if parent is not None:
            parent.children.append(self)
        self.children = []
        self.data_source = data_source
        self.data = {}
        self.incomplete = True
        self.kwargs = kwargs


def update_api_calls(access_controllers):
    for controller in access_controllers:
        with controller.api_calls_remaining_lock:
            controller.api_calls_remaining = 1
            controller.api_calls_remaining_condition.notify()
    if not program_ended:
        threading.Timer(.25, update_api_calls, (access_controllers,)).start()


class ApiAccessController:
    def __init__(self):
        self.api_calls_remaining = 0
        self.api_calls_remaining_lock = threading.Lock()
        self.api_calls_remaining_condition = threading.Condition(self.api_calls_remaining_lock)
        self.failures = 0
        self.failures_lock = threading.Lock()


def worker_thread(
        name,
        work_queue: PriorityQueue,
        db_queue: Queue,
        api_access_controller: ApiAccessController,
        session: requests.Session):
    try:
        while not kill_event.is_set():
            try:
                sleep(DEFAULT_DELAYS[api_access_controller.failures // MAX_WORKERS])
            except IndexError:
                print(f'Too many errors occurred in {name}. Aborting.')
                logger.exception(f'Too many errors occurred in {name}. Aborting.')

            obj: PrioritizedItem = work_queue.get()
            priority = obj.priority
            content = obj.item
            logger.debug(f'P:{priority} C:{content}')
            print(f'P:{priority} C:{content}')
            method, args = content
            if method is None:
                work_queue.task_done()
                session.close()
                logger.debug('Thread Complete.')
                return
            with api_access_controller.api_calls_remaining_lock:
                while api_access_controller.api_calls_remaining <= 0:
                    api_access_controller.api_calls_remaining_condition.wait()
                api_access_controller.api_calls_remaining -= 1
            try:
                method_return = method(args, session=session)
            except requests.ReadTimeout:
                method_return = None
            if method_return is None:
                with api_access_controller.failures_lock:
                    api_access_controller.failures += 1
                    logger.error(
                        f'''Bad return in {name}.
                            Failures: {api_access_controller.failures}.
                            Priority: {priority}.
                            Content: {content}.''')
                    work_queue.put(PrioritizedItem(priority + 1, content))
            else:
                api_access_controller.failures = 0  # Race conditions do not matter
                db_queue.put(method_return)
            work_queue.task_done()
    except Exception as unchecked_exception:
        unchecked_exceptions.append(unchecked_exception)
        raise unchecked_exception


def drop_roof(floor, roof, rate_of_approach=4):
    return roof - (roof - floor) / rate_of_approach


def screen_master(data_tree: DataTree):
    try:
        global yh_screen_dynamic_total
        if utils.progress['screen_state'] == utils.STATE_FINISHED:
            data_tree.incomplete = False
            logger.debug('Screen Complete')
            return

        with requests.Session() as session:
            for screen_state in [utils.STATE_MUTUAL_FUND, utils.STATE_ETF]:
                if utils.progress['screen_state'] == screen_state:
                    logger.debug('Picking up from left off.')
                    current_floor = utils.progress['floor']
                    starting_offset = utils.progress['offset']
                elif screen_state == utils.STATE_MUTUAL_FUND and utils.progress['screen_state'] == utils.STATE_ETF:
                    logger.debug('Skipping Mutual Funds.')
                    continue
                else:
                    current_floor = DEFAULT_FLOOR
                    starting_offset = 0

                utils.progress['screen_state'] = screen_state

                remaining_total = MAX_TOTAL + 1
                while remaining_total > MAX_TOTAL:
                    current_roof = current_floor + DEFAULT_JUMP
                    screen_result = screen_fund(screen_state, starting_offset, current_floor, session=session)
                    screen_result_dict = screen_result.to_dict()
                    remaining_total = screen_result_dict['total']

                    db_write_queue.put(screen_result)

                    yh_screen_dynamic_total_lock.acquire()
                    yh_screen_dynamic_total = remaining_total
                    yh_screen_dynamic_total_lock.release()

                    utils.progress['floor'] = current_floor

                    yh_screen_dynamic_total_update_event.set()
                    for bottom_offset in range(starting_offset, MAX_TOTAL, MAX_RESULTS * MAX_WORKERS):
                        utils.progress['offset'] = bottom_offset
                        utils.dump_progress()

                        while not yh_screen_dynamic_total_update_event.wait(5):
                            if kill_event.is_set():
                                return
                        yh_screen_dynamic_total_update_event.clear()
                        yh_screen_dynamic_total_lock.acquire()
                        logger.debug(f'{bottom_offset}/{yh_screen_dynamic_total}:{remaining_total}')
                        if yh_screen_dynamic_total > MAX_TOTAL:
                            current_roof = drop_roof(current_floor, current_roof)

                        if bottom_offset > yh_screen_dynamic_total:
                            yh_screen_dynamic_total_lock.release()
                            break
                        else:
                            yh_screen_dynamic_total_lock.release()

                        for offset_mod in range(0, MAX_RESULTS * MAX_WORKERS, MAX_RESULTS):
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
        logger.debug('Screen Complete')
    except Exception as unchecked_exception:
        unchecked_exceptions.append(unchecked_exception)
        raise unchecked_exception


class BadFund:
    def __init__(self, symbol=None, perf_id=None):
        logger.debug(f'Symbol/Perf_id {symbol}/{perf_id} is bad.')
        self.symbol = symbol
        self.perf_id = perf_id

    def __str__(self):
        return self.symbol


class AddFund:
    def __init__(self, symbol):
        self.symbol = symbol

    def __str__(self):
        return self.symbol


def screen_fund(screen_type: str, offset: int, floor: int, roof=None, updater=True, **kwargs) -> {}:
    global yh_screen_dynamic_total
    session = kwargs['session']
    if roof is None:
        payload = gt_payload(floor)
    else:
        payload = btwn_payload(floor, roof)

    screen_data = get_screen(session, screen_type, offset, payload)
    if screen_data is None:
        return None
    screener_response = ScreenerResponse(screen_data)
    with yh_api_calls_lock:
        utils.progress['yh_api_calls'] += 1
        if utils.progress['yh_api_calls'] >= utils.settings['max_yh_calls']:
            raise MaxCallsExceededError('Max yh calls exceeded.')

    if updater:
        yh_screen_dynamic_total_lock.acquire()
        yh_screen_dynamic_total = screener_response.to_dict()['total']
        yh_screen_dynamic_total_update_event.set()
        yh_screen_dynamic_total_lock.release()

    return screener_response


def fetch_yh_fund(fund, **kwargs):
    fund = fund['fund']
    session = kwargs['session']
    data = get_yh_info(session, fund)
    if data is None:
        return None

    try:
        yh_finance_response = YHFinanceResponse(data)
        with yh_api_calls_lock:
            utils.progress['yh_api_calls'] += 1
            if utils.progress['yh_api_calls'] >= utils.settings['max_yh_calls']:
                raise MaxCallsExceededError('Max yh calls exceeded.')
        if 'err' in yh_finance_response.defaultKeyStatistics.data:
            return BadFund(symbol=fund)
        return yh_finance_response
    except KeyError:
        logger.debug(data)
        return BadFund(symbol=fund)


def fetch_perf_id(fund, **kwargs):
    fund = fund['fund']
    session = kwargs['session']
    data = get_perf_id(session, fund)
    if data is None:
        return None
    result = None
    for entry in data['results']:
        if entry['ticker'] == fund:
            result = entry
            break
    if result is None:
        return BadFund(symbol=fund)
    with ms_api_calls_lock:
        utils.progress['ms_api_calls'] += 1
        if utils.progress['ms_api_calls'] >= utils.settings['max_ms_calls']:
            raise MaxCallsExceededError('Max ms calls exceeded.')
    return PerformanceIdResponse(result)


def fetch_ms_fund(fund, **kwargs):
    fund = fund['perf_id']
    session = kwargs['session']
    data = get_ms_info(session, fund)
    if data is None:
        return None
    if data == -1:
        return BadFund(perf_id=fund)
    with ms_api_calls_lock:
        utils.progress['ms_api_calls'] += 1
        if utils.progress['ms_api_calls'] >= utils.settings['max_ms_calls']:
            raise MaxCallsExceededError('Max ms calls exceeded.')
    return MSFinanceResponse(data[0])


def fetch_ms_trailing_returns(fund, **kwargs):
    perf_id = fund['perf_id']
    share_id = fund['share_id']
    quote_type = fund['quote_type']
    fund = fund['fund']

    session = kwargs['session']

    share_type = False
    if quote_type == 'ETF':
        data = morningstar_scraper.get_etf_trailing_returns(session, share_id)
        share_type = True
    elif quote_type == 'MUTUALFUND':
        data = morningstar_scraper.get_fund_trailing_returns(session, share_id)
        share_type = True
    else:
        data = morningstar_scraper.get_stock_trailing_returns(session, perf_id)

    if data is None:
        return None

    data['fund'] = fund
    logger.debug(data)
    if share_type:
        return MSFundTrailingReturnsResponse(data)
    else:
        return MSStockTrailingReturnsResponse(data)


class MaxCallsExceededError(Exception):
    pass


@dataclass(order=True)
class PrioritizedItem:
    priority: float
    item: Any = field(compare=False)


def master_thread(queue: PriorityQueue, priority: float, data_source: DataTree, worker, **kwargs):
    has_whitelist = 'whitelist' in kwargs
    if has_whitelist:
        whitelist = kwargs['whitelist']
    else:
        whitelist = []

    try:
        already_queued = set()
        do = True
        while (data_source.parent is not None and data_source.parent.incomplete) or queue.qsize() != 0 or do:
            if kill_event.is_set():
                return
            data = data_source.data
            if len(data.keys()) == 0:
                sleep(5)
                continue
            do = False
            for i in range(len(data['funds'])):
                fund = list(data['funds'])[i]
                if 'performance_ids' in data.keys():
                    perf_id = data['performance_ids'][i]
                else:
                    perf_id = None
                if 'share_class_ids' in data.keys():
                    share_id = data['share_class_ids'][i]
                    quote_type = data['quote_types'][i]
                else:
                    share_id = None
                    quote_type = None
                if fund in already_queued or (has_whitelist and fund not in whitelist):
                    continue
                do = True
                already_queued.add(fund)
                queue.put(PrioritizedItem(priority, (worker, {'fund': fund,
                                                              'perf_id': perf_id,
                                                              'share_id': share_id,
                                                              'quote_type': quote_type})))
        if len(data_source.children) == 0:
            for _ in range(0, MAX_WORKERS):
                queue.put(PrioritizedItem(DEATH_PRIORITY, (None, None)))
            queue.join()
        data_source.incomplete = False
        logger.debug(f'Thread Complete.')
    except Exception as unchecked_exception:
        unchecked_exceptions.append(unchecked_exception)
        raise unchecked_exception


def manage_db(data_trees):
    try:
        db = database.DB()
        db.create_tables()
        while True:
            write_queue_value = None
            try:
                for tree in data_trees:
                    if tree.data_source == 'yh':
                        tree.data = db.valid_for_yh_finance_view()
                    elif tree.data_source == 'perf':
                        tree.data = db.valid_for_perf_id_view()
                    elif tree.data_source == 'ms':
                        tree.data = db.valid_for_ms_finance_view()
                    elif tree.data_source == 'missing_perf':
                        tree.data = db.missing_perf_id_view()
                    elif tree.data_source == 'missing_share':
                        tree.data = db.missing_share_class_id_view()
                    elif tree.data_source == 'having_share':
                        tree.data = db.having_share_class_id_view()
                while not db_write_queue.empty():
                    write_queue_value = db_write_queue.get()
                    if write_queue_value is None:
                        db_write_queue.task_done()
                        db.delete_unscreened()
                        db.close_connections()
                        logger.debug('DB Thread is complete.')
                        return
                    if isinstance(write_queue_value, ScreenerResponse):
                        db.add_from_screener(write_queue_value.to_dict()['quotes'])
                    elif isinstance(write_queue_value, YHFinanceResponse):
                        db.update_from_yh_finance(write_queue_value.to_dict())
                    elif isinstance(write_queue_value, PerformanceIdResponse):
                        db.update_performance_id(write_queue_value.to_dict())
                    elif isinstance(write_queue_value, MSFinanceResponse):
                        db.update_from_ms_finance(write_queue_value.to_dict())
                    elif isinstance(write_queue_value, MSFundTrailingReturnsResponse) or \
                            isinstance(write_queue_value, MSStockTrailingReturnsResponse):
                        db.update_from_ms_trailing_returns(write_queue_value.to_dict())
                    elif isinstance(write_queue_value, AddFund):
                        db.add_fund(write_queue_value.symbol)
                    elif isinstance(write_queue_value, BadFund):
                        logger.debug(f'Bad Fund: {write_queue_value.symbol}/{write_queue_value.perf_id}')
                        db.delete_fund(write_queue_value.symbol, write_queue_value.perf_id)
                    db_write_queue.task_done()
            except Exception as e:
                logger.exception(f'Value: {write_queue_value}, Exception: {e}')
    except Exception as unchecked_exception:
        unchecked_exceptions.append(unchecked_exception)
        raise unchecked_exception


def debug_aid(*args):
    print_str = 'Threads Status: '
    alive_thread = False
    for arg in args:
        alive_thread = alive_thread or arg.is_alive()
        print_str += f'|{arg.name}, {arg.is_alive()}|'
    logger.debug(print_str)
    print_str = f'Queue Status: |db_queue, {db_write_queue.qsize()}||yh_queue, {yh_queue.qsize()}||ms_queue, {ms_queue.qsize()}'
    logger.debug(print_str)
    if not program_ended or alive_thread:
        threading.Timer(STATUS_LATENCY, debug_aid, args).start()


def ticker_tracker() -> bool:
    global program_ended, unchecked_exceptions
    success = True
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
                             args=(f'yh_worker_{i}', yh_queue, db_write_queue, yh_access_control, requests.Session())),
            threading.Thread(target=worker_thread,
                             name=f'ms_worker_{i}',
                             args=(f'ms_worker_{i}', ms_queue, db_write_queue, ms_access_control, requests.Session()))
        ]

    update_api_calls([yh_access_control, ms_access_control])
    debug_aid(db_thread, *threads)
    db_thread.start()
    for thread in threads:
        thread.start()
    for thread in threads:
        while thread.is_alive():
            thread.join(5)
            if len(unchecked_exceptions) > 0:
                kill_event.set()
                program_ended = True
                utils.dump_progress()
                for exception in unchecked_exceptions:
                    logger.exception(repr(exception))
                mail.debug_email(unchecked_exceptions)
                unchecked_exceptions = []
                success = False
    db_write_queue.put(None)
    db_thread.join()
    program_ended = True
    if success:
        db = database.DB()
        csv_data = db.csv_data()
        with open('tickers.csv', 'w', newline='') as output_csv:
            writer = csv.writer(output_csv)
            writer.writerow(utils.settings['headers'])
            for row in csv_data:
                writer.writerow(row)
    return success


def fund_finder(input_file, output_file) -> bool:
    global program_ended, unchecked_exceptions
    input_file_name = input_file.name
    input_file.close()
    with open(input_file_name) as input_file:
        fund_list = csv.reader(input_file)
        funds = set()
        for fund in fund_list:
            db_write_queue.put(AddFund(fund[0]))
            funds.add(fund[0])
    success = True
    missing_perf_data_tree = DataTree(data_source='missing_perf')
    missing_share_data_tree = DataTree(parent=missing_perf_data_tree,
                                       data_source='missing_share')
    having_share_data_tree = DataTree(parent=missing_share_data_tree,
                                      data_source='having_share')

    ms_access_control = ApiAccessController()

    db_thread = threading.Thread(target=manage_db, name='db_master',
                                 args=([missing_perf_data_tree, having_share_data_tree, missing_share_data_tree],))

    threads = [
        threading.Thread(target=master_thread, name='missing_perf_data_master',
                         args=(ms_queue, PERF_ID_PRIORITY, missing_perf_data_tree, fetch_perf_id),
                         kwargs={'whitelist': funds}),
        threading.Thread(target=master_thread, name='missing_share_id_master',
                         args=(ms_queue, MS_PRIORITY, missing_share_data_tree, fetch_ms_fund),
                         kwargs={'whitelist': funds}),
        threading.Thread(target=master_thread, name='having_share_id_master',
                         args=(ms_queue, MS_PRIORITY, having_share_data_tree, fetch_ms_trailing_returns),
                         kwargs={'whitelist': funds})
    ]
    for i in range(0, MAX_WORKERS):
        threads += [
            threading.Thread(target=worker_thread,
                             name=f'ms_worker_{i}',
                             args=(f'ms_worker_{i}', ms_queue, db_write_queue, ms_access_control, requests.Session()))
        ]

    update_api_calls([ms_access_control])
    debug_aid(db_thread, *threads)
    db_thread.start()
    for thread in threads:
        thread.start()
    for thread in threads:
        while thread.is_alive():
            thread.join(5)
            if len(unchecked_exceptions) > 0:
                kill_event.set()
                program_ended = True
                for exception in unchecked_exceptions:
                    logger.exception(repr(exception), exc_info=exception)
                mail.debug_email(unchecked_exceptions)
                unchecked_exceptions = []
                success = False
    db_write_queue.put(None)
    db_thread.join()
    program_ended = True
    if success:
        db = database.DB()
        lists = [['symbol'],
                 ['ytd'],
                 ['oneYear'],
                 ['threeYear'],
                 ['fiveYear'],
                 ['tenYear'],
                 ['fifteenYear'],
                 ['inception'],
                 ['starRating']]
        for fund in funds:
            fund_data = db.fund_data_view(fund)
            if fund_data is None:
                fund_data = [fund, None, None, None, None, None, None, None, None]
            print(fund_data)
            for i in range(len(lists)):
                lists[i].append(fund_data[i])
        output_file_name = output_file.name
        output_file.close()
        with open(output_file_name, 'w', newline='') as output_file:
            writer = csv.writer(output_file)
            for item in lists:
                writer.writerow(item)
    return success


def handle_args():
    parser = argparse.ArgumentParser(
        prog='TickerTracker',
        description='Obtains useful information from Yahoo Finance and Morningstar',
    )
    subparsers = parser.add_subparsers(required=True, help='select a mode')
    screen_parser = subparsers.add_parser('screen', help='"screen" help')
    screen_parser.set_defaults(action=lambda: 'screen')
    fetch_parser = subparsers.add_parser('fetch', help='"fetch" help')
    fetch_parser.add_argument('-i', '--input',
                              required=True,
                              type=argparse.FileType('r'),
                              help='select the input file')
    fetch_parser.add_argument('-o', '--output',
                              type=argparse.FileType('w'),
                              default='./output.csv',
                              help='output file name/directory')
    fetch_parser.set_defaults(action=lambda: 'fetch')

    args = parser.parse_args()
    if 'screen' == args.action():
        mail.start_email()
        logger.debug('Starting New Screen Run------------------------------------------')
        if ticker_tracker():
            mail.complete_email()
    elif 'fetch' == args.action():
        logger.debug('Starting New Fetch Run------------------------------------------')
        fund_finder(args.input, args.output)


if __name__ == '__main__':
    handle_args()