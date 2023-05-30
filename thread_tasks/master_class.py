from queue import PriorityQueue, Queue
from threading import Event

import requests

from main import DataTree, ApiAccessController


def master_thread(queue: PriorityQueue, priority: float, data_source: DataTree, worker, **kwargs):
    has_whitelist = 'whitelist' in kwargs
    if has_whitelist:
        whitelist = kwargs['whitelist']
    else:
        whitelist = []

    try:
        already_queued = set()
        do = True
        while (data_source.parent is not None and data_source.parent.incomplete) or do:
            do = False
            if kill_event.is_set():
                return
            data_source.event.wait()
            data = data_source.data
            data_source.event.clear()
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


class Task:
    def __init__(self):
        self.work_queue: PriorityQueue
        self.kill_event: Event


class ControllerTask(Task):
    def __init__(self):
        super().__init__()
        self.priority: float
        self.data_source: DataTree
        self.worker: WorkerTask


class WorkerTask(Task):
    def __init__(self):
        super().__init__()
        self.name: str
        self.db_queue: Queue
        self.api_access_controller: ApiAccessController
        self.session: requests.Session

    def process(self):


