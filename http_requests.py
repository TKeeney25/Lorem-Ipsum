import json

import requests

import utils
from utils import logger

TIMEOUT = 60

YH_HEADERS = {
    'content-type': "application/json",
    'X-RapidAPI-Host': "yh-finance.p.rapidapi.com",
    'X-RapidAPI-Key': utils.settings['api_key']
}

MS_HEADERS = {
    'X-RapidAPI-Host': "ms-finance.p.rapidapi.com",
    'X-RapidAPI-Key': utils.settings['api_key']
}


# region Payloads
def default_payload(operator: str, operands: []) -> json:
    return [
        {
            "operator": "AND",
            "operands": [
                {
                    "operator": operator,
                    "operands": operands
                },
                {
                    "operator": "eq",
                    "operands": [
                        "region",
                        "us"
                    ]
                }
            ]
        }
    ]


def gt_payload(gt: float) -> json:
    return default_payload('gt', ["intradayprice", gt])


def btwn_payload(floor: float, roof: float) -> json:
    if roof <= floor:
        raise ValueError('Roof value must be greater than floor value.')
    return default_payload('btwn', ['intradayprice', floor, roof + .0001])


# endregion

def get_screen(session: requests.Session, quote_type: str, offset: int, payload: json):
    url = "https://yh-finance.p.rapidapi.com/screeners/list"
    querystring = {"quoteType": quote_type, "sortField": "intradayprice", "region": "US", "size": "50",
                   "offset": offset,
                   "sortType": "ASC"}
    return validate_response(session.post(url, json=payload, headers=YH_HEADERS, params=querystring, timeout=TIMEOUT))


def get_yh_info(session: requests.Session, symbol: str):
    url = "https://yh-finance.p.rapidapi.com/stock/v2/get-summary"
    querystring = {"symbol": symbol, "region": "US"}
    return validate_response(session.get(url, headers=YH_HEADERS, params=querystring, timeout=TIMEOUT))


def get_perf_id(session: requests.Session, symbol: str):
    url = "https://ms-finance.p.rapidapi.com/market/v2/auto-complete"
    querystring = {"q": symbol}
    return validate_response(session.get(url, headers=MS_HEADERS, params=querystring, timeout=TIMEOUT))


def get_ms_info(session: requests.Session, performance_id: str):
    url = "https://ms-finance.p.rapidapi.com/stock/get-detail"
    querystring = {"PerformanceId": performance_id}
    return validate_response(session.get(url, headers=MS_HEADERS, params=querystring, timeout=TIMEOUT))


def validate_response(response):
    try:
        response.raise_for_status()
        json_response = response.json()
        if json_response:
            return json_response
        else:
            return -1
    except Exception as e:
        logger.exception(f'{response.headers}, {e}')
        return None


if __name__ == '__main__':
    test_session = requests.Session()
    # print(get_perf_id(('UNL',)))
