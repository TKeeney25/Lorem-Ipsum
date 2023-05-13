import json
from requests import request

import utils
from utils import logger

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

def get_screen(quote_type: str, offset: int, payload: json):
    url = "https://yh-finance.p.rapidapi.com/screeners/list"
    querystring = {"quoteType": quote_type, "sortField": "intradayprice", "region": "US", "size": "50",
                   "offset": offset,
                   "sortType": "ASC"}
    return validate_response(request("POST", url, json=payload, headers=YH_HEADERS, params=querystring))


def get_yh_info(symbol: str) -> dict:
    url = "https://yh-finance.p.rapidapi.com/stock/v2/get-summary"
    querystring = {"symbol": symbol, "region": "US"}
    return validate_response(request("GET", url, headers=YH_HEADERS, params=querystring))

def get_perf_id(symbol: str) -> dict:
    url = "https://ms-finance.p.rapidapi.com/market/v2/auto-complete"
    querystring = {"q": symbol}
    return validate_response(request("GET", url, headers=MS_HEADERS, params=querystring))[0]

def get_ms_info(performance_id: str) -> dict:
    url = "https://ms-finance.p.rapidapi.com/stock/get-detail"
    querystring = {"PerformanceId": performance_id}
    return validate_response(request("GET", url, headers=MS_HEADERS, params=querystring))[0]


def validate_response(response):
    try:
        if not response.ok:
            raise Exception(response.text)
        json_response = json.loads(response.text)
        if json_response:
            return json_response
    except Exception as e:
        logger.exception(f'{response.headers}, {e}')
        return None
