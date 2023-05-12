import json
import utils

MAX_TOTAL: int = 5000
DEFAULT_JUMP: int = 250
DEFAULT_FLOOR: int = -1
MAX_RESULTS: int = 50
DEFAULT_DELAYS = [1, 5, 10, 60, 300, 600]

total_mutual_funds = -1
total_etfs = -1

YH_HEADERS = {
    'content-type': "application/json",
    'X-RapidAPI-Host': "yh-finance.p.rapidapi.com",
    'X-RapidAPI-Key': utils.settings['api_key']
}

MS_HEADERS = {
    'X-RapidAPI-Host': "ms-finance.p.rapidapi.com",
    'X-RapidAPI-Key': utils.settings['api_key']
}

#region Payloads
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
#endregion
