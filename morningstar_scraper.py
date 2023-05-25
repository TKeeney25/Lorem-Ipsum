import re

import requests

HEADERS = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
    'credentials': 'omit',
    'origin': 'https://www.morningstar.co.uk',
    'referer': 'https://www.morningstar.co.uk/',
    'sec-ch-ua': '"Chromium";v="88", "Google Chrome";v="88", ";Not A Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36',
    'x-api-requestid': '6c20fb56-8055-aa34-1632-5f5a8dbaddde',
    'x-sal-contenttype': 'e7FDDltrTy+tA2HnLovvGL0LFMwT+KkEptGju5wXVTU='
}


def update_bearer():
    global session_bearer
    url = 'https://www.morningstar.dk/Common/funds/snapshot/PortfolioSAL.aspx'
    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36'}
    payload = {
        'Site': 'fr',
        'FC': 'F000010S65',
        'IT': 'FO',
        'LANG': 'fr-FR'}
    response = requests.get(url, headers=headers, params=payload)
    search = re.search('(tokenMaaS:[\w\s]*\")(.*)(\")', response.text, re.IGNORECASE)
    session_bearer = 'Bearer ' + search.group(2)

def get_trailing_returns(performance_id):
    url = f'https://www.us-api.morningstar.com/sal/sal-service/fund/trailingReturn/v2/{performance_id}/data'

    payload = {
        'duration': 'daily',
        'currency': None,
        'limitAge': None,
        'languageId': 'en',
        'locale': 'en',
        'clientId': 'MDC',
        'benchmarkId': 'mstarorcat',
        'component': 'sal-mip-trailing-return',
        'version': '4.14.0',
    }

    response = requests.get(url, headers=HEADERS | {'authorization': session_bearer}, params=payload)
    return response


session_bearer = None
update_bearer()
if __name__ == '__main__':
    response = get_trailing_returns('FOUSA069TK')
    print(response)
    print(response.text)
    url = 'https://api-global.morningstar.com/sal-service/v1/fund/trailingReturn/v2/FOUSA069TK/data'

