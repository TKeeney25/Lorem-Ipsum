test = '''
# for testing
floor = payload[0]['operands'][0]['operands'][1]
if floor == -1:
    floor = 0
roof = 500
try:
    roof = payload[0]['operands'][0]['operands'][2]
except IndexError:
    pass
density = 100
size = 50000
with open('./tests/defaults/screen_data.json') as screen_data:
    json_response = json.load(screen_data)
    json_response['finance']['result'][0]['total'] = int((roof-floor)*density)
    if offset + 50 >= json_response['finance']['result'][0]['total']:
        json_response['finance']['result'][0]['count'] = json_response['finance']['result'][0]['total'] - offset
    return json_response
# end testing'''
