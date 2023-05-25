from typing import Optional


class Response:
    def __init__(self, api_id, data):
        self.api_id = api_id
        try:
            self.data = data[api_id]
        except KeyError:
            self.data = data

    def __str__(self):
        return str(self.data)

    def get_vars(self):
        var = vars(self).copy()
        var.pop('api_id')
        return var

    def to_dict(self):
        return_dict = {}
        all_vars = self.get_vars()
        for var in all_vars:
            if isinstance(all_vars[var], Response):
                if isinstance(all_vars[var].to_dict(), dict):
                    return_dict.update(all_vars[var].to_dict())
                else:
                    return_dict[var] = all_vars[var].to_dict()
            else:
                if len(all_vars) == 1:
                    return all_vars[var]
        return return_dict


class DataResponse(Response):
    def __init__(self, api_id, data):
        super().__init__(api_id, data)
        self.api_id = api_id
        try:
            self.data = data[api_id]
        except KeyError:
            self.data = None
        except TypeError:
            self.data = data


class IntegerResponse(DataResponse):
    def __init__(self, api_id, data):
        super().__init__(api_id, data)
        self.get_integer()

    def get_integer(self) -> Optional[int]:
        if self.data is None:
            return None
        if isinstance(self.data, dict) and 'raw' in self.data:
            self.data = self.data['raw']
        try:
            self.data = int(self.data)
        except TypeError:
            self.data = None


class RealResponse(DataResponse):
    def __init__(self, api_id, data):
        super().__init__(api_id, data)
        self.get_real_number()

    def get_real_number(self) -> Optional[float]:
        if self.data is None:
            return None
        if isinstance(self.data, dict) and 'raw' in self.data:
            self.data = self.data['raw']
        try:
            self.data = float(self.data)
        except TypeError:
            self.data = None


class TextResponse(DataResponse):
    def __init__(self, api_id, data):
        super().__init__(api_id, data)
        self.get_text()

    def get_text(self):
        if self.data is None:
            return None
        self.data = str(self.data)


class ListResponse(DataResponse):
    def __init__(self, api_id, data, data_function=None):
        super().__init__(api_id, data)
        self.data_function = data_function
        self.get_list()

    def get_list(self):
        if self.data is None or self.data_function is None:
            return None
        new_data = []
        for item in self.data:
            new_data.append(self.data_function(item))
        self.data = new_data

    def to_dict(self):
        data_set = []
        for entry in self.data:
            if isinstance(entry, Response):
                data_set.append(entry.to_dict())
            else:
                data_set.append(entry)
        return data_set


class MSTickerResponse(TextResponse):
    def __init__(self, api_id, data):
        super().__init__(api_id, data)

    def get_text(self):
        if self.data is None:
            return None
        self.data = str(self.data).split(':')[-1]


class ScreenerResponse(Response):
    def __init__(self, data):
        super().__init__('main', data)
        data = data['finance']['result'][0]
        self.start = IntegerResponse('start', data)
        self.count = IntegerResponse('count', data)
        self.total = IntegerResponse('total', data)
        self.quotes = ListResponse('quotes', data, Quote)


class Quote(Response):
    def __init__(self, data):
        super().__init__('Quotes', data)
        self.symbol = TextResponse('symbol', data)
        self.longName = TextResponse('longName', data)
        self.quoteType = TextResponse('quoteType', data)
        self.firstTradeDateMilliseconds = IntegerResponse('firstTradeDateMilliseconds', data)
        self.exchange = TextResponse('exchange', data)
        self.market = TextResponse('market', data)
        self.marketCap = IntegerResponse('marketCap', data)
        self.marketState = TextResponse('marketState', data)
        self.priceHint = IntegerResponse('priceHint', data)
        self.priceToBook = RealResponse('priceToBook', data)
        self.regularMarketChange = RealResponse('regularMarketChange', data)
        self.regularMarketChangePercent = RealResponse('regularMarketChangePercent', data)
        self.regularMarketPreviousClose = RealResponse('regularMarketPreviousClose', data)
        self.regularMarketPrice = RealResponse('regularMarketPrice', data)
        self.sharesOutstanding = IntegerResponse('sharesOutstanding', data)
        self.tradeable = TextResponse('tradeable', data)
        self.triggerable = TextResponse('triggerable', data)


class PerformanceIdResponse(Response):
    def __init__(self, data):
        super().__init__('main', data)
        self.id = TextResponse('id', data)
        self.name = TextResponse('name', data)
        self.description = TextResponse('description', data)
        self.exchange = TextResponse('exchange', data)
        self.performanceId = TextResponse('performanceId', data)
        self.securityType = TextResponse('securityType', data)
        self.symbol = TextResponse('ticker', data)
        self.type = TextResponse('type', data)
        self.url = TextResponse('url', data)


# region MSFinance Response
class MSFinanceResponse(Response):
    def __init__(self, data):
        super().__init__('main', data)
        self.detail = Detail(data)
        self.symbol = MSTickerResponse('RegionAndTicker', data)


class Detail(Response):
    def __init__(self, data):
        super().__init__('Detail', data)
        self.starRating = IntegerResponse('StarRating', self.data)


# endregion

# region MS Trailing Returns.
class MSFundTrailingReturnsResponse(Response):
    def __init__(self, data):
        super().__init__('main', data)
        self.fund = TextResponse('fund', data)
        self.columnDefs = ListResponse('columnDefs', data)
        self.totalReturnNAV = ListResponse('totalReturnNAV', data)
        temp_list = self.totalReturnNAV.to_dict()
        self.oneDay = RealResponse('oneDay', temp_list[0])
        self.oneWeek = RealResponse('oneWeek', temp_list[1])
        self.oneMonth = RealResponse('oneMonth', temp_list[2])
        self.threeMonth = RealResponse('threeMonth', temp_list[3])
        self.ytd = RealResponse('ytd', temp_list[4])
        self.oneYear = RealResponse('oneYear', temp_list[5])
        self.threeYear = RealResponse('threeYear', temp_list[6])
        self.fiveYear = RealResponse('fiveYear', temp_list[7])
        self.tenYear = RealResponse('tenYear', temp_list[8])
        self.fifteenYear = RealResponse('fifteenYear', temp_list[9])
        self.inception = RealResponse('inception', temp_list[10])
        self.returnDate = TextResponse('fundReturnDate', data)
        self.ratingDate = TextResponse('ratingDate', data)
        self.starRating = IntegerResponse('overallMorningstarRating', data)


class MSStockTrailingReturnsResponse(Response):
    def __init__(self, data):
        super().__init__('main', data)
        self.returnDate = TextResponse('returnDate', data)
        self.fund = TextResponse('fund', data)
        if 'trailingTotalReturnsList' in data and data['trailingTotalReturnsList']:
            data = data['trailingTotalReturnsList'][0]
        self.symbol = TextResponse('name', data)
        self.oneDay = RealResponse('trailing1DayReturn', data)
        self.oneWeek = RealResponse('trailing1WeekReturn', data)
        self.oneMonth = RealResponse('trailing1MonthReturn', data)
        self.threeMonth = RealResponse('trailing3MonthReturn', data)
        self.sixMonth = RealResponse('trailing6MonthReturn', data)
        self.ytd = RealResponse('trailingYearToDateReturn', data)
        self.oneYear = RealResponse('trailing1YearReturn', data)
        self.threeYear = RealResponse('trailing3YearReturn', data)
        self.fiveYear = RealResponse('trailing5YearReturn', data)
        self.tenYear = RealResponse('trailing10YearReturn', data)
        self.fifteenYear = RealResponse('trailing15YearReturn', data)
        self.inception = RealResponse('DNE', data)
        self.starRating = IntegerResponse('DNE', data)



# endregion

# region YHFinance Response
class YHFinanceResponse(Response):
    def __init__(self, data):
        super().__init__('main', data)
        self.symbol = TextResponse('symbol', data)
        self.defaultKeyStatistics = DefaultKeyStatistics(data)
        self.fundPerformance = FundPerformance(data)
        self.fundProfile = FundProfile(data)


class DefaultKeyStatistics(Response):
    def __init__(self, data):
        super().__init__('defaultKeyStatistics', data)
        self.beta3Year = RealResponse('beta3Year', self.data)
        self.totalAssets = IntegerResponse('totalAssets', self.data)
        self.fundFamily = TextResponse('fundFamily', self.data)
        self.percent_yield = RealResponse('yield', self.data)
        self.category = TextResponse('category', self.data)


class FundPerformance(Response):
    def __init__(self, data):
        super().__init__('fundPerformance', data)
        self.trailingReturns = TrailingReturns(self.data)
        self.annualTotalReturns = AnnualTotalReturns(self.data)


class TrailingReturns(Response):
    def __init__(self, data):
        super().__init__('trailingReturns', data)
        self.ytd = RealResponse('ytd', self.data)
        self.lastBearMkt = RealResponse('lastBearMkt', self.data)
        self.lastBullMkt = RealResponse('lastBullMkt', self.data)
        self.oneMonth = RealResponse('oneMonth', self.data)
        self.threeMonth = RealResponse('threeMonth', self.data)
        self.oneYear = RealResponse('oneYear', self.data)
        self.threeYear = RealResponse('threeYear', self.data)
        self.fiveYear = RealResponse('fiveYear', self.data)
        self.tenYear = RealResponse('tenYear', self.data)


class FundProfile(Response):
    def __init__(self, data):
        super().__init__('fundProfile', data)
        self.feesExpensesInvestment = FeesExpensesInvestment(self.data)
        self.brokerages = ListResponse('brokerages', self.data)


class FeesExpensesInvestment(Response):
    def __init__(self, data):
        super().__init__('feesExpensesInvestment', data)
        self.twelveBOne = RealResponse('twelveBOne', self.data)


class AnnualTotalReturns(Response):
    def __init__(self, data):
        super().__init__('annualTotalReturns', data)
        self.returns = ListResponse('returns', self.data, AnnualReturn)


class AnnualReturn(Response):
    def __init__(self, data):
        super().__init__('Returns', data)
        self.annualValue = RealResponse('annualValue', self.data)
        self.year = IntegerResponse('year', self.data)


# endregion

if __name__ == '__main__':
    # (ScreenerResponse(json.load(open('./tests/defaults/screen_data.json'))).to_dict())
    # print(MSFinanceResponse(json.load(open('./tests/defaults/ms_get_detail.json'))[0]).to_dict())
    # print(PerformanceIdResponse(json.load(open('./tests/defaults/perf_id_data.json'))['results'][0]).to_dict())
    # hlep = MSTrailingReturnsResponse(morningstar_scraper.get_trailing_returns('FOUSA069TK').json())
    # print(hlep.to_dict())
    pass
