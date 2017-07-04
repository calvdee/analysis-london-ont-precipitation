import scrapy
import pandas as pd 
import calendar
import itertools
from datetime import datetime
from webcrawler.items import HourlyWeatherDataItem, DailyWeatherDataItem

STATIONS = {
    '10999': 'London CS',
    '4789': 'London Airport',
    '50093': 'London A'
}

BASE_URL = "http://climate.weather.gc.ca/climate_data/daily_data_e.html?StationID={}"

URL_PARAMS = """
&Prov=ON&
urlExtension=_e.html&
searchType=stnName&
Month={:02}&
txtStationName=London&
Year={}""".replace("\n", "")

class DailyWeatherSpider(scrapy.Spider):
    name = "daily_weather_spider"
    allowed_domains = ["climate.weather.gc.ca"]

    def start_requests(self):
        today = pd.datetime(datetime.today().year,  datetime.today().month,  datetime.today().day)
        dates = pd.Series(pd.date_range('2010-01-01', today + pd.offsets.MonthEnd(), freq='M'))
        date_tups = dates.apply(lambda x: (x.year, x.month)).values
        station_tups = list(itertools.product(list(STATIONS.keys()), date_tups))
        # station_tups = [('4789', (2012, 1))] # DEBUG
        

        # Generate a request for each year/month/station combination
        urls = [[(STATIONS[su[0]], su[1][0], su[1][1]), BASE_URL.format(su[0]) + URL_PARAMS.format(su[1][1], su[1][0])] 
                for su in station_tups]
    
        for url in urls:
            # Generate the request and add date and station values so 
            # we can extract them in parse()
            station, year, month = url[0][0], url[0][1], url[0][2]
            self.logger.info("Retrieving data for station={}, year={}, month={}".format(
                station, year, month))
            request = scrapy.Request(url[1], self.parse)
            request.meta['station'] = station
            request.meta['year'] = year
            request.meta['month'] = month
            yield request

    def parse(self, response):
        table_rows = response.xpath("//div[@id='dynamicDataTable']/table").xpath("//tr")
        data_rows  = table_rows[2:-4]
        station = response.meta['station']
        year = response.meta['year']
        month = response.meta['month']
        
        # Make sure the page has data for the month we requested
        # (if a month is missing data, the report will display data from
        # another month)
        report_month = response.xpath('//h1[@id="wb-cont"]/text()').extract()[0].split()[-2:-1][0]
        request_month = calendar.month_name[month]

        if report_month != request_month:
            # No match, skip the month
            self.logger.error("NO DATA found for {} - {}/{:02}".format(station, year, month))
            return

        self.logger.info("FOUND {} weather observations for {} - {}/{:02}".format(
            len(data_rows), station, year, month))

        for ix, row in enumerate(data_rows):
            item = DailyWeatherDataItem()
            row_data = row.xpath('td')

            if len(row_data) < 2:
                self.logger.error("NO FIELDS for {} - {}/{:02}".format(
                    station, year, month))
                yield item
            else:
                day = ix + 1
                date = "{}-{:02}-{:02}".format(year, month, day)

                self.logger.info("FOUND {} fields for {} - {}".format(len(row_data), station, date))

                item['station'] = station
                item['date'] = date
                item['maxTemp']             = row_data[1].xpath('text()').extract()[0] if len(row_data[1].xpath('text()')) > 0 else ""
                item['minTemp']             = row_data[2].xpath('text()').extract()[0] if len(row_data[2].xpath('text()')) > 0 else ""
                item['meanTemp']            = row_data[3].xpath('text()').extract()[0] if len(row_data[3].xpath('text()')) > 0 else "" 
                item['heatDegDays']         = row_data[4].xpath('text()').extract()[0] if len(row_data[4].xpath('text()')) > 0 else "" 
                item['coolDegDays']         = row_data[5].xpath('text()').extract()[0] if len(row_data[5].xpath('text()')) > 0 else "" 
                item['totalRainMM']         = row_data[6].xpath('text()').extract()[0] if len(row_data[6].xpath('text()')) > 0 else "" 
                item['totalSnowCM']         = row_data[7].xpath('text()').extract()[0] if len(row_data[7].xpath('text()')) > 0 else "" 
                item['totalPrecipMM']       = row_data[8].xpath('text()').extract()[0] if len(row_data[8].xpath('text()')) > 0 else "" 
                item['snowOnGroundCM']      = row_data[9].xpath('text()').extract()[0] if len(row_data[9].xpath('text()')) > 0 else "" 
                item['dirOfMaxGust10sDEG']  = row_data[10].xpath('text()').extract()[0] if len(row_data[10].xpath('text()')) > 0 else "" 
                item['spdOfMaxGustKMH']     = row_data[11].xpath('text()').extract()[0] if len(row_data[11].xpath('text()')) > 0 else "" 
                
                self.logger.info("PARSED ITEM")

                yield item
