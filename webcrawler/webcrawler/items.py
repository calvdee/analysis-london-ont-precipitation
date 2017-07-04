import scrapy


class HourlyWeatherDataItem(scrapy.Item):
    date = scrapy.Field()
    time = scrapy.Field()
    temp = scrapy.Field()
    dew_point_temp = scrapy.Field()
    rel_humidity = scrapy.Field()
    wind_dir = scrapy.Field()
    wind_speed = scrapy.Field()
    visibility = scrapy.Field()
    stn_press = scrapy.Field()
    humidex = scrapy.Field()
    wind_chill = scrapy.Field()
    weather = scrapy.Field()

class DailyWeatherDataItem(scrapy.Item):
    station = scrapy.Field()
    date = scrapy.Field()
    maxTemp = scrapy.Field()
    minTemp = scrapy.Field()
    meanTemp = scrapy.Field()
    heatDegDays = scrapy.Field()
    coolDegDays = scrapy.Field()
    totalRainMM = scrapy.Field()
    totalSnowCM = scrapy.Field()
    totalPrecipMM = scrapy.Field()
    snowOnGroundCM = scrapy.Field()
    dirOfMaxGust10sDEG = scrapy.Field()
    spdOfMaxGustKMH = scrapy.Field()