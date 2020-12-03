import re
import scrapy
from ..loaders import AutoYoulaLoader


class AutoyoulaSpider(scrapy.Spider):
    name = 'autoyoula'
    allowed_domains = ['auto.youla.ru']
    start_urls = ['https://auto.youla.ru/']
    css_query = {
        'brands': '.TransportMainFilters_brandsList__2tIkv '
                  '.ColumnItemList_container__5gTrc .ColumnItemList_column__5gjdt a.blackLink'
    }

    xpath_query = {
        'brands': '//div[@class="TransportMainFilters_brandsList__2tIkv"]//a[@class="blackLink"]/@href',
        'pagination': '//div[contains(@class, "Paginator_block")]//a/@href',
        'ads': '//article[contains(@class, "SerpSnippet_snippet")]//a[contains(@class, "SerpSnippet_name")]/@href',
    }

    itm_template = {
        'title': '//div[@data-target="advert-title"]/text()',
        'images': '//figure[contains(@class, "PhotoGallery_photo")]//img/@src',
        'description': '//div[contains(@class, "AdvertCard_descriptionInner")]//text()',
        'autor': '//script[contains(text(), "window.transitState =")]/text()',
        'specifications':
            '//div[contains(@class, "AdvertCard_specs")]/div/div[contains(@class, "AdvertSpecs_row")]',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def parse(self, response):
        for link in response.xpath(self.xpath_query['brands']):
            yield response.follow(link, callback=self.brand_page_parse)

    def brand_page_parse(self, response):
        for page in response.xpath(self.xpath_query['pagination']):
            yield response.follow(page, callback=self.brand_page_parse)

        for item_link in response.xpath(self.xpath_query['ads']):
            yield response.follow(item_link, callback=self.ads_parse)

    def ads_parse(self, response):
        loader = AutoYoulaLoader(response=response)
        loader.add_value('url', response.url)
        for name, selector in self.itm_template.items():
            loader.add_xpath(name, selector)
        yield loader.load_item()

    def get_specifications(self, response):
        return {itm.css('.AdvertSpecs_label__2JHnS::text').get(): itm.css(
            '.AdvertSpecs_data__xK2Qx::text').get() or itm.css('a::text').get() for itm in
                response.css('.AdvertSpecs_row__ljPcX')}
