import re
import scrapy

import pymongo


class AutoyoulaSpider(scrapy.Spider):
    name = 'autoyoula'
    allowed_domains = ['auto.youla.ru']
    start_urls = ['https://auto.youla.ru/']
    css_query = {
        'brands': '.TransportMainFilters_brandsList__2tIkv '
                  '.ColumnItemList_container__5gTrc .ColumnItemList_column__5gjdt a.blackLink'
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = pymongo.MongoClient()['parse_11'][self.name]

    def parse(self, response):
        for link in response.css(self.css_query['brands']):
            yield response.follow(link.attrib['href'], callback=self.brand_page_parse)

    def brand_page_parse(self, response):
        for page in response.css('.Paginator_block__2XAPy a.Paginator_button__u1e7D'):
            yield response.follow(page.attrib['href'], callback=self.brand_page_parse)

        for item_link in response.css('article.SerpSnippet_snippet__3O1t2 a.SerpSnippet_name__3F7Yu'):
            yield response.follow(item_link.attrib['href'], callback=self.ads_parse)

    def ads_parse(self, response):
        title = response.css('.AdvertCard_advertTitle__1S1Ak::text').get()
        images = [image.attrib['src'] for image in response.css('figure.PhotoGallery_photo__36e_r img')]
        description = response.css('.AdvertCard_descriptionInner__KnuRi::text').get()
        autor = self.js_decoder_autor(response)
        specifications = self.get_specifications(response)
        self.db.insert_one({
            'title': title,
            'images': images,
            'description': description,
            'url': response.url,
            'autor': autor,
            'specifications': specifications,
        })

    def js_decoder_autor(self, response):
        script = response.xpath('//script[contains(text(), "window.transitState =")]/text()').get()
        re_str = re.compile(r"youlaId%22%2C%22([0-9|a-zA-Z]+)%22%2C%22avatar")
        result = re.findall(re_str, script)
        return f'https://youla.ru/user/{result[0]}' if result else None

    def get_specifications(self, response):
        return {itm.css('.AdvertSpecs_label__2JHnS::text').get(): itm.css(
            '.AdvertSpecs_data__xK2Qx::text').get() or itm.css('a::text').get() for itm in
                response.css('.AdvertSpecs_row__ljPcX')}
