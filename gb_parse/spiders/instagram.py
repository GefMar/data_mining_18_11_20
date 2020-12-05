import json
import scrapy


class InstagramSpider(scrapy.Spider):
    name = 'instagram'
    allowed_domains = ['www.instagram.com']
    login_url = 'https://www.instagram.com/accounts/login/ajax/'
    start_urls = ['https://www.instagram.com/']
    
    def __init__(self, login, password, start_hash_tags: list, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.login = login
        self.password = password
        self.start_hash_tags = [f"/explore/tags/{tag}/" for tag in start_hash_tags]
    
    def parse(self, response):
        try:
            js_data = self.get_js_data(response)
            yield scrapy.FormRequest(
                self.login_url,
                method='POST',
                callback=self.parse,
                formdata={
                    'username': self.login,
                    'enc_password': self.password,
                },
                headers={'X-CSRFToken': js_data['config']['csrf_token']}
            )
        except Exception:
            data = response.json()
            if data['authenticated']:
                for tag in self.start_hash_tags:
                    yield response.follow(tag, callback=self.tag_page_parse)
        print(1)
    
    def tag_page_parse(self, response):
        data = self.get_js_data(response)
        print(1)
    
    def get_js_data(self, response) -> dict:
        json_text = response.xpath('//script[contains(text(), "window._sharedData")]/text()').get()
        return json.loads(json_text.replace("window._sharedData = ", '')[:-1])
