import json
import requests


class Parse5Ka:
    params = {
        'records_per_page': 100,
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.16; rv:82.0) Gecko/20100101 Firefox/82.0",
    }

    def __init__(self, start_url):
        self.start_url = start_url

    def parse(self):
        url = self.start_url
        params = self.params
        while url:
            response: requests.Response = requests.get(url, params=params, headers=self.headers)
            data = response.json()
            url = data.get('next')
            if params:
                params = {}
            for product in data.get('results', []):
                self.save_products(product)

    def save_products(self, product: dict):
        # todo переделать пути к файлам
        with open(f'products/{product["id"]}.json', 'w', encoding='UTF-8') as file:
            json.dump(product, file, ensure_ascii=False)


if __name__ == '__main__':
    url = 'https://5ka.ru/api/v2/special_offers/'
    parser = Parse5Ka(url)
    parser.parse()
