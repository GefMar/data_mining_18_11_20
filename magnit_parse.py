import os
import dotenv
import requests
from urllib.parse import urljoin
import bs4
import pymongo as pm

dotenv.load_dotenv('.env')


class MagnitParser:

    def __init__(self, start_url):
        self.start_url = start_url
        mongo_client = pm.MongoClient(os.getenv('DATA_BASE'))
        self.db = mongo_client['parse_11']

    def _get(self, url: str) -> bs4.BeautifulSoup:
        # todo обработока статусов и повторные запросы
        response = requests.get(url)
        return bs4.BeautifulSoup(response.text, 'lxml')

    def run(self):
        soup = self._get(self.start_url)
        for product in self.parse(soup):
            self.save(product)

    def parse(self, soup: bs4.BeautifulSoup) -> dict:
        catalog = soup.find('div', attrs={'class': 'сatalogue__main'})

        for product in catalog.findChildren('a'):
            try:
                pr_data = {
                    'url': urljoin(self.start_url, product.attrs.get('href')),
                    'image': urljoin(self.start_url, product.find('img').attrs.get('data-src')),
                    'name': product.find('div', attrs={'class': 'card-sale__title'}).text,
                }
                yield pr_data
            except AttributeError:
                continue

    def save(self, data: dict):
        collection = self.db['magnit']
        collection.insert_one(data)


if __name__ == '__main__':
    parser = MagnitParser('https://magnit.ru/promo/?geo=moskva')
    parser.run()

#
# url = 'https://magnit.ru/promo/?geo=moskva'
#
# response = requests.get(url)
#
# url_p = urlparse(response.url)
# soup = bs4.BeautifulSoup(response.text, "lxml")
# catalog = soup.find('div', attrs={'class': 'сatalogue__main'})
# a = catalog.find('a')
# print(1)
