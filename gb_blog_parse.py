import bs4
import requests
from urllib.parse import urljoin

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import models

engine = create_engine('sqlite:///gb_blog.db')

models.Base.metadata.create_all(bind=engine)

session_maker = sessionmaker(bind=engine)


class GbBlogParse:

    def __init__(self, start_url):
        self.start_url = start_url
        self.page_done = set()

    def __get(self, url) -> bs4.BeautifulSoup:
        response = requests.get(url)
        self.page_done.add(url)
        soup = bs4.BeautifulSoup(response.text, 'lxml')
        return soup

    def run(self, url=None):
        if not url:
            url = self.start_url

        if url not in self.page_done:
            soup = self.__get(url)
            posts, pagination = self.parse(soup)

            for post_url in posts:
                page_data = self.page_parse(self.__get(post_url), post_url)
                self.save(page_data)
            for p_url in pagination:
                self.run(p_url)

    def parse(self, soup):
        ul_pag = soup.find('ul', attrs={'class': 'gb__pagination'})
        paginations = set(
            urljoin(self.start_url, url.get('href')) for url in ul_pag.find_all('a') if url.attrs.get('href'))
        posts = set(
            urljoin(self.start_url, url.get('href')) for url in soup.find_all('a', attrs={'class': 'post-item__title'}))
        return posts, paginations

    def page_parse(self, soup, url) -> dict:
        data = {
            'url': url,
            'title': soup.find('h1').text,
            'tags': []
        }
        for tag in soup.find_all('a', attrs={'class': "small"}):
            tag_data = {
                'url': urljoin(self.start_url, tag.get('href')),
                'name': tag.text
            }
            data['tags'].append(tag_data)
        return data

    def save(self, page_data:dict):
        db = session_maker()
        tags = []
        for tag in page_data['tags']:
            tmp_tag = db.query(models.Tag).filter(models.Tag.url == tag['url']).first()
            if not tmp_tag:
                tmp_tag = models.Tag(**tag)
                try:
                    db.add(tmp_tag)
                    db.commit()
                except Exception:
                    db.rollback()
            tags.append(tmp_tag)
        tmp_post = db.query(models.Post).filter(models.Post.url == page_data['url']).first()
        if not tmp_post:
            tmp_post = models.Post(url=page_data['url'], title=page_data['title'])

        tmp_post.tags.extend(tags)

        try:
            db.add(tmp_post)
            db.commit()
        except Exception:
            db.rollback()




        print(1)


if __name__ == '__main__':
    parser = GbBlogParse('https://geekbrains.ru/posts')
    parser.run()
