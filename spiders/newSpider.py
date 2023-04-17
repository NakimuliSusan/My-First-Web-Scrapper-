import hashlib
import uuid
from datetime import datetime
from typing import Dict

import scrapy
from attr import dataclass


@dataclass
class DocumentItem:
    """ Contains relevant scraped details.
    Attributes:
        id (uuid): unique identifier, identical for every scrape
        url (str): URL of scraped page
        scraper (str): name of scraper the item originated from
        version (str): e.g. 1.0.2
        data (dict): relevant scraped values
        timestamp (datetime): of the moment
        scraper_blobs List[PgDocumentBlob]: The blobs with the data for the individual files
    """
    id: str
    url: str
    scraper: str
    version: str
    timestamp: datetime
    data: Dict[str, str]

    @staticmethod
    def create_uid_from(name: str, *args) -> str:
        """ Generate a UID based on a spider name and given details. """
        encoded = f'{name}{"".join(str(a) for a in args)}'.encode()
        hashed = hashlib.sha256(encoded).hexdigest()
        return str(uuid.UUID(hashed[::2]))

    def serialize(self):
        dic = self.__dict__
        dic['source_file'] = None
        return dic


class WinesSpider(scrapy.Spider):
    name = "extract"
    start_urls = ['https://www.wine-selection.com/shop']

    def parse(self, response):
        for product in response.css('li.product'):
            item = DocumentItem(
                id=str(uuid.uuid4()),
                url=response.url,
                version="5",
                scraper=self.name,
                timestamp=datetime.utcnow(),
                data={}
            )
            item.data["name"] = product.css(
                'h2.woocommerce-loop-product_title a.woocommerce-LoopProduct-link::text').get()
            item.data["price"] = product.css('span.woocommerce-Price-amount > bdi::text').get()
            item.data["link"] = product.css('a::attr(href)').get()
            yield item

        for page in response.css('ul.page-numbers li'):
            href = page.css('a.page-numbers::attr(href)').get()
            if href is not None:
                yield scrapy.Request(href, callback=self.parse_page)

    def parse_page(self, response):
        for product in response.css('li.product'):
            item = DocumentItem(
                id=str(uuid.uuid4()),
                url=response.url,
                version="5",
                scraper=self.name,
                timestamp=datetime.utcnow(),
                data={}
            )
            item.data["name"] = product.css(
                'h2.woocommerce-loop-product_title a.woocommerce-LoopProduct-link::text').get()
            item.data["price"] = product.css('span.woocommerce-Price-amount > bdi::text').get()
            item.data["link"] = product.css('a::attr(href)').get()
            print(item)
            yield item
