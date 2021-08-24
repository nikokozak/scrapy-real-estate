import scrapy
import re
import json

def load_cache(filename, default={}):
    try:
        with open(filename, 'r+') as cache:
            return json.load(cache)
    except (FileNotFoundError, json.JSONDecodeError) as _e:
        return default

def write_cache(filename, obj_to_write):
    with open(filename, 'w') as cache_out:
        json.dump(obj_to_write, cache_out)
        print(f'Successfully saved cache to {filename}')

class TruliaSpider(scrapy.Spider):
    name = 'trulia'

    def start_requests(self):
        self.url_id_regex = re.compile('[0-9]+$')
        self.urls_traversed = load_cache('./visited.json', {'New York': {}, 'Brooklyn': {}})
        self.ny_page_count = 0
        self.brooklyn_page_count = 0
        urls = {
            'New York': 'https://trulia.com/NY/New_York/',
            'Brooklyn': 'https://trulia.com/NY/Brooklyn/'
        }

        for city, url in urls.items():
            yield scrapy.Request(url=url, callback=self.parse, cb_kwargs=dict(city=city))

    def parse(self, response, city):
        listing_links = response.xpath('//div[contains(@data-testid, "home-card-sale")]')
        next_page_link = response.xpath('//a[@rel="next" and @aria-label="Next Page"]/@href').get()

        for link in listing_links:
            relative_url = link.xpath('.//a/@href').get()
            absolute_url = response.urljoin(relative_url)
            yield scrapy.Request(absolute_url, callback=self.parse_listing, cb_kwargs=dict(city=city))

        absolute_next_page_link = response.urljoin(next_page_link)
        if self.ny_page_count < 3:
            self.ny_page_count += 1
            yield scrapy.Request(absolute_next_page_link, callback=self.parse, cb_kwargs=dict(city=city))

    def parse_listing(self, response, city):
        match_result = self.url_id_regex.search(response.url)
        listing_id = match_result.group()
        print(listing_id)
        if not self.urls_traversed[city].get(listing_id, False):
            self.urls_traversed[city][listing_id] = True
            data_to_yield = {
                'city': city,
                'id': self.url_id_regex.search(response.url).group(),
                'location': response.xpath('//a[@data-testid="neighborhood-link"]/text()').get(),
                'price': response.xpath('//h3[@data-testid="on-market-price-details"]/div[contains(text(), "$")]/text()').get(),
                'details': {
                    'bedrooms': response.xpath('//div[contains(text(), "Beds") and string-length(text())<10]/text()').get(),
                    'bathrooms': response.xpath('//div[contains(text(), "Baths") and string-length(text())<10]/text()').get(),
                    'size': response.xpath('//li[@data-testid="floor"]//div[contains(text(), "sqft")]/text()').get(),
                    'year': response.xpath('//div[text()="Year Built"]/following-sibling::div/text()').get(),
                    'parking': response.xpath('//div[text()="Parking"]/following-sibling::div/text()').get(),
                    'heating': response.xpath('//div[text()="Heating"]/following-sibling::div/text()').get(),
                    'cooling': response.xpath('//div[text()="Cooling"]/following-sibling::div/text()').get()
                }
            }
            write_cache('./visited.json', self.urls_traversed)
            yield data_to_yield
