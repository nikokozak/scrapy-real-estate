# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import re
import csv

num_reg = re.compile('[0-9\.]+')

def parse_num(num_string, default=None):
    if num_string is not None:
        if type(num_string) == int or type(num_string) == float:
            return num_string
        elif num_reg.match(num_string):
            try:
                return int(num_string)
            except ValueError:
                return float(num_string)
        else:
            return default

def extract_number(number_string, default=None):
    amount_regex = re.compile('[0-9,]+')
    if number_string is not None:
        try:
            return amount_regex.search(number_string).group().replace(',', '')
        except AttributeError:
            return default

class TruliaPipeline:

    def open_spider(self, spider):
        self.csv_output = open('output.csv', 'w')
        self.csv_writer = csv.writer(self.csv_output, delimiter=',')
        self.csv_writer.writerow(['city', 'location', 'price', 'bedrooms', 'bathrooms', 'sqft', 'year'])

    def close_spider(self, spider):
        self.csv_output.close()

    def process_item(self, item, spider):
        if item['price']:
            item['price'] = parse_num(extract_number(item['price']))
        else:
            raise DropItem(f'Missing price for item {item["title"]}')

        item['details']['bedrooms'] = parse_num(extract_number(item['details']['bedrooms']))
        item['details']['bathrooms'] = parse_num(extract_number(item['details']['bathrooms']))
        item['details']['size'] = parse_num(extract_number(item['details']['size']))
        item['details']['year'] = parse_num(extract_number(item['details']['year']))

        self.csv_writer.writerow([
            item['city'],
            item['location'],
            item['price'],
            item['details']['bedrooms'],
            item['details']['bathrooms'],
            item['details']['size'],
            item['details']['year']])

        return item
