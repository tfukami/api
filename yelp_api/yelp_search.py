"""
Yelp Fusion API code sample.

This program demonstrates the capability of the Yelp Fusion API
by using the Search API to query for businesses by a search term and location,
and the Business API to query additional information about the top result
from the search query.

Please refer to http://www.yelp.com/developers/v3/documentation for the API
documentation.

This program requires the Python requests library, which you can install via:
`pip install -r requirements.txt`.

Sample usage of the program:
`python sample.py --term="bars" --location="San Francisco, CA"`
"""
import argparse
import json
import pprint
import requests
import sys
import urllib
import os
import re
from logging import getLogger,Formatter,StreamHandler,FileHandler,DEBUG
import MySQLdb
import time

from urllib.error import HTTPError
from urllib.parse import quote
from urllib.parse import urlencode

logger = getLogger('dev-debug')
formatter = Formatter\
        ('%(asctime)s [%(levelname)s] [%(filename)s: \
        %(funcName)s: %(lineno)d] %(message)s')
handlerSh = StreamHandler()
handlerFile = FileHandler('error.log')
handlerSh.setFormatter(formatter)
handlerSh.setLevel(DEBUG)
handlerFile.setFormatter(formatter)
handlerFile.setLevel(DEBUG)
logger.setLevel(DEBUG)
logger.addHandler(handlerSh)
logger.addHandler(handlerFile)
logger.debug('log start')

# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app
API_KEY= os.environ.get("YELP_API_KEY") 


# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'
BUSINESS_PATH = '/v3/businesses/'  # Business ID will come after slash.


# Defaults for our simple example.
DEFAULT_TERM = 'dinner'
DEFAULT_LOCATION = 'San Francisco, CA'
SEARCH_LIMIT = 50
SORT_BY = 'rating'
LOCALE = 'jp_JP'

class DataHandle():
    def start_db(self):
        params = {
                'host':'localhost',
                'db':'yelp',
                'user':'data_writer',
                'passwd':os.environ.get('MYSQL_PASSWORD'),
                'charset':'utf8mb4',
                }

        self.conn = MySQLdb.connect(**params)
        self.c = self.conn.cursor()
        self.c.execute('''
            create table if not exists raw_data(
                id integer not null auto_increment,
                business_id varchar(1000),
                name varchar(1000),
                url varchar(1000),
                latitude double,
                longitude double,
                city varchar(255),
                country varchar(255),
                zip_code varchar(100),
                category_alias varchar(255),
                category_title varchar(255),
                review_count int,
                rating float,
                price varchar(10),
                search_term varchar(255),
                search_location varchar(255),
                primary key(id)
            )
        ''')
        self.conn.commit()


    def close_db(self):
        self.conn.close()


    def insert_item(self, item):
        if not item:
            logger.Info('No Item')
            return

        check_sql = \
                'select business_id from raw_data where business_id = "{}"'\
                .format(item['business_id'])
        logger.debug('SQL:{}'.format(check_sql))
        self.c.execute(check_sql)
        self.conn.commit()

        if self.c.fetchall():
            logger.debug('Item already exists in db')
        elif item['country'] == 'JP':
            logger.debug('Add new item')
            insert_query = "insert into raw_data (\
                    business_id,\
                    name, \
                    url, \
                    latitude, \
                    longitude, \
                    city, \
                    country, \
                    zip_code, \
                    category_alias, \
                    category_title, \
                    review_count, \
                    rating, \
                    price, \
                    search_term, \
                    search_location) \
                    values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            insert_item = (
                    item['business_id'],
                    item['name'],
                    item['url'],
                    item['latitude'],
                    item['longitude'],
                    item['city'],
                    item['country'],
                    item['zip_code'],
                    item['category_alias'],
                    item['category_title'],
                    item['review_count'],
                    item['rating'],
                    item['price'],
                    item['search_term'],
                    item['search_location']
                    )
            self.c.execute(insert_query, insert_item)
            self.conn.commit()
        else:
            logger.warning('Item has not JP info:{}'.format(item['business_id']))


def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.

    Returns:
        dict: The JSON response from the request.

    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    logger.debug('Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def search(api_key, term, location, offset):
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.

    Returns:
        dict: The JSON response from the request.
    """
    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        #'locale': LOCALE,
        'limit': SEARCH_LIMIT,
        'offset': offset
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)


def get_business(api_key, business_id):
    """Query the Business API by a business ID.

    Args:
        business_id (str): The ID of the business to query.

    Returns:
        dict: The JSON response from the request.
    """
    business_path = BUSINESS_PATH + business_id

    return request(API_HOST, business_path, api_key)


def query_api(term, location):
    """Queries the API by the input values from the user.

    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """
    response = search(API_KEY, term, location, 0)
    businesses = response.get('businesses')
    if not businesses:
        logger.info('response contents: {}'.format(response.text))
        logger.info('response header: {}'.format(response.headers))

    dh = DataHandle()
    dh.start_db()

    off = 0
    while(businesses):
        logger.debug('TOTL_NUM:{}'.format(response.get('total')))
        logger.info('OFFSET:{}'.format(off))
        logger.info('business num:{}'.format(len(businesses)))
        for bus in businesses:
            item = {}
            if not bus['id']:
                pass

            try:
                item['name'] = bus['name']
            except:
                item['name'] = None
                logger.warning('Item has No name in {}'.format(bus['id']))
            try:
                item['url'] = bus['url']
            except:
                item['url'] = None
                logger.warning('Item has No url in {}'.format(bus['url']))
            try:
                item['latitude'] = bus['coordinates']['latitude']
                item['longitude'] = bus['coordinates']['longitude']
            except:
                item['latitude'] = None
                item['longitude'] = None
                logger.warning('Item has no coordinate in {}'\
                        .format(bus['id']))
            try:
                item['city'] = bus['location']['city']
            except:
                item['city'] = None
                logger.warning('Item has no city name in {}'.format(bus['id']))
            try:
                item['country'] = bus['location']['country']
            except:
                item['country'] = None
                logger.warning('Itemn has no coutry name in {}'.format(bus['id']))
            try:
                item['zip_code'] = bus['location']['zip_code']
            except:
                item['zip_code'] = None
                logger.warning('Item has no zip_code in {}'.format(bus['id']))
            try:
                a, t = '', ''
                for cate in bus['categories']:
                    a += cate['alias'] + ','
                    t += cate['title'] + ','
                item['category_alias'] = a
                item['category_title'] = t
            except:
                item['category_alias'] = None
                item['category_title'] = None
                logger.warning('Item has no category in {}'.format(bus['id']))
            try:
                item['review_count'] = bus['review_count']
            except:
                item['review_count'] = None
                logger.warning('Item has no review in {}'.format(bus['id']))
            try:
                item['rating'] = bus['rating']
            except:
                item['rating'] = None
                logger.warning('Item has no rating in {}'.format(bus['id']))
            try:
                item['price'] = bus['price']
            except:
                item['price'] = None
                logger.warning('Item has no price in {}'.format(bus['id']))

            dh.insert_item(
                    {
                        'business_id': bus['id']\
                        , 'name': item['name']\
                        , 'url': item['url']\
                        , 'latitude': item['latitude']\
                        , 'longitude': item['longitude']\
                        , 'city': item['city']\
                        , 'country': item['country']\
                        , 'zip_code': item['zip_code']\
                        , 'category_alias': item['category_alias']\
                        , 'category_title': item['category_title']\
                        , 'review_count': item['review_count']\
                        , 'rating': item['rating']\
                        , 'price': item['price']\
                        , 'search_term': term\
                        , 'search_location': location\
                        }
                    )

        off += SEARCH_LIMIT
        response = search(API_KEY, term, location, off)
        businesses = response.get('businesses')

    dh.close_db()


def main():
    #parser = argparse.ArgumentParser()

    #parser.add_argument('-q', '--term', dest='term', default=DEFAULT_TERM,
    #                    type=str, help='Search term (default: %(default)s)')
    #parser.add_argument('-l', '--location', dest='location',
    #                    default=DEFAULT_LOCATION, type=str,
    #                    help='Search location (default: %(default)s)')

    #input_values = parser.parse_args()

    location_item = ['北海道','青森','山形','秋田','岩手','福島','宮城',\
            '栃木','茨城','群馬','埼玉','千葉','東京','神奈川','山梨',\
            '新潟','石川','福井','長野','富山',\
            '愛知','岐阜','三重','静岡',\
            '滋賀','奈良','京都','和歌山','大阪','兵庫',\
            '岡山','広島','鳥取','島根','山口',\
            '高知','愛媛','徳島','香川',\
            '福岡','長崎','大分','佐賀','熊本','宮崎','鹿児島','沖縄',
            '日本','仙台','横浜','名古屋','神戸']
    temp_item = ['tokyo','新宿','渋谷','恵比寿','品川','池袋','上野','銀座',\
            '八王子','有楽町','歌舞伎町','目黒','目白','秋葉原','浅草','青山',\
            '三重県']

    try:
        #query_api(input_values.term, input_values.location)
        for loc in temp_item:
            logger.debug('search-area:{}'.format(loc))
            query_api('', loc)
    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )


if __name__ == '__main__':
    main()
