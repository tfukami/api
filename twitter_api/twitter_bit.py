import requests
from requests_oauthlib import OAuth1Session
from pymongo import MongoClient
import MySQLdb
import tweepy
import time
from datetime import datetime
import sys
import os

api_key = os.environ.get('TWITTER_BIT_API_KEY')
api_secret = os.environ.get('TWITTER_BIT_API_SECRET')
access_token = os.environ.get('TWITTER_BIT_ACCESS_TOKEN')
access_secret = os.environ.get('TWITTER_BIT_ACCESS_SECRET')


stock_num = 0

url = 'https://stream.twitter.com/1.1/statuses/filter.json'
twitter = OAuth1Session(api_key, api_secret, access_token, access_secret)
auth = tweepy.OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_secret)


conn = MySQLdb.connect(
        host='localhost', 
        port=3306, 
        user='data_writer', 
        passwd=os.environ.get('MYSQL_PASSWORD'), 
        db='twitter', 
        charset='utf8mb4')
c = conn.cursor()
create_table = '''
          create table if not exists raw_bit_data (
            id integer not null auto_increment,
            twitter_id varchar(255),
            screen_name varchar(255),
            name varchar(255),
            followers_cnt integer,
            listed_cnt integer,
            created_at timestamp,
            text varchar(255),
            primary key (id)
          )
          '''
c.execute(create_table)
conn.commit()

class MySQLStreamListerner(tweepy.StreamListener):

    def on_status(self, status):
        global stock_num
        if status.text:
            stock_num += 1

            sql_insert = "insert into raw_bit_data (\
                    twitter_id,\
                    screen_name,\
                    name,\
                    followers_cnt,\
                    listed_cnt,\
                    created_at,\
                    text\
                    ) values (%s,%s,%s,%s,%s,%s,%s)"
            insert_item = (
                    status.user.id,
                    status.user.screen_name,
                    status.user.name,
                    status.user.followers_count,
                    status.user.listed_count,
                    status.created_at,
                    status.text
            )
            try:
                c.execute(sql_insert, insert_item)
                conn.commit()
                if stock_num % 1000.0 == 0.0:
                    print('STOCK : {} ITEMS.'.format(stock_num))
            except:
                stock_num -= 1
                try:
                    print('TWITTER ID : {} LOSE'.format(status.user.id), datetime.now())
                    print('INSERT_ITEM:{}'.format(insert_item))
                except:
                    print('Number:{} LOSE'.format(stock_num), datetime.now())
            if stock_num == 1000000:
                print('GET {} GET-TWEET'.format(stock_num), datetime.now())
                sys.exit()
        return True

    def on_error(self, status_code):
        print('error : ', str(status_code))
        return True

stock_num = 0
stream = tweepy.Stream(auth, MySQLStreamListerner())
while True:
    try:
        stream.filter(languages=['ja'], track=['CoffeeCoin,CFC',
            'StrongHands,SHND',
            'KemCredit,KMC',
            'FlappyCoin,FLAP',
            'DimonCoin,FUDD',
            'BunnyCoin,BUN',
            'BatCoin,BAT',
            'RabbitCoin,RBBT',
            'OCOW,OCOW',
            'DixAsset,DIX',
            'Farstcoin,FRCT',
            'Sprouts,SPRTS',
            'Kittehcoin,MEOW',
            'SwapToken,TOKEN',
            'PeepCoin,PCN',
            'Selfiecoin,SLFI',
            'LePen,LEPEN',
            'TheCypherfunks,FUNK',
            'LottoCoin,LOT',
            'PACcoin,PAC',
            'HempCoin,HMP',
            'AppleCoin,APW',
            'Zeitcoin,ZEIT',
            'GCoin,GCN',
            'NamoCoin,NAMO',
            'Protean,PRN',
            'Dimecoin,DIME',
            'Swisscoin,SIC',
            'InflationCoin,IFLT',
            'Karmacoin,KARMA',
            'EXRNchain,EXRN',
            'EmberCoin,EMB',
            'FedoraCoin,TIPS',
            'Bitok,BITOK',
            'MoneyCoin,MONEY',
            'PeopleCoin,MEN',
            'KashhCoin,KASHH',
            'TeraCoin,TERA',
            'SmileyCoin,SMLY',
            'UNCoin,UNC',
            'Valorbit,VAL',
            'Rcoin,RCN',
            'PayPeer,PAYP',
            'Yescoin,YES',
            'SproutsExtreme,SPEX',
            'HitCoin,HTC',
            'Veros,VRS',
            'ReeCoin,REE',
            '808Coin,808',
            'VapersCoin,VPRC',])
#        stream.filter(languages=['ja'], track=['bitcoin','$ETH','BITCOIN','#ETH','BTC'])
#        stream.filter(languages=['ja'], locations=[128.416338,30.443678,146.113366,44.982576])
    except SystemExit:
        import tarceback
        traceback.print_exc()
        break
    except:
        print('error occured.reconnection start in 60sec.', datetime.now())
        time.sleep(60)

