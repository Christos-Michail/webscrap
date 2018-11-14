from __future__ import print_function
import json
import rds_config
from datetime import datetime
import zipfile
import urllib
import boto3
import io
import gzip
from gzip import GzipFile
import csv
import uuid
import requests
import httpagentparser
import collections
import random
import operator
import logging
import pymysql
import sys
import credentials


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    # checks if the event received is for printing the program results
    try:
        if(event['print']):
            printResults()
            printUniqueResults()
    except KeyError:
        pass
# case the event is a Create file from S3 calls a method to read the file data
    try:
        key = event['Records'][0]['s3']['object']['key']
        processFile(key)
    except KeyError:
        pass
# checks if the event is an api request from the endpoint -browser
    try:
        if(event['request_type'] == 'browser'):
            return browser(event['start_date'], event['end_date'])
    except KeyError:
        pass
# checks if the event is an api request from the endpoint -os
    try:
        if(event['request_type'] == 'os'):
            return os(event['start_date'], event['end_date'])
    except KeyError:
        pass


# Saves all the event data to RDS
def savetoRDS(dictevents, key):
    # RDS credentials
    try:
        rds_host = 'mydbinstance.cywlauzq2hl0.eu-west-2.rds.amazonaws.com'
        name = rds_config.name
        password = rds_config.password
        db_name = rds_config.db_name
        conn = pymysql.connect(rds_host, user=name, passwd=password,
                               db=db_name, port=3306, connect_timeout=5)
        tempid = ''
        # Insert data to SQL using a cursor
        try:
            with conn.cursor() as cur:
                for event in dictevents['event']:
                    tempid = str(uuid.uuid1())
                    sql = """insert into Event (event_id, user_id, os,
                                                browser, event_date)
                                                values(%s,%s,%s,%s,%s)"""
                    args = (tempid, str(event['user_id']), str(event['os']), 
                            str(event['browser']), str(event['date']),)
                    cur.execute(sql, args)
                print(len(dictevents['event']), ' rows added from file ', key)
                conn.commit()
        except Exception as e:
            print('Could not update database',e)
    except Exception as e:
        print('RDS connection failed',e)


# method to read and process the data from zip file
def processFile(key):
    s3 = boto3.client('s3', region_name = 'eu-west-2', aws_access_key_id = credentials.aws_access_key_id,
                        aws_secret_access_key = credentials.aws_secret_access_key)
    bucket = 'my-yi-bucket'
    countries = []
    cities = []
    ipdata = []
    countries_counted = {}
    cities_counted = {}
    try:
        if '.gz'in key:
            # get object from S3 according to key and bucket name
            obj = s3.get_object(Bucket=bucket, Key=key)
            putObjects = []
            # make a file object from a zip file
            with io.BytesIO(obj['Body'].read()) as tf:
                gz_file = gzip.GzipFile(None, 'rb', fileobj=tf)
                reader = csv.reader(io.TextIOWrapper(gz_file), delimiter='\t')
                l = 0
                # dictionary to keep the event data
                dictevents = {'event': []}
                for line in reader:
                    # detect user agent string
                    os = httpagentparser.simple_detect(line[5])[0]
                    browser = httpagentparser.simple_detect(line[5])[1]
                    user_id = line[2]
                    ipdata = getlocation(line[4])
                    countries.append(ipdata[0])
                    cities.append(ipdata[1])
                    dictevents['event'].append({"os": os, "browser": browser,
                    "user_id": user_id, "date": line[0], })
                    # print(ipdata,'*******')
                    # break
        # save to RDS the event data - all rows needed for part B
        savetoRDS(dictevents, key)
        # count countries and cities using collections
        countries_counted = collections.Counter(countries)
        cities_counted = collections.Counter(cities)
        # save the counts for each to Dynamo to keep sum
        savetoDynamo(countries_counted, 'countries', 'country')
        savetoDynamo(cities_counted, 'cities', 'city')
    except Exception as e:
        print(e)
        raise e
    print('File process finished ', key)


# receives a dictionary of counted data , the dynamo table and the primary key
# to save in appropriate place
def savetoDynamo(counted, table_name, dykey):
    dynamodb_client = boto3.resource('dynamodb', region_name = 'eu-west-2', aws_access_key_id = credentials.aws_access_key_id,
                        aws_secret_access_key = credentials.aws_secret_access_key)
    for key in counted:
        dynamodb_client.Table(table_name).update_item(
            TableName=table_name,
            Key={
                dykey: key
                },
            UpdateExpression="ADD occurences:val",
            ExpressionAttributeValues={':val' : counted[key]}
        )


# returns location data using freegeoip -
# at some request no the api will stop sending back data
def getlocation(ip):
    temp = []
    try:
        api = 'https://freegeoip.net/json/'+ip
        responce = requests.get(api)
        data = responce.json()
        if len(data['country_name']) > 0:
            temp.append(data['country_name'])
        else: temp.append('unknown')
        if len(data['city']) > 0:
            temp.append(data['city'])
        else: temp.append('unknown')
        return temp
    except Exception as e:
        print('API exception',e)
        return (['unknown', 'unknown'])


# prints the aggregated data
def printUniqueResults():
    # query to return all distinct browsers from RDS
    query_browser = """select browser,count(distinct user_id) AS total from 
                       Event group by browser order by count(distinct user_id)
                       desc limit 5"""

    # query to return all distinct os from RDS
    query_os = """select os,count(distinct user_id) AS total from Event 
                  group by os order by count(distinct user_id) desc limit 5"""
    # send the query to RDS query method
    print('')
    print('Top 5 browsers : ')
    print(queryRds(query_browser))
    print('')
    print('Top 5 os : ')
    print(queryRds(query_os))


def printResults():
    dynamodb_client = boto3.resource('dynamodb', region_name = 'eu-west-2', aws_access_key_id = credentials.aws_access_key_id,
                        aws_secret_access_key = credentials.aws_secret_access_key)
    # scan the Dynamo db to get country counts
    countries_stats = dynamodb_client.Table('countries').scan()
    # sort the results
    sorted_cou_list = sorted(countries_stats['Items'], key=lambda
                             user: user['occurences'], reverse=True)
    # print the top 5 from sorted results
    print('Top 5 Countries : ')
    for line in sorted_cou_list[:5]:
        print(line['country'], line['occurences'])
    # scan the Dynamo db to get city counts
    cities_stats = dynamodb_client.Table('cities').scan(TableName='cities')
    # sort the results
    sorted_city_list = sorted(cities_stats['Items'], key=lambda
                              user: user['occurences'], reverse=True)
    # print the top 5 from sorted results
    print('')
    print('Top 5 cities : ')
    for line in sorted_city_list[:5]:
        print(line['city'], line['occurences'])


# Query the RDS and print result according to received query
def queryRds(query):
    try:
        rds_host = 'mydbinstance.cywlauzq2hl0.eu-west-2.rds.amazonaws.com'
        name = rds_config.name
        password = rds_config.password
        db_name = rds_config.db_name
        conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, 
                               port=3306, connect_timeout=100,)
        results = {}
        with conn.cursor() as cur:
            cur.execute(query)
            results['stats'] = list(cur)
            for row in cur:
                print(row)
        return results
    except Exception as e:
        print('RDS query not successful', e)


def browser(start_date,end_date):
    where_clause=''
    if start_date and end_date:
        where_clause = 'Where event_date Between ' + "'"+start_date+"'" + ' And ' + "'"+end_date+"'"

    query_api = 'Select browser, Concat(Count(browser)* 100 / (Select Count(*) From Event ' + where_clause + "),' %') From Event " \
                  + where_clause + ' group by browser order by Concat(Count(browser)* 100 / (Select Count(*) From Event ' \
                  + where_clause + """),' %') """+  'desc'
    return queryRds(query_api)


def os(start_date,end_date):
    where_clause=''
    if start_date and end_date:
        where_clause = 'Where event_date BETWEEN ' + "'" + start_date + "'" + ' And ' + "'" + end_date + "'"

    query_api = 'Select os, Concat(Count(os)* 100 / (Select Count(*) From Event '+where_clause+"),' %') From Event " \
                  + where_clause + ' group by os order by Concat(Count(os)* 100 / (Select Count(*) From Event ' \
                  + where_clause+"""),' %') """+  'desc'
    return queryRds(query_api)
