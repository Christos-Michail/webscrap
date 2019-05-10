from __future__ import print_function
import json
import io
import gzip
import csv
import uuid
import collections
import pymysql
import credentials
import rds_config
import boto3
import requests
import httpagentparser


RDS_HOST = 'mydbinstance.cywlauzq2hl0.eu-west-2.rds.amazonaws.com'


def lambda_handler(event, context):
    """Main lambda listener receives an event and acts accordingly
    -Print program results
    -A new file was added to S3 and needs to be processed
    -Listens to Api calls and sends a response of aggregations
    """
    print("Received event: " + json.dumps(event, indent=2))
    # checks if the event received is for printing the program results
    try:
        if event['print']:
            print_results()
            print_unique_results()
    except key_error:
        pass
# case the event is a Create file from S3 calls a method to read the file data
    try:
        key = event['Records'][0]['s3']['object']['key']
        process_file(key)
    except key_error:
        pass
# checks if the event is an api request from the endpoint browser
    try:
        if event['request_type'] == 'browser':
            return browser(event['start_date'], event['end_date'])
    except key_error:
        pass
# checks if the event is an api request from the endpoint -os_type
    try:
        if event['request_type'] == 'os_type':
            return os_type(event['start_date'], event['end_date'])
    except key_error:
        pass


def save_to_rdsdb(dictevents, key):
    """Saves events to SQL database"""
    try:
        name = rds_config.name
        password = rds_config.password
        db_name = rds_config.db_name
        conn = pymysql.connect(RDS_HOST, user=name, passwd=password,
                               db=db_name, port=3306, connect_timeout=5)
        tempid = ''
        # Insert data to SQL using a cursor
        try:
            with conn.cursor() as cur:
                for event in dictevents['event']:
                    tempid = str(uuid.uuid1())
                    sql = """insert into Event (event_id, user_id, os_type,
                                                browser, event_date)
                                                values(%s,%s,%s,%s,%s)"""
                    args = (tempid, str(event['user_id']), str(event['os_type']),
                            str(event['browser']), str(event['date']),)
                    cur.execute(sql, args)
                print(len(dictevents['event']), ' rows added from file ', key)
                conn.commit()
        except Exception as except_error:
            print('Could not update database', except_error)
    except Exception as except_error:
        print('RDS connection failed', except_error)


def process_file(key):
    """Processes files from S3 (zip format) in a data pipeline manner"""
    s3 = boto3.client('s3', region_name='eu-west-2',
                      aws_access_key_id=credentials.aws_access_key_id,
                      aws_secret_access_key=credentials.aws_secret_access_key)
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
                    ipdata = get_location(line[4])
                    countries.append(ipdata[0])
                    cities.append(ipdata[1])
                    dictevents['event'].append({"os_type": os, "browser": browser,
                                                "user_id": user_id, "date": line[0], })
        # save to RDS the event data - all rows needed for part B
        save_to_rdsdb(dictevents, key)
        # count countries and cities using collections
        countries_counted = collections.Counter(countries)
        cities_counted = collections.Counter(cities)
        # save the counts for each to Dynamo to keep sum
        save_to_dynamo(countries_counted, 'countries', 'country')
        save_to_dynamo(cities_counted, 'cities', 'city')
    except Exception as ExceptError:
        print(ExceptError)
    print('File process finished ', key)


def save_to_dynamo(counted, table_name, dykey):
    """
    Receives a dictionary of counted data , the dynamo table and the primary key
    Saves accumulation of occurences for each file to dynamo table as key, values
    """
    dynamodb_client = boto3.resource('dynamodb', region_name='eu-west-2',
                                     aws_access_key_id=credentials.aws_access_key_id,
                                     aws_secret_access_key=credentials.aws_secret_access_key)
    for key in counted:
        dynamodb_client.Table(table_name).update_item(
            TableName=table_name,
            Key={
                dykey: key
                },
            UpdateExpression="ADD occurences:val",
            ExpressionAttributeValues={':val' : counted[key]}
        )


def get_location(ip):
    """Uses external GeoIP package to translate locations of coordinates"""
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
    except Exception as except_error:
        print('API exception', except_error)
        return (['unknown', 'unknown'])


def print_unique_results():
    """Prints all the data that were gathered through file processing and the database - and are unique"""
    # query to return all distinct browsers from RDS
    query_browser = """select browser,count(distinct user_id) AS total from
                       Event group by browser order by count(distinct user_id)
                       desc limit 5"""

    # query to return all distinct os_type from RDS
    query_os = """select os_type,count(distinct user_id) AS total from Event
                  group by os_type order by count(distinct user_id) desc limit 5"""
    # send the query to RDS query method
    print('')
    print('Top 5 browsers : ')
    print(query_rdsdb(query_browser))
    print('')
    print('Top 5 os_type : ')
    print(query_rdsdb(query_os))


def print_results():
    """Prints remaining results """
    dynamodb_client = boto3.resource('dynamodb', region_name='eu-west-2',
                                     aws_access_key_id=credentials.aws_access_key_id,
                                     aws_secret_access_key=credentials.aws_secret_access_key)
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
    sorted_city_list = sorted(cities_stats['Items'], key=lambda user: user['occurences'],
                              reverse=True)
    # print the top 5 from sorted results
    print('')
    print('Top 5 cities : ')
    for line in sorted_city_list[:5]:
        print(line['city'], line['occurences'])


# Query the RDS and print result according to received query
def query_rdsdb(query):
    """Receives a query for the database and returns the result"""
    try:
        name = rds_config.name
        password = rds_config.password
        db_name = rds_config.db_name
        conn = pymysql.connect(RDS_HOST, user=name, passwd=password, db=db_name,
                               port=3306, connect_timeout=100,)
        results = {}
        with conn.cursor() as cur:
            cur.execute(query)
            results['stats'] = list(cur)
            for row in cur:
                print(row)
        return results
    except Exception as except_error:
        print('RDS query not successful', except_error)


def browser(start_date, end_date):
    """Returns browser types from the database"""
    where_clause = ''
    if start_date and end_date:
        where_clause = 'Where event_date Between ' + "'"+start_date+"'" + ' And ' + "'"+end_date+"'"

    query_api = 'Select browser, Concat(Count(browser)* 100 / (Select Count(*) From Event ' \
                + where_clause \
                + "),' %') From Event " \
                  + where_clause + ' group by browser order by Concat(Count(browser)' \
                                   '* 100 / (Select Count(*) From Event ' \
                  + where_clause + """),' %') """ + 'desc'
    return query_rdsdb(query_api)


def os_type(start_date, end_date):
    """Returns Os types from the database"""
    where_clause = ''
    if start_date and end_date:
        where_clause = 'Where event_date BETWEEN ' + "'" + start_date + "'" \
                       + ' And ' + "'" + end_date + "'"

    query_api = 'Select os_type, Concat(Count(os_type)* 100 / (Select Count(*) From Event ' \
                + where_clause+"),' %') From Event " \
                  + where_clause + ' group by os_type order by Concat(Count(os_type)' \
                                   '* 100 / (Select Count(*) From Event ' \
                  + where_clause+"""),' %') """ + 'desc'
    return query_rdsdb(query_api)
