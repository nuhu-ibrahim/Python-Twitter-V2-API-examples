# imports
import requests
import os
import json
import time
import pymongo
import datetime
import numpy as np
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta


# returns the bearer token
def auth():
    # TODO add bearer token of developer Twitter for academic research (!important)
    bearer_token = ""
    return bearer_token


# creates and returns the request header
def create_headers(bearer_token):
    """
    Parameters
    ----------
    bearer_token : str
        Twitter bearer token
    """
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


# creates a url for looking up a twitter username
def create_lookup_url(usernames, user_fields):
    """
    Parameters
    ----------
    usernames : str
        Username that you want to search.
        => Sample: usernames=TwitterDev <=

    user_fields : str
        User fields are adjustable, options include:
        created_at, description, entities, id, location, name,
        pinned_tweet_id, profile_image_url, protected,
        public_metrics, url, username, verified, and withheld
        => Sample: user.fields=description,created_at <=
    """
    url = "https://api.twitter.com/2/users/by?{}&{}".format(
        usernames, user_fields)
    return url


# sends a request to the username lookup url and catch the exception if any happens
def connect_to_lookup_endpoint(url, headers):
    response = requests.request("GET", url, headers=headers)

    # print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


# lookup a username on twitter with some specific fields and return a response
def lookup_user(username, user_fields=""):
    """
    Parameters
    ----------
    username : str
        Username that you want to search.
        => Sample: usernames=TwitterDev <=

    user_fields : str
        User fields are adjustable, options include:
        created_at, description, entities, id, location, name,
        pinned_tweet_id, profile_image_url, protected,
        public_metrics, url, username, verified, and withheld
        => Sample: user.fields=description,created_at <=
    """

    bearer_token = auth()
    url = create_lookup_url(username, user_fields)
    headers = create_headers(bearer_token)
    json_response = connect_to_lookup_endpoint(url, headers)

    return json_response


# returns true if a twitter username exists and false otherwise
def confirm_username(username):
    """
    Parameters
    ----------
    usernames : str
        Username that you want to search.
        => TwitterDev <=
    """
    username_attr = "usernames="+username
    user_response = lookup_user(username_attr)

    if 'data' in user_response and username == user_response['data'][0]['username']:
        return True

    return False


# returns the twitter full archive url
def full_archive_url():
    search_url = "https://api.twitter.com/2/tweets/search/all"

    return search_url


# connects to the full archive url and return a response or throw an exception
def connect_to_search_endpoint(url, headers, params):
    search_url = full_archive_url()
    response = requests.request(
        "GET", search_url, headers=headers, params=params)

    # print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

# searches for a query on twitter and return the response


def keyword_spartial_search(query, start_date_request, end_date_request, next_token=0, tweets_per_request=100):
    search_url = full_archive_url()
    query_params = {
        'query': query,
        'max_results': tweets_per_request,
        'start_time': start_date_request,
        'end_time': end_date_request,
        'tweet.fields': 'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld',
        'expansions': 'attachments.poll_ids,attachments.media_keys,author_id,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id',
        'user.fields': 'id,username,location,created_at,description',
        'place.fields': 'contained_within,country,country_code,full_name,geo,id,name,place_type',
        'media.fields': 'duration_ms,height,media_key,preview_image_url,type,url,width,public_metrics'
    }

    if next_token != 0:
        query_params['next_token'] = next_token

    bearer_token = auth()
    headers = create_headers(bearer_token)
    json_response = connect_to_search_endpoint(
        search_url, headers, query_params)

    return json_response


# extracts tweets from every response
def extract_response_tweets(response):
    tweets = []
    if 'data' in response and len(response['data']) != 0:
        tweets = response['data']

    return tweets


# extracts media from every response
def extract_response_medias(response):
    media = []
    if 'includes' in response and 'media' in response['includes'] and len(response['includes']['media']) != 0:
        media = response['includes']['media']

    return media


# extract users from every response
def extract_response_users(response):
    users = []
    if 'includes' in response and 'users' in response['includes'] and len(response['includes']['users']) != 0:
        users = response['includes']['users']

    return users


# extract places from every response
def extract_response_places(response):
    places = []
    if 'includes' in response and 'places' in response['includes'] and len(response['includes']['places']) != 0:
        places = response['includes']['places']

    return places


# extract tweet information from every response
def extract_response_tweets_info(response):
    tweets_info = []
    if 'includes' in response and 'tweets' in response['includes'] and len(response['includes']['tweets']) != 0:
        tweets_info = response['includes']['tweets']

    return tweets_info


# extract errors from every response
def extract_response_errors(response):
    errors = []
    if 'errors' in response and len(response['errors']) != 0:
        errors = response['errors']

    return errors


# extracts token from every response
def extract_next_token(response):
    next_token = 0
    if 'meta' in response and 'next_token' in response['meta']:
        next_token = response['meta']['next_token']

    return next_token


# checks whether the search should be continued
def should_continue_search(tweets_count, number_of_tweets, next_token):
    continue_search = False
    if next_token != 0 and not (number_of_tweets != -1 and tweets_count > number_of_tweets):
        continue_search = True
    else:
        # Search should end if there is no next token or if expected tweets have been found
        continue_search = False

    return continue_search


# performs a search based on set conditions, i.e., both date span and minimum number of tweets
def retrieve_keyword_spartial(keyword_query, spatial_query, start_date, end_date, number_of_tweets=-1):
    is_first = True
    continue_search = False
    search_response = None
    next_token_ = 0
    query = ' '.join([keyword_query, spatial_query])
    tweets = []
    users = []
    places = []
    tweet_infos = []
    errors = []
    media = []

    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    start_date_request = str(start_date_obj.replace(
        tzinfo=timezone.utc)).replace(" ", "T")

    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    end_date_request = str(end_date_obj.replace(
        tzinfo=timezone.utc)).replace(" ", "T")

    while is_first or continue_search:
        if is_first:
            # performs the first search without next token
            search_response = keyword_spartial_search(
                query, start_date_request, end_date_request)
            is_first = False
        else:
            # Sleep for 3.1s per request because of twitter api limit
            time.sleep(3.1)

            search_response = keyword_spartial_search(
                query, start_date_request, end_date_request, next_token_)

        # insert the returned tweets from the response into an array compiling the searches
        extracted_tweet = extract_response_tweets(search_response)
        tweets = np.append(tweets, extracted_tweet)

        # insert the returned users from the response into an array compiling the searches
        extracted_users = extract_response_users(search_response)
        users = np.append(users, extracted_users)

        # insert the returned places from the response into an array compiling the searches
        extracted_places = extract_response_places(search_response)
        places = np.append(places, extracted_places)

        # insert the returned tweet info from the response into an array compiling the searches
        extracted_tweet_info = extract_response_tweets_info(search_response)
        tweet_infos = np.append(tweet_infos, extracted_tweet_info)

        # insert the returned response_errors from the response into an array compiling the searches
        extracted_response_errors = extract_response_errors(search_response)
        errors = np.append(errors, extracted_response_errors)

        # insert the returned medias from the response into an array compiling the searches
        extracted_response_media = extract_response_medias(search_response)
        media = np.append(media, extracted_response_media)

        # extract token
        next_token = extract_next_token(search_response)

        # check if search should continue
        continue_search = should_continue_search(
            len(tweets), number_of_tweets, next_token)

    return json.dumps(tweets.tolist()), json.dumps(users.tolist()), json.dumps(places.tolist()), json.dumps(tweet_infos.tolist()), json.dumps(errors.tolist()), json.dumps(media.tolist())


# connects to mongo db
def connect_to_mongo():
    # TODO add your updated mongodb connection string (!important)
    return MongoClient("")


# send data to mongo db
def send_to_mongo(dbname, dbcollection, data):
    client = connect_to_mongo()
    db = getattr(client, dbname)
    db_collection = getattr(db, dbcollection)

    db_collection.insert_one(data)


# code execution point
if __name__ == "__main__":
    # Catch exception and save the exceptions in mongo db

    try:
        # Keyword and spartial search => Similar to SQL boolean logic but nothing for AND.
        keyword_query = '(football OR "Manchester United" OR "Paul Pogba") lang:en'
        spatial_query = 'place_country:GB OR place_country:US'

        # search conditions, i.e, date span and number of tweet. If you want to use only date span, pass number of tweets as -1 or don't pass it at all
        start_date_request_ = '2015-08-15'
        end_date_request_ = '2015-08-17'
        number_of_tweets = 100

        # send the request and accept the response
        tweets, users, places, tweets_info, errors, media = retrieve_keyword_spartial(keyword_query, spatial_query,
                                                                                      start_date_request_, end_date_request_, number_of_tweets)
        # compress the responses into a single dictionary object
        data = {
            "keyword": keyword_query,
            "spartial": spatial_query,
            "data": {
                "tweets": tweets,
                "users": users,
                "places": places,
                "tweets_info": tweets_info,
                "errors": errors,
                "media": media
            },
            "was_successful": True
        }

        # save the response into a database
        dbname = "project"
        dbcollection = "tweets"
        send_to_mongo(dbname, dbcollection, data)
    except Exception as inst:
        data = {
            "keyword": keyword_query,
            "spartial": spatial_query,
            "data": {},
            "was_successful": False,
            "exception": str(inst)
        }

        # save the response into a database
        dbname = "project"
        dbcollection = "tweets"
        send_to_mongo(dbname, dbcollection, data)
