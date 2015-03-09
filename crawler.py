import calendar
import datetime
import logging
import redis
import time
import sys
from apscheduler.schedulers.blocking import BlockingScheduler
# from geopy.geocoders import Nominatim
from instagram import client
from instagram.models import ApiModel
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

CONFIG = {
    'client_id': '9ebbe900a23540f4a1c1d1806d3e1d93',
    'client_secret': '5e56519f23e843ce8723b8c4806cafed',
    'redirect_uri': 'http://localhost:8515/oauth_callback'
}

API_CONFIG = {
    'return_json': True
}

POST_TYPES = {
    'instagram': 0,
    'twitter': 1
}

SF_CRAWL_PARAMS = {
    'start_lat': 37.813725,
    'start_lng': -122.380526,
    'end_lat': 37.687768,
    'end_lng': -122.528842,
    'distance': 1000,
    'step_degree': -0.008,
    'start_time': '2015-02-16 16:00:01',
    'end_time': '2015-02-16 16:15:00'
}

# Instagram api client
instagram_api = client.InstagramAPI(**CONFIG)
# Mongodb client
mongo_client = MongoClient('localhost', 27017)
test_db = mongo_client.test_db
# Redis client
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
# Initialize a scheduler
scheduler = BlockingScheduler()


def convert_media_to_json(media_dict):
    media_dict_json = {}
    for k, v in media_dict.items():
        if isinstance(v, ApiModel):
            media_dict_json[k] = convert_media_to_json(v.__dict__)
        elif isinstance(v, datetime.date):
            media_dict_json[k] = calendar.timegm(v.timetuple())
        elif isinstance(v, list):
            media_dict_json[k] = [convert_media_to_json((hasattr(vi, '__dict__') and vi.__dict__) or vi) for vi in v]
        elif isinstance(v, dict):
            media_dict_json[k] = convert_media_to_json(media_dict[k])
        else:
            media_dict_json[k] = v

    return media_dict_json


def construct_media_dict(media):
    try:
        ig_object = convert_media_to_json(media.__dict__)
    except Exception as e:
        logging.error('Failed to convert media to json, {0}'.format(media))
        logging.error(e)
        ig_object = None

    media_dict = {
        'post_type': POST_TYPES['instagram'],
        'source_url': media.link,
        # coordinate
        'location': [media.location.point.latitude, media.location.point.longitude],
        'latitude': media.location.point.latitude,
        'longitude': media.location.point.longitude,
        'caption': (media.caption is not None and media.caption.text) or None,
        'image_url': {
            'standard_resolution_url': media.get_standard_resolution_url(),
            'low_resolution_url': media.get_low_resolution_url(),
            'thumbnail_url': media.get_thumbnail_url()
        },
        'created_time': calendar.timegm(media.created_time.timetuple()),
        'user': {
            'username': hasattr(media.user, 'username') and media.user.username or None,
            'full_name': hasattr(media.user, 'full_name') and media.user.full_name or None,
            'id': media.user.id,
            'profile_picture_url': hasattr(media.user, 'profile_picture') and media.user.profile_picture or None,
            'bio': hasattr(media.user, 'bio') and media.user.bio or None,
            'location': None
        },
        'ig_object': ig_object
    }
    return media_dict


def media_search_with_timeout(lat, lng, distance, count, min_timestamp, max_timestamp):
    trial = 0
    # We give it 3 trials for API timeout.
    while trial < 3:
        try:
            # 15 seconds for API timeout.
            # signal.alarm(15)
            media_search_ret = instagram_api.media_search(lat=str(lat), lng=str(lng), distance=distance, count=count,
                                                          max_timestamp=max_timestamp, min_timestamp=min_timestamp)
            # signal.alarm(0)
            return media_search_ret
        except Exception:
            # signal.alarm(0)
            logging.warn('#{0} trial api call failed...')
            trial += 1
            continue
    logging.error('All trials failed (lat:{0},lng:{1},distance:{2},count:{3},min_timestamp:{4},max_timestamp:{5})!'
                  .format(lat, lng, distance, count, min_timestamp, max_timestamp))
    # Return empty list if all trials failed.
    return []


def crawl_location_by_time(lat, lng, distance, count, min_timestamp, max_timestamp):
    media_ret = []
    media_max_timestamp = max_timestamp
    media_min_timestamp = min_timestamp
    while media_max_timestamp > media_min_timestamp:
        media_search_ret = media_search_with_timeout(lat=str(lat), lng=str(lng), distance=distance, count=count,
                                                     max_timestamp=media_max_timestamp,
                                                     min_timestamp=media_min_timestamp)
        media_appended = False
        timestamp = 0
        for media in media_search_ret:
            timestamp = calendar.timegm(media.created_time.timetuple())
            if media_min_timestamp <= timestamp <= media_max_timestamp:
                media_ret.append(media)
                media_appended = True

        # If no new media is appended or the number of media returned is less than 50,
        # we think we will not get new media if we request more. So we break the loop.
        if not media_appended or len(media_search_ret) < 50:
            break
        media_max_timestamp = timestamp - 1
    return media_ret


def crawl():
    start_time = datetime.datetime.strptime(redis_client.get('start_time'), '%Y-%m-%d %H:%M:%S')
    end_time = datetime.datetime.strptime(redis_client.get('end_time'), '%Y-%m-%d %H:%M:%S')
    crawl_by_time(start_time, end_time)

    # Set next crawl time range.
    next_start_time = start_time + datetime.timedelta(minutes=15)
    next_end_time = end_time + datetime.timedelta(minutes=15)
    redis_client.set('start_time', str(next_start_time))
    redis_client.set('end_time', str(next_end_time))
    logging.info('Set next crawl time range to [{0} - {1}]'.format(str(next_start_time), str(next_end_time)))


def crawl_by_time(start_time, end_time):
    # Get db collection.
    test_collection = test_db.test_collection

    lat = SF_CRAWL_PARAMS['start_lat']
    lng = SF_CRAWL_PARAMS['start_lng']
    distance = SF_CRAWL_PARAMS['distance']
    step_degree = SF_CRAWL_PARAMS['step_degree']
    max_timestamp = time.mktime(end_time.timetuple())
    min_timestamp = time.mktime(start_time.timetuple())
    logging.info('Crawling time range [{0} - {1}].'.format(start_time, end_time))

    # For dedup purpose.
    media_links = set()

    total_saved_media = 0
    while lat >= SF_CRAWL_PARAMS['end_lat']:
        while lng >= SF_CRAWL_PARAMS['end_lng']:
            try:
                # TODO: Tried to use geo_location to decide whether we want to
                # crawl a specific location. Cons are:
                # 1. Geo location service times out sometimes.
                # 2. Not accurate to decide not to crawl the area based on the pinpoint location.
                # 3. geo_location throws UnicodeErrorException sometimes.
                media_search_ret = crawl_location_by_time(lat, lng, distance, 1000, min_timestamp, max_timestamp)
                if media_search_ret:
                    saved_count = 0
                    dup_count = 0
                    for media in media_search_ret:
                        try:
                            if media.link and media.link not in media_links:
                                media_links.add(media.link)
                                media_dict = construct_media_dict(media)
                                test_collection.save(media_dict)
                                saved_count += 1
                                total_saved_media += 1
                            else:
                                dup_count += 1
                        except DuplicateKeyError:
                            logging.warn('Duplicate key: {0}'.format(media.link))
                            continue
                    logging.debug('lat {0}, lng {1}, {2} images, {3} saved/{4} dup'.format(
                        lat, lng, len(media_search_ret), saved_count, dup_count))
                else:
                    logging.debug('lat {0}, lng {1}, 0 images.'.format(lat, lng))
            except Exception as e:
                logging.error(e)
                logging.warn('Retrying...')
                continue
            # San Francisco is at 37.77493 latitude degree.
            # According to http://www.csgnetwork.com/degreelenllavcalc.html, 1000m at this latitude
            # is equivalent to 0.009 latitude degree.
            lng += step_degree
        # Update latitude, and reset longitude.
        lat += step_degree
        lng = SF_CRAWL_PARAMS['start_lng']
    logging.info('Finishing crawling from Instagram. Saved {0} media in total.'.format(total_saved_media))


def start():
    logging.info('=============================================')
    logging.info('[{0}] Start crawling from Instagram...'.format(datetime.datetime.now()))
    crawling_start_time = time.time()
    crawl()
    crawling_end_time = time.time()
    time_spent = int(crawling_end_time - crawling_start_time)
    logging.info('Time spent: {0}min {1}s'.format(time_spent / 60, time_spent % 60))
    logging.info('=============================================')


def get_nearest_start_time():
    nearest_start_timestamp = long(time.time() / (60 * 15) + 1) * 60 * 15
    return datetime.datetime.fromtimestamp(nearest_start_timestamp)


def one_time_crawl(start_time_str, end_time_str):
    start_time = datetime.datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
    end_time = datetime.datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
    crawl_by_time(start_time, end_time)


def start_scheduler(should_continue=False):
    # Config logging and alarm.
    logging.basicConfig(filename='crawler.log', level=logging.DEBUG)

    scheduler_start_time = get_nearest_start_time()
    redis_client.set('end_time', str(scheduler_start_time))
    if not should_continue:
        redis_client.set('start_time', str(scheduler_start_time - datetime.timedelta(minutes=14, seconds=59)))
    scheduler.add_job(start, 'interval', start_date=scheduler_start_time, minutes=15, misfire_grace_time=600)
    scheduler.start()


if __name__ == '__main__':
    if len(sys.argv) > 4:
        print 'Usage: crawler.py [-c|-n|-o]'
    elif len(sys.argv) >= 2:
        if sys.argv[1] == '-c':
            start_scheduler(should_continue=True)
        elif sys.argv[1] == '-o' and len(sys.argv) == 4:
            one_time_crawl(sys.argv[2], sys.argv[3])
        elif sys.argv[1] == '-n':
            start_scheduler(should_continue=False)
    else:
        start_scheduler(should_continue=False)
