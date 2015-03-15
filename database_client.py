from pymongo import MongoClient


class MongoDBClient(object):

    def __init__(self, address, port):
        self.client = MongoClient(address, port)
        self.db = self.client.absol_db
        self.collection = self.db.media_collection

    def insert_media(self, media):
        self.collection.insert(media)

    def search_media_by_time(self, min_timestamp, max_timestamp):
        media_results = self.collection.find({'created_time': {'$gt': min_timestamp, '$lt': max_timestamp}})
        return media_results

    def search_media_by_geo_and_time(self, lat_range, lng_range, time_range):
        media_results = self.collection.find({
            'location': {
                '$geoWithin': {
                    '$box': [
                        [lat_range[0], lng_range[0]],
                        [lat_range[1], lng_range[1]]
                    ]}},
            'created_time': {
                '$gt': time_range[0],
                '$lt': time_range[1]
            }
        })
        return media_results

INTERNAL_IP = '172.31.11.119'
PUBLIC_IP = '54.67.80.76'

mongo_client = MongoDBClient(INTERNAL_IP, 27017)