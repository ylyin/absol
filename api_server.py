import time
import traceback
import sys
# from database_client import mongo_client
from cluster import MediaClusterBuilder, DistanceMode
from flask import Flask
from flask_restful import reqparse, abort, Api, Resource

application = Flask(__name__)
api = Api(application)

_CITY_CONFIGS = {
    'sf': {
        'lat_range': [37.687768, 37.813725],
        'lng_range': [-122.528842, -122.380526]
    }
}


def abort_if_city_doesnt_exist(city):
    if city not in _CITY_CONFIGS:
        abort(404, message='City {0} does not exist.'.format(city))

parser = reqparse.RequestParser()
parser.add_argument('')


class Clusters(Resource):
    def get(self, city):
        abort_if_city_doesnt_exist(city)
        try:
            media_cluster_builder = MediaClusterBuilder(distance_mode=DistanceMode.PAIRWISE_ALL)
            # Compute time range.
            now_timestamp_s = long(time.time())
            one_day_ago_timestamp_s = now_timestamp_s - 60 * 60 * 48
            time_range = [one_day_ago_timestamp_s, now_timestamp_s]
            lat_range = _CITY_CONFIGS[city]['lat_range']
            lng_range = _CITY_CONFIGS[city]['lng_range']
            # Get media results from mongodb.
            # media_results = mongo_client.search_media_by_geo_and_time(lat_range, lng_range, time_range)
            media_results = []
            # Prepare returned data.
            data = dict()
            data['db_time_spent'] = int(time.time() - now_timestamp_s)
            # Generate clusters.
            for media in media_results:
                media_cluster_builder.track_media(media)
            media_clusters = media_cluster_builder.get_clean_sorted_clusters()
            data['center'] = {'lat': 37.813725, 'lng': -122.380526}
            data['time_spent'] = int(time.time() - now_timestamp_s)
            data['cluster_count'] = len(media_clusters)
            # Construct clusters for returning.
            clusters = []
            for i in range(min(100, len(media_clusters))):
                media_cluster = media_clusters[i]
                clusters.append({
                    'media_count': media_cluster.media_count,
                    'center': media_cluster.cluster_center,
                    'radius': media_cluster.get_radius(),
                    'media_list': media_cluster.media_list})
            data['clusters'] = clusters
            return data
        except:
            traceback.print_exc(file=sys.stdout)
            return 'Something is wrong! We are taking care of it!'


api.add_resource(Clusters, '/clusters/<city>')


if __name__ == '__main__':
    application.run()