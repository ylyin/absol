import time
from enum import Enum
from database_client import mongo_client
from math import sqrt


class DistanceMode(Enum):
    CENTER = 0
    PAIRWISE_ALL = 1
    PAIRWISE_AVG = 2


class MediaCluster(object):

    def __init__(self, media):
        self.media_list = [media]
        self.cluster_center = {'lat': media.get('lat'), 'lng': media.get('lng')}
        self.media_count = 1

    def dominant_locaction_media(self):
        # Calculate the percentage of media with same location.
        dominant_location = None
        count = 0
        for media in self.media_list:
            if dominant_location != {'lat': media.get('lat'), 'lng': media.get('lng')}:
                if count == 0:
                    dominant_location = {'lat': media.get('lat'), 'lng': media.get('lng')}
                    count = 1
                else:
                    count -= 1
            else:
                count += 1
        count = 0
        for media in self.media_list:
            if dominant_location == {'lat': media.get('lat'), 'lng': media.get('lng')}:
                count += 1
        return float(count) / self.media_count


    @staticmethod
    def distance(m1, m2):
        dist = sqrt(
            (m1.get('lat') - m2.get('lat'))**2 +
            (m1.get('lng') - m2.get('lng'))**2)
        return dist

    def should_contain(self, media, distance_mode):
        if distance_mode == DistanceMode.PAIRWISE_AVG:
            # PAIRWISE_AVG means the average distance of media with every existing
            # media in the cluster.
            dist_avg = 0.0
            for m in self.media_list:
                dist_avg += self.distance(media, m)
            dist_avg /= self.media_count
            return dist_avg < 0.001
        elif distance_mode == DistanceMode.PAIRWISE_ALL:
            # PAIRWISE_ALL means every distance of media with existing media in cluster
            # should be less than 0.001.
            for m in self.media_list:
                dist = self.distance(media, m)
                if dist >= 0.001:
                    return False
            return True
        else:
            # CENTER is the default method.
            # CENTER means calculate the distance between media and cluster center.
            dist = self.distance(media, self.cluster_center)
            return dist < 0.001

    def add_media(self, media):
        self.media_list.append(media)
        self.media_count += 1
        self.cluster_center = {
            'lat': sum(m.get('lat') for m in self.media_list) / self.media_count,
            'lng': sum(m.get('lng') for m in self.media_list) / self.media_count
        }

    def get_radius(self):
        # Max distance between center and existing points in the cluster.
        max_dist = -1
        for media in self.media_list:
            dist = self.distance(media, self.cluster_center)
            if dist > max_dist:
                max_dist = dist
        return max_dist

    def __repr__(self):
        return 'MediaCluster({0} media): {1}'.format(self.media_count, self.media_list)


class MediaClusterBuilder(object):

    def __init__(self, distance_mode=None):
        self.clusters = list()
        self.distance_mode = distance_mode

    def track_media(self, media):
        # Construct a meta media dict, which is a single node.
        lat = media.get('latitude')
        lng = media.get('longitude')
        timestamp = media.get('created_time')
        source_url = media.get('source_url')
        image_url = media.get('image_url')
        caption = media.get('caption')
        if lat and lng and timestamp and source_url:
            meta_media = {
                'lat': lat,
                'lng': lng,
                'timestamp': timestamp,
                'source_url': source_url,
                'image_url': image_url,
                'caption': caption
            }

            # Check if we want to add it to one of the existing clusters.
            media_added = False
            for cluster in self.clusters:
                if cluster.should_contain(meta_media, self.distance_mode):
                    cluster.add_media(meta_media)
                    media_added = True
                    break

            # Create a new cluster if it is not added to one of the clusters.
            if not media_added:
                self.clusters.append(MediaCluster(meta_media))

    def get_clusters(self):
        return self.clusters

    def get_sorted_clusters(self):
        self.clusters.sort(key=lambda cluster: cluster.media_count, reverse=True)
        return self.clusters

    def get_clean_sorted_clusters(self):
        self.clusters.sort(key=lambda cluster: cluster.media_count, reverse=True)
        for i in range(min(3, len(self.clusters))):
            if self.clusters[i].dominant_locaction_media() > 0.85:
                self.clusters.remove(self.clusters[i])
        return self.clusters


def start():
    media_cluster_builder = MediaClusterBuilder()
    # Get all media of last 24 hours.
    now_timestamp_s = long(time.time())
    one_day_ago_timestamp_s = now_timestamp_s - 60 * 60 * 24
    media_results = mongo_client.search_media_by_time(one_day_ago_timestamp_s, now_timestamp_s)
    for media in media_results:
        media_cluster_builder.track_media(media)
    media_clusters = media_cluster_builder.get_clusters()
    media_clusters.sort(key=lambda cluster: cluster.media_count, reverse=True)
    print '{0} clusters, time spent {1}s:'.format(len(media_clusters), int(time.time() - now_timestamp_s))
    for media_cluster in media_clusters:
        print media_cluster


if __name__ == '__main__':
    start()
