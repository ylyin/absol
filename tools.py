import sys
import time
import traceback
from cluster import DistanceMode, MediaClusterBuilder
from database_client import mongo_client
from flask import Flask
from flask import render_template


_CLUSTER_INFO_TEMPLATE = '<div class="clusterInfo"><a href="{0}" target="_blank">{1} media</a><br /><p>{2}</p></div>'
_MEDIA_IMAGE_TEMPLATE = '<img src="{0}" />'

app = Flask(__name__)

@app.route('/')
def internal_tools():
    try:
        return render_template('tools.html')
    except:
        traceback.print_exc(file=sys.stdout)


@app.route('/verify_clusters')
def verify_clusters_tool():
    try:
        media_cluster_builder = MediaClusterBuilder(distance_mode=DistanceMode.PAIRWISE_ALL)
        now_timestamp_s = long(time.time())
        one_day_ago_timestamp_s = now_timestamp_s - 60 * 60 * 12
        media_results = mongo_client.search_media_by_time(one_day_ago_timestamp_s, now_timestamp_s)
        data = dict()
        data['db_time_spent'] = int(time.time() - now_timestamp_s)
        for media in media_results:
            media_cluster_builder.track_media(media)
        global media_clusters
        media_clusters = media_cluster_builder.get_clean_sorted_clusters()
        data['center'] = {'lat': 37.813725, 'lng': -122.380526}
        data['time_spent'] = int(time.time() - now_timestamp_s)
        data['cluster_count'] = len(media_clusters)
        clusters = []
        for i in range(len(media_clusters)):
            media_cluster = media_clusters[i]
            media_content = ''
            for media in media_cluster.media_list:
                media_content += _MEDIA_IMAGE_TEMPLATE.format(media.get('image_url').get('thumbnail_url'))
            cluster_info = _CLUSTER_INFO_TEMPLATE.format('/cluster/{0}'.format(i), media_cluster.media_count, media_content)
            clusters.append({
                'media_count': media_cluster.media_count,
                'url': '/cluster/{0}'.format(i),
                'center': media_cluster.cluster_center,
                'cluster_info': cluster_info})
        data['clusters'] = clusters
        return render_template('clusters.html', data=data)
    except:
        traceback.print_exc(file=sys.stdout)


@app.route('/cluster/<int:cluster_id>')
def display_cluster(cluster_id):
    try:
        global media_clusters
        media_cluster = media_clusters[cluster_id]
        ret = ''
        for media in media_cluster.media_list:
            caption = media.get('caption')
            if caption:
                caption = caption.encode('utf-8')
            ret += '<p><img src="{0}" />{1}, location: ({2}, {3}), timestamp: {4}</p>'.format(
                media.get('image_url').get('thumbnail_url'),
                caption,
                media.get('lat'),
                media.get('lng'),
                media.get('timestamp')
            )
        return ret
    except:
        traceback.print_exc(file=sys.stdout)


if __name__ == '__main__':
    global media_clusters
    media_clusters = list()
    app.run()
