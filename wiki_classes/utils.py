import urllib.request
from pathlib import Path

def download_counts(year = 2021, month = 10, day = 1, method = 'user'):
  url = f'https://dumps.wikimedia.org/other/pageview_complete/{year}/{year}-{month:02d}/pageviews-{year}{month:02d}{day:02d}-user.bz2'
  urllib.request.urlretrieve(url, url.split("/")[-1])
