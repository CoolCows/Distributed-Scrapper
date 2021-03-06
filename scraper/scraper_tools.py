from requests.models import ReadTimeoutError
from scraper.scraper_const import CONNECTION_ERROR, CONNECTION_TIMEOUT
from scrap_chord.util import remove_back_slashes
import requests
from requests.exceptions import ConnectTimeout, RequestException, MissingSchema, ConnectionError, ReadTimeout
from bs4 import BeautifulSoup
from urllib.parse import urlparse 

def extract_html(url):
    try:
        reqs = requests.get(url, timeout=(3, 3))
    except (RequestException, ValueError, ConnectTimeout) as exception:
        if isinstance(exception, MissingSchema):
            return extract_html("http://" + url)
        if isinstance(exception, (ConnectTimeout, ReadTimeout)):
            return CONNECTION_TIMEOUT, set()
        # if isinstance(exception, ConnectionError):
        #     return CONNECTION_ERROR, set()
        return "Bad Request", set()

    domain = get_header(url)
    
    soup = BeautifulSoup(reqs.text, "html.parser")
    urls = set()
    for link in soup.find_all("a"):
        l = link.get("href")
        if has_header(l):
            if get_header(l) != domain:
                continue
        else:
            l = url + l
        urls.add(remove_back_slashes(l))

    return reqs.text, urls

def get_header(url):
    domain = urlparse(url).netloc
    return domain

def has_header(url):
    domain = urlparse(url).netloc
    return domain != ""

