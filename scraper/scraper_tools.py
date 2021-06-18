import requests
from requests.exceptions import RequestException, MissingSchema
from bs4 import BeautifulSoup
from urllib.parse import urlparse 

def extract_html(url, logger):
    try:
        reqs = requests.get(url)
    except (RequestException, ValueError) as exception:
        if isinstance(exception, MissingSchema):
            return extract_html("http://" + url, logger)
        return "Bad Request", set()

    domain = get_header(url)
    
    soup = BeautifulSoup(reqs.text, "html.parser")
    urls = set()
    for link in soup.find_all("a"):
        l = link.get("href")
        if has_header(l):
            if get_header(l) != domain:
                logger.debug(f"{l} is outside the domain {domain}")
                continue
        else:
            l = url + l
        urls.add(l)

    return reqs.text, urls

def get_header(url):
    domain = urlparse(url).netloc
    return domain

def has_header(url):
    domain = urlparse(url).netloc
    return domain != ""
