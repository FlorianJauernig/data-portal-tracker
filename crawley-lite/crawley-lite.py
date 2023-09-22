import os
import re
import json
import time
import string
import datetime
import argparse
import urllib.parse
from urllib.parse import urlparse, urljoin
from serpapi import GoogleSearch
from bs4 import BeautifulSoup


def extract_urls(html_string):
    url_pattern = re.compile(r'(?:http[s]?://)?(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
    urls = re.findall(url_pattern, html_string)
    return urls


def extract_urlsBS(base_url, html_string):
    soup = BeautifulSoup(html_string, 'html.parser')
    urls = []
    for tag in soup.find_all(['a', 'img', 'script', 'link', 'iframe']):
        if 'src' in tag.attrs:
            url = urljoin(base_url, tag.attrs['src'])
            urls.append(url)
        if 'href' in tag.attrs:
            url = urljoin(base_url, tag.attrs['href'])
            urls.append(url)
    return urls


def printProgressBar(iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r", color="black", overwrite=True):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    if overwrite != True:
        print(f'\r{prefix} |{bar}| {iteration}/{total} | {percent}% {suffix} ')
    else:
        print(f'\r{prefix} |{bar}| {iteration}/{total} | {percent}% {suffix} ', end="\r")
    if iteration == total:
        print()


def extract_base_url(url):
    parsed_url = urlparse(url)
    base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    return base_url


def url_to_filename(url):
    return ''.join(c for c in urllib.parse.quote(url, safe=string.ascii_letters + string.digits) if c not in ['/', ':', '*'])


def filename_to_url(filename):
    return urllib.parse.unquote(filename)


def searchesLeft(key):
    search = GoogleSearch({"api_key": key})
    account = search.get_account()
    return account['total_searches_left']


def saveResults(search):
    result = search.get_dict()
    filename = f"results/result_{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}_{now.second}_{ts}.json"
    with open(filename, "w", encoding='utf8') as outfile:
        json.dump(result, outfile, indent=4, ensure_ascii=False)
    print(f"Engine: {engine} | Query: {query} | Count: {count} | Offset: {offset} | File: {filename}")
    try:
        print(f"Organic results: {len(result['organic_results'])}")
        resultsOrganic = result['organic_results']
        try:
            lastResult = ""
            for r in resultsOrganic:
                lastResult = r['link']
            print(f"Last: {lastResult}")
        except Exception as e:
            print(e)
    except:
        print(f"Organic results: 0")

ts = time.time()
now = datetime.datetime.now()

parser=argparse.ArgumentParser()
parser.add_argument("--query", "-q", help="Query")
parser.add_argument("--offset", "-o", help="Offset on results (default is 0)")
parser.add_argument("--mkt", "-m", help="Target market (default: en-US)")
parser.add_argument("--count", "-c", help="Count of results per page (default is 10, max for Google 100)")
parser.add_argument("--engine", "-e", help="Currently only Google (serp.api)")
parser.add_argument("--all", "-a", help="All engines")
args=parser.parse_args()

""" Code for Bing was removed from here. """

keys = []
with open('keys.txt', 'r', encoding='utf-8') as file:
    for line in file:
        keys.append(line)
print(f"Available keys: {len(keys)}")

os.makedirs('results', exist_ok=True)

""" Code for --validate / --links arguments was removed from here. Use portal handler's validate_list() instead! """

query = None
if not args.query:
    raise Exception('No Query defined!')
else:
    query = args.query

mkt = args.mkt
if not args.mkt:
    mkt = "en-US"

offset = 0
if args.offset:
    offset = int(args.offset)

count = 10
if args.count:
    count = int(args.count)

engine = "None"
if args.engine:
    engine = args.engine

""" Commented (and unfinished?) code for Bing was removed from here. """

search = None

for k in keys:
    searchesLeftCheck = searchesLeft(k)
    print(f"Searches left: {searchesLeftCheck} | on key: {k}")
    if searchesLeftCheck > 0:
        print(f"Key to be used: {k}")
        key = k
        break

if args.all or engine == "Google":
    search = GoogleSearch({
        "q": query,
        "filter":0,
        "start":offset,
        "num":count,
        "api_key": key
      })
    saveResults(search)

""" Placeholder code for other search engines and commented logging code was removed from here. """
