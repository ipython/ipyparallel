"""
An exceptionally lousy site spider
Ken Kinder <ken@kenkinder.com>

Updated for newparallel by Min Ragan-Kelley <benjaminrk@gmail.com>

This module gives an example of how the task interface to the
IPython controller works.  Before running this script start the IPython controller
and some engines using something like::

    ipcluster start -n 4
"""
from __future__ import print_function

import sys
import ipyparallel as ipp
import time
import bs4 # this isn't necessary, but it helps throw the dependency error earlier

try:
    raw_input
except NameError:
    raw_input = input

def fetchAndParse(url, data=None):
    import requests
    try:
        from urllib.parse import urljoin
    except ImportError:
        from urlparse import urljoin
    import bs4
    links = []
    r = requests.get(url, data=data)
    r.raise_for_status()
    if 'text/html' in r.headers.get('content-type'):
        doc = bs4.BeautifulSoup(r.text, "html.parser")
        for node in doc.findAll('a'):
            href = node.get('href', None)
            if href:
                links.append(urljoin(url, href))
    return links

class DistributedSpider(object):

    # Time to wait between polling for task results.
    pollingDelay = 0.5

    def __init__(self, site):
        self.client = ipp.Client()
        self.view = self.client.load_balanced_view()
        self.mux = self.client[:]

        self.allLinks = []
        self.linksWorking = {}
        self.linksDone = {}

        self.site = site

    def visitLink(self, url):
        if url not in self.allLinks:
            self.allLinks.append(url)
            if url.startswith(self.site):
                print('    ', url)
                self.linksWorking[url] = self.view.apply(fetchAndParse, url)

    def onVisitDone(self, links, url):
        print(url + ':')
        self.linksDone[url] = None
        del self.linksWorking[url]
        for link in links:
            self.visitLink(link)

    def run(self):
        self.visitLink(self.site)
        while self.linksWorking:
            print(len(self.linksWorking), 'pending...')
            self.synchronize()
            time.sleep(self.pollingDelay)

    def synchronize(self):
        for url, ar in list(self.linksWorking.items()):
            # Calling get_task_result with block=False will return None if the
            # task is not done yet.  This provides a simple way of polling.
            try:
                links = ar.get(0)
            except ipp.error.TimeoutError:
                continue
            except Exception as e:
                self.linksDone[url] = None
                del self.linksWorking[url]
                print('%s: %s' % (url, e))
            else:
                self.onVisitDone(links, url)

def main():
    if len(sys.argv) > 1:
        site = sys.argv[1]
    else:
        site = raw_input('Enter site to crawl: ')
    distributedSpider = DistributedSpider(site)
    distributedSpider.run()

if __name__ == '__main__':
    main()
