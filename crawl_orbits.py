#!/usr/bin/env python
"""
Crawl orbits and submit orbit dataset generation jobs.
"""

from future import standard_library
standard_library.install_aliases()
from builtins import str
import os, sys, re, json, logging, traceback, requests, argparse, backoff
from datetime import datetime, timedelta
from pprint import pformat
from requests.packages.urllib3.exceptions import (InsecureRequestWarning,
                                                  InsecurePlatformWarning)
try: from html.parser import HTMLParser
except: from html.parser import HTMLParser

from hysds_commons.job_utils import submit_mozart_job
from hysds.celery import app
from bs4 import BeautifulSoup

# disable warnings for SSL verification
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
requests.packages.urllib3.disable_warnings(InsecurePlatformWarning)


# set logger
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('crawl_orbits')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


#QC_SERVER = 'https://qc.sentinel1.eo.esa.int/'
#QC_SERVER = 'http://aux.sentinel1.eo.esa.int/'
#DATA_SERVER = 'http://aux.sentinel1.eo.esa.int/'

QC_SERVER = 'https://s1qc.asf.alaska.edu/'
DATA_SERVER = 'https://s1qc.asf.alaska.edu/'

ORBITMAP = [('precise','aux_poeorb', 100),
            ('restituted','aux_resorb', 100)]

OPER_RE = re.compile(r'S1\w_OPER_AUX_(?P<type>\w+)_OPOD_(?P<yr>\d{4})(?P<mo>\d{2})(?P<dy>\d{2})')


def cmdLineParse():
    """Command line parser."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ds_es_url", help="ElasticSearch URL for datasets, e.g. " +
                        "http://aria-products.jpl.nasa.gov:9200")
    parser.add_argument("--dataset_version", help="dataset version",
                        default="v1.1", required=False)
    parser.add_argument("--tag", help="PGE docker image tag (release, version, " +
                                      "or branch) to propagate",
                        default="master", required=False)
    parser.add_argument("--days_back", help="How far back to query for orbits relative to today",
                        default="1", required=False)
    return parser.parse_args()


class MyHTMLParser(HTMLParser):

    def __init__(self):
        HTMLParser.__init__(self)
        self.fileList = []
        self.pages = 0
        self.in_td = False
        self.in_a = False
        self.in_ul = False

    def handle_starttag(self, tag, attrs):
        if tag == 'td':
            self.in_td = True
        elif tag == 'a':
            self.in_a = True
        elif tag == 'ul':
            for k,v in attrs:
                if k == 'class' and v.startswith('pagination'):
                    self.in_ul = True
        elif tag == 'li' and self.in_ul:
            self.pages += 1

    def handle_data(self,data):
        if self.in_a:
            if OPER_RE.search(data):
                self.fileList.append(data.strip())

    def handle_endtag(self, tag):
        if tag == 'td':
            self.in_td = False
            self.in_a = False
        elif tag == 'a':
            self.in_a = False
        elif tag == 'ul' and self.in_ul:
            self.in_ul = False
        elif tag == 'html':
            if self.pages == 0:
                self.pages = 1
            else:
                # decrement page back and page forward list items
                self.pages -= 2


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException,
                      max_tries=8, max_value=32)
def session_get(session, url):
    return session.get(url, verify=False)


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException,
                      max_tries=8, max_value=32)
def check_orbit(es_url, es_index, id):
    """Query for orbits with specified input ID."""

    query = {"query":{"bool":{"must":[{"term":{"_id":id}}]}}}

    if es_url.endswith('/'):
        search_url = '%s%s/_search' % (es_url, es_index)
    else:
        search_url = '%s/%s/_search' % (es_url, es_index)
    #logger.info("search_url: %s" % search_url)
    r = requests.post(search_url, data=json.dumps(query))
    if r.status_code == 200:
        result = r.json()
        #logger.info(pformat(result))
        total = result['hits']['total']
        id = 'NONE' if total == 0 else result['hits']['hits'][0]['_id']
    else:
        logger.error("Failed to query %s:\n%s" % (es_url, r.text))
        logger.error("query: %s" % json.dumps(query, indent=2))
        logger.error("returned: %s" % r.text)
        if r.status_code == 404: total, id = 0, 'NONE'
        else: r.raise_for_status()
    return total, id


def crawl_orbits(dataset_version, days_back):
    """Crawl for orbit urls."""
    days_back = int(days_back)
    date_today = datetime.now()
    date_delta = timedelta(days = days_back)
    start_date = date_today - date_delta
    mission_type = "sat"
    margin=60.0
    datefmt="%Y%m%dT%H%M%S"
    found = False
    session = requests.Session()
    for spec in ORBITMAP:
            results = {}
        #for x in range(days_back):
            def is_orbit(href):
                return href and re.compile(r'^S1.*EOF$').search(href)
            #new_delta = timedelta(days = x)
            #new_date = start_date + new_delta
            oType = spec[0]
            url = QC_SERVER + spec[1]
            page_limit = spec[2]
            query = url #+ '/' + date
            print("The url is: {}".format(query))
            #logger.info(query)

            #logger.info('Querying for {0} orbits'.format(oType))
            r = session_get(session, query)
            if r.status_code != 200:
                #logger.info("No orbits found at this url: {}".format(query))
                continue
            #r.raise_for_status()
            #parser = MyHTMLParser()
            #parser.feed(r.text)
            parser = BeautifulSoup(r.text, 'html.parser')
            print("All found links")
            print(parser.find_all(href=is_orbit))
            print(len(parser.find_all(href=is_orbit)))
            for a in parser.find_all(href=is_orbit):
                print("All links")
                print(a['href'])
                m = re.search(r'^(?P<sat>S1[AB])_.*$', a['href'])
                sat = m.groupdict()['sat']
                if mission_type != sat:
                    orbit = os.path.basename(a['href'])
                    print("The orbit is: ")
                    print(orbit)
                    fields = orbit.split('_')
                    orbit_start_date_time = datetime.strptime(fields[6].replace('V',''), datefmt) + timedelta(seconds=margin)
                    orbit_stop_date_time = datetime.strptime(fields[7].replace('.EOF',''), datefmt) - timedelta(seconds=margin)
                    #results[os.path.splitext(os.path.basename(a['href']))[0]] = f"{url}{a['href']}"
                    found = True
                    results[orbit] = f"{url}{a['href']}"
                    print("Adding new key/val")
                    print(orbit)
                    print(f"{url}/{a['href']}")
                    #break
                   #if slc_start_dt >= orbit_start_date_time and slc_end_dt < orbit_stop_date_time:
                    #    results[os.path.splitext(os.path.basename(a['href']))[0]] = f"{url}{a['href']}"
                    #    found = True
                    #    break
                else: print("Not equal to sat")
            #if found: break
            #logger.info("Found {} pages".format(parser.pages))
            print("Length of results")
            print(len(results))
            for res in list(results):
                id = "%s-%s" % (os.path.splitext(res)[0], dataset_version)
                match = OPER_RE.search(res)
                if not match:
                    raise RuntimeError("Failed to parse orbit: {}".format(res))
                results[id] = DATA_SERVER + spec[1] + '/' + "{}".format(res)
                #results[id] = os.path.join("https://s1qc.asf.alaska.edu/", "/", "{}.EOF".format(res))
                print(results[id])
                yield id, results[id]

    # close session
    session.close()

    #logger.info(json.dumps(results, indent=2, sort_keys=True))
    #logger.info(len(results))


def submit_job(id, url, ds_es_url, tag, dataset_version):
    """Submit job for orbit dataset generation."""

    job_spec = "job-s1_orbit_ingest:%s" % tag
    job_name = "%s-%s" % (job_spec, id)
    job_name = job_name.lstrip('job-')

    #Setup input arguments here
    rule = {
        "rule_name": "s1_orbit_ingest",
        "queue": "factotum-job_worker-large",
        "priority": 0,
        "kwargs":'{}'
    }
    params = [
        {   
            "name": "version_opt",
            "from": "value",
            "value": "--dataset_version",
        },
        {   
            "name": "version",
            "from": "value",
            "value": dataset_version,
        },
        {
            "name": "orbit_url",
            "from": "value",
            "value": url,
        },
        {
            "name": "orbit_file",
            "from": "value",
            "value": os.path.basename(url),
        },
        {
            "name": "es_dataset_url",
            "from": "value",
            "value": ds_es_url,
        }
    ]
    print("submitting orbit ingest job for %s" % id)
    submit_mozart_job({}, rule,
        hysdsio={"id": "internal-temporary-wiring",
                 "params": params,
                 "job-specification": job_spec},
        job_name=job_name)


def crawl(ds_es_url, dataset_version, tag, days_back):
    """Crawl for orbits and submit job if they don't exist in ES."""

    for id, url in crawl_orbits(dataset_version, days_back):
        #logger.info("%s: %s" % (id, url))
        total, found_id = check_orbit(ds_es_url, "grq", id)
        if total > 0:
            logger.info("Found %s." % id)
            #prods_found.append(acq_id)
        else:
            logger.info("Missing %s. Submitting job." % id)
            #prods_missing.append(acq_id)
            submit_job(id, url, ds_es_url, tag, dataset_version)


if __name__ == '__main__':
    inps = cmdLineParse()
    try: status = crawl(inps.ds_es_url, inps.dataset_version, inps.tag, inps.days_back)
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)
