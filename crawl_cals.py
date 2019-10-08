#!/usr/bin/env python
"""
Crawl calibration files, create and ingest calibration datasets.
"""

from future import standard_library
standard_library.install_aliases()
from builtins import str
import os, sys, re, json, logging, traceback, requests, argparse, backoff, shutil
from datetime import datetime
from requests.packages.urllib3.exceptions import (InsecureRequestWarning,
                                                  InsecurePlatformWarning)
try: from html.parser import HTMLParser
except: from html.parser import HTMLParser

from osaka.main import get, rmall

from create_cal_ds import check_cal, create_cal_ds


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

logger = logging.getLogger('crawl_cals')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


QC_SERVER = 'https://qc.sentinel1.eo.esa.int/'
DATA_SERVER = 'https://qc.sentinel1.eo.esa.int/'
#DATA_SERVER = 'http://aux.sentinel1.eo.esa.int/'

CAL_RE = re.compile(r'(?P<sat>S1\w)_(?P<type>AUX_CAL)_V(?P<dt>\d{8}T\d{6})')


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
        elif tag == 'a' and self.in_td:
            self.in_a = True
        elif tag == 'ul':
            for k,v in attrs:
                if k == 'class' and v.startswith('pagination'):
                    self.in_ul = True
        elif tag == 'li' and self.in_ul:
            self.pages += 1

    def handle_data(self,data):
        if self.in_td and self.in_a:
            if CAL_RE.search(data):
                self.fileList.append(data.strip())

    def handle_endtag(self, tag):
        if tag == 'td':
            self.in_td = False
            self.in_a = False
        elif tag == 'a' and self.in_td:
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


def crawl_cals(dataset_version):
    """Crawl for calibration urls."""

    results = {}
    session = requests.Session()
    oType = 'calibration'
    url = QC_SERVER + 'aux_cal'
    page_limit = 100
    query = url + '/?adf__active=True'

    logger.info(query)

    logger.info('Querying for {0} calibration files'.format(oType))
    r = session_get(session, query)
    r.raise_for_status()
    parser = MyHTMLParser()
    parser.feed(r.text)
    logger.info("Found {} pages".format(parser.pages))

    for res in parser.fileList:
        id = "%s-%s" % (os.path.splitext(res)[0], dataset_version)
        results[id] = os.path.join(url, res)
        match = CAL_RE.search(res)
        if not match:
            raise RuntimeError("Failed to parse cal: {}".format(res))
        results[id] = os.path.join(DATA_SERVER, "product", "/".join(match.groups()), "{}.SAFE.TGZ".format(res))
        yield id, results[id]

    # page through and get more results
    page = 2
    reached_end = False
    while True:
        page_query = "{}?page={}".format(query, page)
        logger.info(page_query)
        r = session_get(session, page_query)
        r.raise_for_status()
        page_parser = MyHTMLParser()
        page_parser.feed(r.text)
        for res in page_parser.fileList:
            id = "%s-%s" % (os.path.splitext(res)[0], dataset_version)
            if id in results or page >= page_limit:
                reached_end = True
                break
            else:
                match = CAL_RE.search(res)
                if not match:
                    raise RuntimeError("Failed to parse cal: {}".format(res))
                results[id] = os.path.join(DATA_SERVER, "product", "/".join(match.groups()), "{}.SAFE.TGZ".format(res))
                yield id, results[id]
        if reached_end: break
        else: page += 1

    # close session
    session.close()

    #logger.info(json.dumps(results, indent=2, sort_keys=True))
    logger.info(len(results))


def create_active_cal_ds(active_ids, dataset_version, root_ds_dir="."):
    """Create active calibration files dataset."""

    # set id
    id = "S1_AUX_CAL_ACTIVE"

    # get metadata json
    now = datetime.utcnow()
    met = {
        "creationTime": now.isoformat('T'),
        "data_product_name": id,
        "sensor": "SAR-C Sentinel1",
        "dataset": "S1-AUX_CAL_ACTIVE",
        "active_ids": active_ids,
    }
    logger.info("met: %s" % json.dumps(met, indent=2, sort_keys=True))

    # get dataset json
    ds = {
        "version": dataset_version,
        "label": met['data_product_name'],
        "starttime": met['creationTime'],
    }

    # create dataset dir
    #ds_id = "%s-%04d%02d%02dT%02d%02d%02d" % (id, now.year, now.month,
    #                                          now.day, now.hour, now.minute,
    #                                          now.second)
    ds_id = id
    root_ds_dir = os.path.abspath(root_ds_dir)
    ds_dir = os.path.join(root_ds_dir, ds_id)
    if not os.path.isdir(ds_dir): os.makedirs(ds_dir, 0o755)

    # dump dataset and met JSON
    ds_file = os.path.join(ds_dir, "%s.dataset.json" % ds_id)
    met_file = os.path.join(ds_dir, "%s.met.json" % ds_id)
    with open(ds_file, 'w') as f:
        json.dump(ds, f, indent=2, sort_keys=True)
    with open(met_file, 'w') as f:
        json.dump(met, f, indent=2, sort_keys=True)

    logger.info("created dataset %s" % ds_dir)
    return id, ds_dir


@backoff.on_exception(backoff.expo, requests.exceptions.RequestException,
                      max_tries=8, max_value=32)
def purge_active_cal_ds(es_url, dataset_version):
    """Purge active cal dataset."""

    id = "S1_AUX_CAL_ACTIVE"
    query = {
        "query":{
            "bool":{
                "must": [
                    { "term": { "_id": id } },
                ]
            }
        },
        "fields": [ "urls" ],
    }
    es_index = "grq_%s_s1-aux_cal_active" % dataset_version
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
        if total > 0:
            hit = result['hits']['hits'][0]['fields']
            for url in hit['urls']:
                if not (url.startswith('http') or url.startswith('ftp')):
                    rmall(url)
    else:
        logger.error("Failed to query %s:\n%s" % (es_url, r.text))
        logger.error("query: %s" % json.dumps(query, indent=2))
        logger.error("returned: %s" % r.text)
        if r.status_code != 404: r.raise_for_status()


def crawl(ds_es_url, dataset_version, tag):
    """Crawl for calibration files and create datasets if they don't exist in ES."""

    active_ids = []
    for id, url in crawl_cals(dataset_version):
        #logger.info("%s: %s" % (id, url))
        active_ids.append(id)
        total, found_id = check_cal(ds_es_url, "grq", id)
        if total > 0:
            logger.info("Found %s." % id)
        else:
            logger.info("Missing %s. Creating dataset." % id)
            cal_tar_file = os.path.basename(url)
            get(url, cal_tar_file)
            safe_tar_file = cal_tar_file.replace('.TGZ', '')
            shutil.move(cal_tar_file, safe_tar_file)
            create_cal_ds(safe_tar_file, ds_es_url, dataset_version)
    purge_active_cal_ds(ds_es_url, dataset_version)
    create_active_cal_ds(active_ids, dataset_version)


if __name__ == '__main__':
    inps = cmdLineParse()
    try: status = crawl(inps.ds_es_url, inps.dataset_version, inps.tag)
    except Exception as e:
        with open('_alt_error.txt', 'w') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'w') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
    sys.exit(status)
