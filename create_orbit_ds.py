#!/usr/bin/env python
"""
Create a HySDS dataset from a Sentinel1 precise or restituted orbit.
"""

from builtins import str
import os, sys, time, re, json, requests, shutil, logging, traceback, argparse
from datetime import datetime, timedelta

from crawl_orbits import check_orbit

# set logger
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'id'): record.id = '--'
        return True

logger = logging.getLogger('create_orbit_ds')
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


# regexes
ORBIT_RE = re.compile(r'^(?P<sat>S1.+?)_OPER_AUX_(?P<type>.*?)_OPOD_(?P<cr_yr>\d{4})(?P<cr_mo>\d{2})(?P<cr_dy>\d{2})T(?P<cr_hh>\d{2})(?P<cr_mm>\d{2})(?P<cr_ss>\d{2})_V(?P<vs_yr>\d{4})(?P<vs_mo>\d{2})(?P<vs_dy>\d{2})T(?P<vs_hh>\d{2})(?P<vs_mm>\d{2})(?P<vs_ss>\d{2})_(?P<ve_yr>\d{4})(?P<ve_mo>\d{2})(?P<ve_dy>\d{2})T(?P<ve_hh>\d{2})(?P<ve_mm>\d{2})(?P<ve_ss>\d{2})-(?P<version>.+)$')
PLATFORM_RE = re.compile(r'S1(.+?)_')


def get_dataset_json(met, version):
    """Generated HySDS dataset JSON from met JSON."""

    return {
        "version": version,
        "label": met['data_product_name'],
        "starttime": met['sensingStart'],
        "endtime": met['sensingStop'],
    }


def create_dataset(ds, met, orbit_file, root_ds_dir="."):
    """Create dataset. Return tuple of (dataset ID, dataset dir)."""

    # create dataset dir
    id = met['data_product_name']
    root_ds_dir = os.path.abspath(root_ds_dir)
    ds_dir = os.path.join(root_ds_dir, id)
    if not os.path.isdir(ds_dir): os.makedirs(ds_dir, 0o755)

    # dump dataset and met JSON
    ds_file = os.path.join(ds_dir, "%s.dataset.json" % id)
    met_file = os.path.join(ds_dir, "%s.met.json" % id)
    with open(ds_file, 'w') as f:
        json.dump(ds, f, indent=2, sort_keys=True)
    with open(met_file, 'w') as f:
        json.dump(met, f, indent=2, sort_keys=True)

    # copy orbit file
    shutil.copy(orbit_file, ds_dir)

    logger.info("created dataset %s" % ds_dir)
    return id, ds_dir


def create_orbit_ds(orbit_file, ds_es_url, version="v1.1"):
    """Create orbit dataset."""

    # extract info from orbit filename
    orbit_file_base = os.path.basename(orbit_file)
    id = "%s-%s" % (os.path.splitext(orbit_file_base)[0], version)
    match = ORBIT_RE.search(id)
    if not match:
        raise RuntimeError("Failed to extract info from orbit filename %s." % id)
    info = match.groupdict()

    # get dates
    create_dt = datetime(*[int(info[i]) for i in ['cr_yr', 'cr_mo', 'cr_dy', 'cr_hh', 'cr_mm', 'cr_ss']])
    valid_start = datetime(*[int(info[i]) for i in ['vs_yr', 'vs_mo', 'vs_dy', 'vs_hh', 'vs_mm', 'vs_ss']])
    valid_end = datetime(*[int(info[i]) for i in ['ve_yr', 've_mo', 've_dy', 've_hh', 've_mm', 've_ss']])
    logger.info("create date:         %s" % create_dt)
    logger.info("validity start date: %s" % valid_start)
    logger.info("validity end date:   %s" % valid_end)

    # get sat/platform and sensor
    sensor = "SAR-C Sentinel1"
    sat = info['sat']
    if sat == "S1A": platform = "Sentinel-1A"
    elif sat == "S1B": platform = "Sentinel-1B"
    else: raise RuntimeError("Failed to recognize sat: %s" % sat)
    logger.info("sat: %s" % sat)
    logger.info("sensor: %s" % sensor)
    logger.info("platform: %s" % platform)

    # get orbit product type
    typ = "orbit"
    orbit_type = info['type']
    if orbit_type == "POEORB": dataset = "S1-AUX_POEORB"
    elif orbit_type == "RESORB": dataset = "S1-AUX_RESORB"
    else: raise RuntimeError("Failed to recognize orbit type: %s" % orbit_type)
    logger.info("typ: %s" % typ)
    logger.info("orbit_type: %s" % orbit_type)
    logger.info("dataset: %s" % dataset)

    # get metadata json
    met = {
        "creationTime": create_dt.isoformat('T'),
        "data_product_name": id,
        "sensingStart": valid_start.isoformat('T'),
        "sensingStop": valid_end.isoformat('T'),
        "sensor": sensor,
        "platform": platform,
        "dataset": dataset,
        "archive_filename": orbit_file_base,
    }
    logger.info("met: %s" % json.dumps(met, indent=2, sort_keys=True))

    # get dataset json
    ds = get_dataset_json(met, version)
    logger.info("dataset: %s" % json.dumps(ds, indent=2, sort_keys=True))

    # dedup dataset
    total, found_id = check_orbit(ds_es_url, "grq", id)
    logger.info("total, found_id: %s %s" % (total, found_id))
    if total > 0:
        logger.info("Found %s in %s. Dedupping dataset." % (id, ds_es_url))
        return

    # create dataset
    id, ds_dir = create_dataset(ds, met, orbit_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("orbit_file", help="Sentinel1 precise/restituted orbit file")
    parser.add_argument("ds_es_url", help="ElasticSearch URL for datasets, e.g. " +
                        "http://aria-products.jpl.nasa.gov:9200")
    parser.add_argument("--dataset_version", help="dataset version",
                        default="v1.1", required=False)
    args = parser.parse_args()
    try: create_orbit_ds(args.orbit_file, args.ds_es_url, args.dataset_version)
    except Exception as e:
        with open('_alt_error.txt', 'a') as f:
            f.write("%s\n" % str(e))
        with open('_alt_traceback.txt', 'a') as f:
            f.write("%s\n" % traceback.format_exc())
        raise
