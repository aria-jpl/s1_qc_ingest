#!/usr/bin/env python
"""
Cron script to submit Sentinel-1 crawler job.
"""

from __future__ import print_function

import os, sys, json, requests, argparse
from datetime import datetime, timedelta
import argparse

from hysds_commons.job_utils import submit_mozart_job
from hysds.celery import app


if __name__ == "__main__":
    '''
    Main program that is run by cron to submit a Sentinel-1 crawler job
    '''

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ds_es_url", help="ElasticSearch URL for datasets, e.g. " +
                        "http://aria-products.jpl.nasa.gov:9200")
    parser.add_argument("--dataset_version", help="dataset version",
                        default="v1.1", required=False)
    parser.add_argument("--tag", help="PGE docker image tag (release, version, " +
                                      "or branch) to propagate",
                        default="master", required=False)
    parser.add_argument("--type", help="Sentinel-1 QC file type to crawl",
                        choices=['orbit', 'calibration'], required=True)
    parser.add_argument("--days_back", help="How far back to query for orbits relative to today",
                        default="1", required=False)
    args = parser.parse_args()

    ds_es_url = args.ds_es_url
    dataset_version = args.dataset_version
    tag = args.tag
    qc_type = args.type
    days_back = args.days_back
    job_spec = "job-s1_%s_crawler:%s" % (qc_type, tag)

    job_name = job_spec
    job_name = job_name.lstrip('job-')

    #Setup input arguments here
    rule = {
        "rule_name": job_name,
        "queue": "factotum-job_worker-small",
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
            "name": "tag_opt",
            "from": "value",
            "value": "--tag",
        },
        {   
            "name": "tag",
            "from": "value",
            "value": tag,
        },
        {   
            "name": "days_back_opt",
            "from": "value",
            "value": "--days_back",
        },
        {   
            "name": "days_back",
            "from": "value",
            "value": days_back,
        },
        {
            "name": "es_dataset_url",
            "from": "value",
            "value": ds_es_url,
        }
    ]
    print("submitting %s crawler job" % qc_type)
    submit_mozart_job({}, rule,
        hysdsio={"id": "internal-temporary-wiring",
                 "params": params,
                 "job-specification": job_spec},
        job_name=job_name)
