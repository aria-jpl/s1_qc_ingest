# s1_qc_ingest
Sentinel1 Quality Control File Ingest and Crawler

## Release
- current release: release-20170613

## Requirements
- HySDS
- Osaka

## crawl_orbits.py
- crawl ESA QC web service for precise (S1-AUX_POEORB) and restituted (S1-AUX_RESORB) orbits
- compare catalog of orbit files with those ingested into dataset ES (elasticsearch)
- submit jobs for ingest for orbit files not ingested into dataset ES
- Usage:
```
usage: crawl_orbits.py [-h] [--dataset_version DATASET_VERSION] [--tag TAG]
                       ds_es_url
crawl_orbits.py: error: too few arguments
```
- Example:
```
$ ./crawl_orbits.py http://100.64.134.71:9200 --tag dev
```

## crawl_cals.py
- crawl ESA QC web service for active calibration files (S1-AUX_CAL)
- compare catalog of calibration files with those ingested into dataset ES (elasticsearch)
- create HySDS dataset for calibration files not ingested into dataset ES
- create singleton HySDS dataset for list of active calibration files (S1-AUX_CAL_ACTIVE)
- Usage:
```
usage: crawl_cals.py [-h] [--dataset_version DATASET_VERSION] [--tag TAG]
                     ds_es_url

Crawl calibration files, create and ingest calibration datasets.

positional arguments:
  ds_es_url             ElasticSearch URL for datasets, e.g. http://aria-
                        products.jpl.nasa.gov:9200

optional arguments:
  -h, --help            show this help message and exit
  --dataset_version DATASET_VERSION
                        dataset version
  --tag TAG             PGE docker image tag (release, version, or branch) to
                        propagate
```
- Example:
```
$ ./crawl_cals.py http://100.64.134.71:9200 --tag dev
```

## create_orbit_ds.py
- create a HySDS dataset from a Sentinel1 precise or restituted orbit
- Usage:
```
usage: create_orbit_ds.py [-h] [--dataset_version DATASET_VERSION]
                          orbit_file ds_es_url

Create a HySDS dataset from a Sentinel1 precise or restituted orbit.

positional arguments:
  orbit_file            Sentinel1 precise/restituted orbit file
  ds_es_url             ElasticSearch URL for datasets, e.g. http://aria-
                        products.jpl.nasa.gov:9200

optional arguments:
  -h, --help            show this help message and exit
  --dataset_version DATASET_VERSION
                        dataset version
```
- Example:
```
$ wget --no-check-certificate https://qc.sentinel1.eo.esa.int/aux_poeorb/S1B_OPER_AUX_POEORB_OPOD_2017061
4T111434_V20170524T225942_20170526T005942.EOF
--2017-06-14 16:59:08--  https://qc.sentinel1.eo.esa.int/aux_poeorb/S1B_OPER_AUX_POEORB_OPOD_20170614T111434_V20170524T225942_20170526T005942.EOF
Resolving qc.sentinel1.eo.esa.int (qc.sentinel1.eo.esa.int)... 131.176.235.71
Connecting to qc.sentinel1.eo.esa.int (qc.sentinel1.eo.esa.int)|131.176.235.71|:443... connected.
WARNING: cannot verify qc.sentinel1.eo.esa.int's certificate, issued by '/C=GB/ST=Greater Manchester/L=Salford/O=COMODO CA Limited/CN=COMODO RSA Organizat
ion Validation Secure Server CA':
  Unable to locally verify the issuer's authority.
HTTP request sent, awaiting response... 200 OK
Length: 4410158 (4.2M) [application/octet-stream]
Saving to: 'S1B_OPER_AUX_POEORB_OPOD_20170614T111434_V20170524T225942_20170526T005942.EOF'

100%[================================================================================================================>] 4,410,158    998KB/s   in 4.3s   

2017-06-14 16:59:14 (998 KB/s) - 'S1B_OPER_AUX_POEORB_OPOD_20170614T111434_V20170524T225942_20170526T005942.EOF' saved [4410158/4410158]

$ ./create_orbit_ds.py S1B_OPER_AUX_POEORB_OPOD_20170614T111434_V20170524T225942_20170526T005942.EOF http://100.64.134.71:9200 --dataset_version v1.1
[2017-06-14 16:59:45,332: INFO create_orbit_ds.py:create_orbit_ds] create date:         2017-06-14 11:14:34
[2017-06-14 16:59:45,332: INFO create_orbit_ds.py:create_orbit_ds] validity start date: 2017-05-24 22:59:42
[2017-06-14 16:59:45,332: INFO create_orbit_ds.py:create_orbit_ds] validity end date:   2017-05-26 00:59:42
[2017-06-14 16:59:45,332: INFO create_orbit_ds.py:create_orbit_ds] sat: S1B
[2017-06-14 16:59:45,333: INFO create_orbit_ds.py:create_orbit_ds] sensor: SAR-C Sentinel1
[2017-06-14 16:59:45,333: INFO create_orbit_ds.py:create_orbit_ds] platform: Sentinel-1B
[2017-06-14 16:59:45,333: INFO create_orbit_ds.py:create_orbit_ds] typ: orbit
[2017-06-14 16:59:45,333: INFO create_orbit_ds.py:create_orbit_ds] orbit_type: POEORB
[2017-06-14 16:59:45,333: INFO create_orbit_ds.py:create_orbit_ds] dataset: S1-AUX_POEORB
[2017-06-14 16:59:45,333: INFO create_orbit_ds.py:create_orbit_ds] met: {
  "archive_filename": "S1B_OPER_AUX_POEORB_OPOD_20170614T111434_V20170524T225942_20170526T005942.EOF", 
  "creationTime": "2017-06-14T11:14:34", 
  "data_product_name": "S1B_OPER_AUX_POEORB_OPOD_20170614T111434_V20170524T225942_20170526T005942-v1.1", 
  "dataset": "S1-AUX_POEORB", 
  "platform": "Sentinel-1B", 
  "sensingStart": "2017-05-24T22:59:42", 
  "sensingStop": "2017-05-26T00:59:42", 
  "sensor": "SAR-C Sentinel1"
}
[2017-06-14 16:59:45,333: INFO create_orbit_ds.py:create_orbit_ds] dataset: {
  "endtime": "2017-05-26T00:59:42", 
  "label": "S1B_OPER_AUX_POEORB_OPOD_20170614T111434_V20170524T225942_20170526T005942-v1.1", 
  "starttime": "2017-05-24T22:59:42", 
  "version": "v1.1"
}
[2017-06-14 16:59:45,341: INFO create_orbit_ds.py:create_orbit_ds] total, found_id: 1 S1B_OPER_AUX_POEORB_OPOD_20170614T111434_V20170524T225942_20170526T005942-v1.1
[2017-06-14 16:59:45,341: INFO create_orbit_ds.py:create_orbit_ds] Found S1B_OPER_AUX_POEORB_OPOD_20170614T111434_V20170524T225942_20170526T005942-v1.1 in http://100.64.134.71:9200. Dedupping dataset.
```

## create_cal_ds.py
- create a HySDS dataset from a Sentinel1 calibration tar file
- Usage:
```
usage: create_cal_ds.py [-h] [--dataset_version DATASET_VERSION]
                        cal_tar_file ds_es_url

Create a HySDS dataset from a Sentinel1 calibration tar file.

positional arguments:
  cal_tar_file          Sentinel1 calibration tar file
  ds_es_url             ElasticSearch URL for datasets, e.g. http://aria-
                        products.jpl.nasa.gov:9200

optional arguments:
  -h, --help            show this help message and exit
  --dataset_version DATASET_VERSION
                        dataset version
```
- Example:
```
$ wget --no-check-certificate https://qc.sentinel1.eo.esa.int/aux_cal/S1A_AUX_CAL_V20160627T000000_G20170522T132042.SAFE
--2017-06-14 17:06:12--  https://qc.sentinel1.eo.esa.int/aux_cal/S1A_AUX_CAL_V20160627T000000_G20170522T132042.SAFE
Resolving qc.sentinel1.eo.esa.int (qc.sentinel1.eo.esa.int)... 131.176.235.71
Connecting to qc.sentinel1.eo.esa.int (qc.sentinel1.eo.esa.int)|131.176.235.71|:443... connected.
WARNING: cannot verify qc.sentinel1.eo.esa.int's certificate, issued by '/C=GB/ST=Greater Manchester/L=Salford/O=COMODO CA Limited/CN=COMODO RSA Organization Validation Secure Server CA':
  Unable to locally verify the issuer's authority.
HTTP request sent, awaiting response... 200 OK
Length: 493188 (482K) [application/x-gzip]
Saving to: 'S1A_AUX_CAL_V20160627T000000_G20170522T132042.SAFE'

100%[================================================================================================================>] 493,188      251KB/s   in 1.9s   

2017-06-14 17:06:15 (251 KB/s) - 'S1A_AUX_CAL_V20160627T000000_G20170522T132042.SAFE' saved [493188/493188]

$ ./create_cal_ds.py S1A_AUX_CAL_V20160627T000000_G20170522T132042.SAFE http://100.64.134.71:9200 --dataset_version v1.1
[2017-06-14 17:07:16,267: INFO/create_cal_ds] create date:         2017-05-22 13:20:42
[2017-06-14 17:07:16,267: INFO/create_cal_ds] validity start date: 2016-06-27 00:00:00
[2017-06-14 17:07:16,267: INFO/create_cal_ds] sat: S1A
[2017-06-14 17:07:16,268: INFO/create_cal_ds] sensor: SAR-C Sentinel1
[2017-06-14 17:07:16,268: INFO/create_cal_ds] platform: Sentinel-1A
[2017-06-14 17:07:16,268: INFO/create_cal_ds] typ: auxiliary
[2017-06-14 17:07:16,268: INFO/create_cal_ds] aux_type: CAL
[2017-06-14 17:07:16,268: INFO/create_cal_ds] dataset: S1-AUX_CAL
[2017-06-14 17:07:16,268: INFO/create_cal_ds] met: {
  "archive_filename": "S1A_AUX_CAL_V20160627T000000_G20170522T132042.SAFE", 
  "creationTime": "2017-05-22T13:20:42", 
  "data_product_name": "S1A_AUX_CAL_V20160627T000000_G20170522T132042-v1.1", 
  "dataset": "S1-AUX_CAL", 
  "platform": "Sentinel-1A", 
  "sensingStart": "2016-06-27T00:00:00", 
  "sensor": "SAR-C Sentinel1"
}
[2017-06-14 17:07:16,268: INFO/create_cal_ds] dataset: {
  "label": "S1A_AUX_CAL_V20160627T000000_G20170522T132042-v1.1", 
  "starttime": "2016-06-27T00:00:00", 
  "version": "v1.1"
}
[2017-06-14 17:07:16,275: INFO/create_cal_ds] total, found_id: 1 S1A_AUX_CAL_V20160627T000000_G20170522T132042-v1.1
[2017-06-14 17:07:16,275: INFO/create_cal_ds] Found S1A_AUX_CAL_V20160627T000000_G20170522T132042-v1.1 in http://100.64.134.71:9200. Dedupping dataset.
```

## cron_crawler.py
- cron script to submit Sentinel-1 crawler job
- Usage:
```
usage: cron_crawler.py [-h] [--dataset_version DATASET_VERSION] [--tag TAG]
                       --type {orbit,calibration}
                       ds_es_url

Cron script to submit Sentinel-1 crawler job.

positional arguments:
  ds_es_url             ElasticSearch URL for datasets, e.g. http://aria-
                        products.jpl.nasa.gov:9200

optional arguments:
  -h, --help            show this help message and exit
  --dataset_version DATASET_VERSION
                        dataset version
  --tag TAG             PGE docker image tag (release, version, or branch) to
                        propagate
  --type {orbit,calibration}
                        Sentinel-1 QC file type to crawl
```
- Example cron:
```
# crawl for orbits
0,30 * * * * $HOME/verdi/bin/python $HOME/verdi/ops/s1_qc_ingest/cron_crawler.py \
  --type orbit --dataset_version v1.1 --tag release-20170613 
  http://100.64.134.71:9200 > $HOME/verdi/log/s1_orbit_cron_crawler.log 2>&1

# crawl for active calibrations
15,45 * * * * $HOME/verdi/bin/python $HOME/verdi/ops/s1_qc_ingest/cron_crawler.py \
  --type calibration --dataset_version v1.1 --tag release-20170613 
  http://100.64.134.71:9200 > $HOME/verdi/log/s1_calibration_cron_crawler.log 2>&1
```
