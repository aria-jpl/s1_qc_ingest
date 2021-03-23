FROM hysds/pge-base

MAINTAINER aria-dev "aria-dev@jpl.nasa.gov"
LABEL description="Sentinel1 orbit crawler/ingester PGE"

USER ops

# copy packages
COPY . /home/ops/verdi/ops/s1_qc_ingest
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/s1_qc_ingest \
 && $HOME/verdi/bin/pip install git+https://github.com/hysds/hysds-sdswatch.git@master 

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
