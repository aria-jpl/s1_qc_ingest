FROM hysds/pge-base:latest

MAINTAINER aria-dev "aria-dev@jpl.nasa.gov"
LABEL description="Sentinel1 orbit crawler/ingester PGE"

USER ops

# copy packages
COPY . /home/ops/verdi/ops/s1_qc_ingest
RUN set -ex \
 && source /home/ops/verdi/bin/activate \
 && sudo chown -R ops:ops /home/ops/verdi/ops/s1_qc_ingest

WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
