FROM osgeo/gdal:ubuntu-small-3.2.1

ENV AWS_REQUEST_PAYER=requester
ENV CFSI_BASE_DIR=/app/cfsi
ENV DATACUBE_DB_URL=postgresql://opendatacube:opendatacube@db:5432/opendatacube

COPY ./requirements.txt /tmp/requirements.txt
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates libgomp1 python3-pip python3-distutils python3-psycopg2  && \
    pip3 install --requirement /tmp/requirements.txt && \
    apt-get purge -y --auto-remove python3-pip && \
    mkdir -p /etc/pki/tls/certs && \
    cp /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -m app

WORKDIR /app
COPY cfsi cfsi

USER app
CMD cfsi/utils/wait-for-it.sh db:5432 -- cfsi/scripts/setup/setup_odc.sh \
    && python3 -m cfsi.scripts.index.s2_index \
    && python3 -m cfsi.scripts.masks.s2cloudless_masks
