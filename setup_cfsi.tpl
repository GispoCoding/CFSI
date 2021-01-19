#!/bin/sh

echo export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}\; \
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}\; \
export CFSI_BASE_DIR="/home/ubuntu/cfsi"\; \
export CFSI_OUTPUT_DIR="/home/ubuntu/cfsi_output" > /etc/profile.d/cfsi.sh

. /etc/profile.d/cfsi.sh

set -e
apt update && apt install -y \
    docker docker-compose git
sudo usermod -aG docker ubuntu
git clone https://github.com/GispoCoding/CFSI "$CFSI_BASE_DIR"
chown -R ubuntu:ubuntu "$CFSI_BASE_DIR"
cd "$CFSI_BASE_DIR"
# shellcheck disable=SC2174
mkdir -m 777 -p "$CFSI_OUTPUT_DIR"
docker-compose up -d
