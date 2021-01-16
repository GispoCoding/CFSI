#!/bin/sh

echo export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}\; \
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}\; \
export CFSI_BASE_DIR="/home/ubuntu/cfsi" > /etc/profile.d/cfsi.sh

. /etc/profile.d/cfsi.sh

set -e
apt update && apt install -y \
    docker docker-compose git
git clone https://github.com/GispoCoding/CFSI "$CFSI_BASE_DIR"
cd "$CFSI_BASE_DIR"
docker-compose up -d
