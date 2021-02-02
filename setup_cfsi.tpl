#!/bin/sh

# This script sets up CFSI on a new machine

# Set environment variables in /etc/profile.d/cfsi.sh
echo export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}"\; \
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}"\; \
export CFSI_BASE_HOST="${CFSI_BASE_HOST}"\; \
export CFSI_OUTPUT_HOST="${CFSI_BASE_HOST}" \
> /etc/profile.d/cfsi.sh

# Source environment variables
. /etc/profile.d/cfsi.sh

set -e
# Install host machine dependencies
apt update && apt install -y \
    docker docker-compose git
sudo usermod -aG docker "$CFSI_USER_HOST"

# Clone source code
git clone --branch "$CFSI_BRANCH" "$CFSI_REPOSITORY" "$CFSI_BASE_HOST"
chown -R "${CFSI_USER_HOST}:${CFSI_USER_HOST}" "$CFSI_BASE_HOST"
cd "$CFSI_BASE_HOST"

# Create output directory
# shellcheck disable=SC2174
mkdir -m 777 -p "$CFSI_OUTPUT_HOST"

# Start CFSI
docker-compose up -d
