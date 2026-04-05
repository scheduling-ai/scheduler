#!/usr/bin/env bash
# Polls git for new commits, rebuilds the scheduler services on changes.
# Designed to run from cron every minute.
#
# SECURITY: Do NOT run this script directly from the git checkout.
# A bad commit could modify this file and execute arbitrary commands outside of container sandbox, which can be dangerous.
# Instead, copy it to a location outside the repo and run from there.
#
# Setup:
#   1. Clone the repo to the deploy directory (one-time):
#      git clone https://github.com/scheduling-ai/scheduler.git /tmp/scheduler-deploy
#      cp .env /tmp/scheduler-deploy/.env
#
#   2. Copy this script outside the checkout (one-time):
#      cp /tmp/scheduler-deploy/scripts/deploy-loop.sh /tmp/deploy-loop.sh
#      chmod +x /tmp/deploy-loop.sh
#
#   3. Add to crontab (crontab -e):
#      * * * * * /tmp/deploy-loop.sh >> /tmp/deploy.log 2>&1
#
#   To update the deploy loop itself, manually copy the new version:
#      cp /tmp/scheduler-deploy/scripts/deploy-loop.sh /tmp/deploy-loop.sh

set -euo pipefail

DEPLOY_DIR="/tmp/scheduler-deploy"
LOGFILE="/tmp/deploy.log"
MAX_LOG_LINES=10000
cd "$DEPLOY_DIR"

DC="docker compose -f docker-compose.yml -f deploy/docker-compose.prod.yml"

# Rotate log: keep last N lines
if [ -f "$LOGFILE" ] && [ "$(wc -l < "$LOGFILE")" -gt "$MAX_LOG_LINES" ]; then
    tail -n "$MAX_LOG_LINES" "$LOGFILE" > "$LOGFILE.tmp" && mv "$LOGFILE.tmp" "$LOGFILE"
fi

export GIT_SHA=$(git rev-parse --short HEAD)

git fetch origin main --quiet
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

SKIP="pomerium"
SERVICES=$($DC config --services | grep -v -F "$SKIP")

if [ "$LOCAL" != "$REMOTE" ]; then
    echo "[$(date -Is)] New commits detected, pulling & rebuilding..."
    $DC stop $SERVICES
    git checkout -- .
    git pull --ff-only origin main
    GIT_SHA=$(git rev-parse --short HEAD)
    $DC up -d --build --remove-orphans $SERVICES
fi

# Ensure app services are running (e.g. after reboot)
$DC up -d --remove-orphans $SERVICES
