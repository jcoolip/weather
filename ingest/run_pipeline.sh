#!/bin/bash

# Config
LOG_DIR="/home/justin/dev/job_market/ingest/logs"
LOG_FILE="$LOG_DIR/job_market_$(date +'%Y-%m-%d_%H-%M-%S').log"
EMAIL="job_market@jcullop.com"

# Activate virtualenv
source /home/justin/dev/job_market/.venv/bin/activate

cd /home/justin/dev/job_market/ingest

# Run pipeline
echo "Starting pipeline at $(date +'%m-%d-%Y_%H-%M-%S')" | tee -a "$LOG_FILE"
ROWS_ADDED=$(/home/justin/dev/job_market/.venv/bin/python3 -m pipeline 2>&1 | tee -a "$LOG_FILE")

EXIT_CODE=${PIPESTATUS[0]}  # capture pipeline exit code

# Compose message
SUBJECT=""
BODY=""

SUBJECT="Job Market Pipeline Results"
BODY="$(date) results:\n$EXIT_CODE\n\n$ROWS_ADDED"
echo -e "$BODY" | mail -s "$SUBJECT" "$EMAIL"
