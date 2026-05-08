#!/bin/bash

COMMANDS=()
SCHEDULE_TIME=""
S3_FILE_LINK=""

# Configuration
S3_OUTPUT_BUCKET="${S3_OUTPUT_BUCKET:-""}"
SQS_QUEUE_URL="${SQS_QUEUE_URL:-""}"

# Retry configuration
MAX_RETRIES=${MAX_RETRIES:-5}
BASE_DELAY=${BASE_DELAY:-2}
MAX_DELAY=${MAX_DELAY:-300}

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to generate random jitter
get_jitter() {
    local max_jitter=$1
    echo $((RANDOM % max_jitter + 1))
}

# Retry function with exponential backoff and jitter
retry_with_backoff() {
    local operation_name="$1"
    local max_retries="$2"
    shift 2
    local command=("$@")
    
    local attempt=1
    local delay=$BASE_DELAY
    
    while [ $attempt -le $max_retries ]; do
        log "[$operation_name] Attempt $attempt/$max_retries: ${command[*]}"
        
        "${command[@]}"
        local exit_code=$?

        if [ $exit_code -eq 0 ]; then
            log "[$operation_name] Success on attempt $attempt"
            return 0
        fi
        
        if [ $attempt -eq $max_retries ]; then
            log "[$operation_name] Failed after $max_retries attempts"
            return $exit_code
        fi
        
        # Calculate delay with exponential backoff and jitter
        local jitter=$(get_jitter $delay)
        local total_delay=$((delay + jitter))
        
        # Cap the delay at MAX_DELAY
        if [ $total_delay -gt $MAX_DELAY ]; then
            total_delay=$MAX_DELAY
        fi
        
        log "[$operation_name] Attempt $attempt failed (exit: $exit_code), retrying in ${total_delay}s..."
        sleep $total_delay
        
        # Exponential backoff
        delay=$((delay * 2))
        attempt=$((attempt + 1))
    done
    
    return 1
}

# Store all the variables from the cmd line
while [[ $# -gt 0 ]]; do
  case $1 in
      -c|--command)
          COMMANDS+=("$2")
          shift 2
          ;;
      -s|--scheduletime)
          SCHEDULE_TIME="$2"
          shift 2
          ;;
      -f|--filelink)
          S3_FILE_LINK="$2"
          shift 2
          ;;
      *)
          log "Unknown parameter: $1"
          shift
          ;;
  esac
done

if [[ -z "$SCHEDULE_TIME" || ! "$SCHEDULE_TIME" =~ ^[0-9]+$ ]]; then
    log "Error: Schedule time is not in the required format (-s|--scheduletime)"
    COMPLETION_STATUS="CONFIG_FAILURE"
    exit 1
fi

if [[ -z "$S3_FILE_LINK" || ! "$S3_FILE_LINK" =~ ^s3://([^/]+)/(.+)$ ]]; then
    log "Error: File link is not in the required format (-f|--filelink)"
    COMPLETION_STATUS="CONFIG_FAILURE"
    exit 1
fi

if [ ${#COMMANDS[@]} -eq 0 ]; then
    log "Error: Commands are not in the required format (-c|--command)"
    COMPLETION_STATUS="CONFIG_FAILURE"
    exit 1
fi

if [[ "$COMPLETION_STATUS" =~ "CONFIG_FAILURE" ]]; then
    FAILURE_MSG="{\"status\":\"config_failure\",\"file\":\"\",\"execution_id\":\"$EXECUTION_ID\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
    retry_with_backoff "SQS_SEND" "$MAX_RETRIES" aws sqs send-message --queue-url "$SQS_QUEUE_URL" --message-body "$FAILURE_MSG"
fi

# Download from S3
log "Downloading from S3: $S3_FILE_LINK"
if [[ "$S3_FILE_LINK" =~ /$ ]]; then
    if ! retry_with_backoff "S3_DOWNLOAD" $MAX_RETRIES aws s3 cp "$S3_FILE_LINK" /app/ --recursive; then
        log "Error: Failed to download directory from S3 after $MAX_RETRIES attempts"
        FAILURE_MSG="{\"status\":\"download_failed\",\"file\":\"\",\"execution_id\":\"$EXECUTION_ID\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
        retry_with_backoff "SQS_SEND" "$MAX_RETRIES" aws sqs send-message --queue-url "$SQS_QUEUE_URL" --message-body "$FAILURE_MSG"
        exit 1
    fi
else
    if ! retry_with_backoff "S3_DOWNLOAD" $MAX_RETRIES aws s3 cp "$S3_FILE_LINK" /app/; then
        log "Error: Failed to download file from S3 after $MAX_RETRIES attempts"
        FAILURE_MSG="{\"status\":\"download_failed\",\"file\":\"\",\"execution_id\":\"$EXECUTION_ID\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}"
        retry_with_backoff "SQS_SEND" "$MAX_RETRIES" aws sqs send-message --queue-url "$SQS_QUEUE_URL" --message-body "$FAILURE_MSG"
        exit 1
    fi
fi

echo "Directory structure after download:" && ls -ltr /app/

# Might add prescript commands before .start_timestamp creation

# Create timestamp marker after download
touch /app/.start_timestamp
log "Created timestamp marker for output detection"

# Wait until scheduled time
current_time=$(date +%s)
if [ $SCHEDULE_TIME -gt $current_time ]; then
    sleep_duration=$(($SCHEDULE_TIME - $current_time))
    log "Sleeping for ${sleep_duration}s until scheduled time"
    sleep $sleep_duration
fi

log "Starting command execution at $(date)"

COMPLETION_STATUS="SUCCEEDED"
# Execute commands
for i in "${!COMMANDS[@]}"; do
    cmd="${COMMANDS[$i]}"
    log "[$((i+1))/${#COMMANDS[@]}] Executing: $cmd"
    
    bash -c "$cmd"
    exit_code=$?

    if [ $exit_code -eq 0 ]; then
        log "Command $((i+1)) completed successfully"
    else
        log "Command $((i+1)) failed with exit code: $exit_code"
        if [[ "$COMPLETION_STATUS" == "SUCCEEDED" ]]; then
            if [ $i -gt 0 ]; then
                COMPLETION_STATUS="PARTIAL_SUCCESS"
            else
                COMPLETION_STATUS="FAILED"
            fi
        fi
    fi
done

# Upload outputs
log "Uploading output files to S3..."
find /app -type f -newer /app/.start_timestamp 2>/dev/null | while read -r file; do
    if [ -f "$file" ]; then
        RELATIVE_PATH="${file#/app/}"
        S3_Location="outputs/${EXECUTION_ID}/$RELATIVE_PATH"
        S3_UPLOAD="${S3_OUTPUT_BUCKET}/${S3_Location}"

        log "Uploading $RELATIVE_PATH to $S3_UPLOAD"

        if retry_with_backoff "S3_UPLOAD" "$MAX_RETRIES" aws s3 cp "$file" "$S3_UPLOAD"; then
            log "Successfully uploaded $RELATIVE_PATH"
        else
            log "Error: Failed to upload $RELATIVE_PATH after $MAX_RETRIES attempts"
        fi
    fi
done

log "Sending completion message to SQS..."
COMPLETION_MSG="{
    \"status\": \"$COMPLETION_STATUS\",
    \"output_bucket\": \"$S3_OUTPUT_BUCKET\",
    \"execution_id\": \"$EXECUTION_ID\",
    \"execution_time\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
}"

if retry_with_backoff "SQS_SEND" $MAX_RETRIES aws sqs send-message --queue-url "$SQS_QUEUE_URL" --message-body "$COMPLETION_MSG"; then
    log "Completion message sent to SQS"
else
    log "Failed to send completion message to SQS after $MAX_RETRIES attempts"
fi