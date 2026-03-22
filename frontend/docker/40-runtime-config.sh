#!/bin/sh
set -eu

API_BASE_URL="${UAI_API_BASE_URL:-/api}"
API_BASE_URL_ESCAPED=$(printf '%s' "$API_BASE_URL" | sed 's/\\/\\\\/g; s/"/\\"/g')

cat > /usr/share/nginx/html/runtime-config.js <<EOF
window.__UAI_API_BASE_URL__ = "${API_BASE_URL_ESCAPED}";
EOF
