#!/bin/bash
# echo Inside launch script
# echo $HOST_NAME
# echo $HOST_PORT
cd /workspace
mamba run -n jupyter -c jupyter lab \
    --ServerApp.custom_display_url "http://$HOST_NAME.ghdna.io:$HOST_PORT" \
    --ServerApp.default_url "http://$HOST_NAME.ghdna.io:$HOST_PORT" \
    --ip 0.0.0.0 \
    --port 8888 \
    --no-browser \
    --allow-root
