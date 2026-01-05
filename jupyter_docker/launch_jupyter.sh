#!/bin/bash
cd $HOME
# eval "$(micromamba shell hook --shell bash)"
eval "$(mamba shell hook --shell bash)"
bash -c "mamba run -n jupyter -c jupyter lab \
    --ServerApp.custom_display_url "http://$HOST_NAME.ghdna.io:$HOST_PORT" \
    --ServerApp.default_url "http://$HOST_NAME.ghdna.io:$HOST_PORT" \
    --ip 0.0.0.0 \
    --port 8888 \
    --no-browser \
    --allow-root"
