#!/bin/bash
eval "$(mamba shell hook --shell bash)"
mamba activate $1
mamba install -y ipykernel
mamba run -n $1 python -m ipykernel install --user --name $1
