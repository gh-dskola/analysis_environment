FROM condaforge/miniforge3

WORKDIR /build

COPY environment-jupyter.yml .
RUN mamba create -f environment-jupyter.yml
COPY environment-bnfo-base.yml .
RUN mamba create -f environment-bnfo-base.yml

WORKDIR /
COPY launch_jupyter.sh .

ENTRYPOINT ["bash", "/launch_jupyter.sh"]

