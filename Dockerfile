FROM kbase/narrative:latest as narrative

FROM python:3.6-slim

# Build arguments passed into the docker command for image metadata
ARG BUILD_DATE
ARG COMMIT
ARG BRANCH


RUN apt-get update -y && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/* && \
    pip install mysql-connector-python-rf pymongo setuptools


RUN mkdir -p /kb/runtime

# Copy over all of the libraries in the Narrative runtime. This is overkill
# but kind of guarantees that anything that runs in a narrative python setup
# will run here as well
COPY --from=narrative /kb/runtime/lib /kb/runtime/lib
COPY --from=narrative /kb/dev_container/narrative/src/dist/biokbase-0.0.1-py3.6.egg /tmp/biokbase-0.0.1-py3.6.egg

# The *.egg directories in /kb/runtime/lib/python3.7/site-packages didn't come
# through the installer, so they aren't automatically added to sys.path. Put a
# modified version of the narrative containers easy-install.pth file into the
# the default search path so that the eggs are picked up by this container's
# python interpreter
ENV PYTHONPATH=/kb/runtime/lib/python3.6/site-packages/

RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir --upgrade six 


COPY bin /root/bin
RUN cd /root/bin && \
    wget https://github.com/kbase/dockerize/raw/master/dockerize-linux-amd64-v0.6.1.tar.gz && \
    tar xzf dockerize-linux-amd64-v0.6.1.tar.gz && \
    rm dockerize-linux-amd64-v0.6.1.tar.gz && \
    python /kb/runtime/lib/python3.6/site-packages/setuptools/command/easy_install.py --no-deps /tmp/biokbase-0.0.1-py3.6.egg

ENV PYTHONPATH=/kb/runtime/lib/python3.6/site-packages/:/kb/runtime/lib/python3.6/site-packages/biokbase-0.0.1-py3.6.egg

COPY source /root/source
WORKDIR /root/source

ENV PATH="/root/bin:/root/source:${PATH}"

LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.vcs-url="https://github.com/kbase/metrics.git" \
      org.label-schema.vcs-ref=$COMMIT \
      org.label-schema.schema-version="1.0.0-rc1" \
      us.kbase.vcs-branch=$BRANCH  \
      maintainer="Steve Chan sychan@lbl.gov"

ENTRYPOINT [ "/bin/bash" ]

