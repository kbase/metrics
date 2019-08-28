FROM kbase/narrative:py3-update as narrative

FROM python:3.7-slim

# Build arguments passed into the docker command for image metadata
ARG BUILD_DATE
ARG COMMIT
ARG BRANCH

RUN pip install mysql-connector-python-rf pymongo

RUN mkdir -p /kb/runtime

# Copy over all of the libraries in the Narrative runtime. This is overkill
# but kind of guarantees that anything that runs in a narrative python setup
# will run here as well
COPY --from=narrative /kb/runtime/lib/python3.7/site-packages/ /kb/runtime/lib/python3.7/site-packages/
COPY --from=narrative /kb/runtime/lib/python3.6/site-packages/ /kb/runtime/lib/python3.6/site-packages/
COPY bin /root/bin
COPY source /root/source
WORKDIR /root/source

# The *.egg directories in /kb/runtime/lib/python3.7/site-packages didn't come
# through the installer, so they aren't automatically added to sys.path. Put a
# modified version of the narrative containers easy-install.pth file into the
# the default search path so that the eggs are picked up by this container's
# python interpreter
RUN sed 's/^\./\/kb\/runtime\/lib\/python3.7\/site-packages/' /kb/runtime/lib/python3.7/site-packages/easy-install.pth >/usr/local/lib/python3.7/site-packages/kbase.pth
ENV PYTHONPATH=/kb/runtime/lib/python3.7/site-packages/:/kb/runtime/lib/python3.6/site-packages/
ENV PATH="/root/bin:/root/source:${PATH}"

LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.vcs-url="https://github.com/kbase/metrics.git" \
      org.label-schema.vcs-ref=$COMMIT \
      org.label-schema.schema-version="1.0.0-rc1" \
      us.kbase.vcs-branch=$BRANCH  \
      maintainer="Steve Chan sychan@lbl.gov"

ENTRYPOINT [ "/bin/bash" ]

