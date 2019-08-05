FROM kbase/narrative:py3-update as narrative

FROM python:3.7-slim

RUN pip install mysql-connector-python-rf pymongo

RUN mkdir -p /kb/runtime

# Copy over all of the libraries in the Narrative runtime. This is overkill
# but kind of guarantees that anything that runs in a narrative python setup
# will run here as well
COPY --from=narrative /kb/runtime/lib /kb/runtime/lib
COPY source /root/source
WORKDIR /root/source

ENV PYTHONPATH=/kb/runtime/lib/python3.7/site-packages/:/kb/runtime/lib/python3.6/site-packages/

ENTRYPOINT [ "/bin/bash" ]
