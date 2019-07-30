FROM kbase/narrative:latest as narrative

FROM python:2.7-slim

RUN mkdir -p /kb/runtime

# Copy over all of the libraries in the Narrative runtime. This is overkill
# but kind of guarantees that anything that runs in a narrative python setup
# will run here as well
COPY --from=narrative /kb/runtime/lib /kb/runtime/lib

ENV PYTHONPATH=/kb/runtime/lib/python2.7/site-packages/

ENTRYPOINT ["/bin/bash"]
