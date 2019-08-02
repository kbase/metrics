FROM kbase/narrative:latest as narrative

FROM mysql

#FROM mysql:connector

FROM python:2.7-slim

#RUN apt-get update -y \
#    && apt-get install -y python-mysql.connector
#    && apt-get install -y mysql-connector-python-rf 

RUN pip install mysql-connector-python-rf



#FROM python:2.7

#FROM mysql

RUN mkdir -p /kb/runtime

# Copy over all of the libraries in the Narrative runtime. This is overkill
# but kind of guarantees that anything that runs in a narrative python setup
# will run here as well
COPY --from=narrative /kb/runtime/lib /kb/runtime/lib
COPY source /root/source
WORKDIR /root/source

ENV PYTHONPATH=/kb/runtime/lib/python2.7/site-packages/

ENTRYPOINT [ "/bin/bash" ]
