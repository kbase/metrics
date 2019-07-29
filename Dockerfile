FROM python:slim

RUN pip install pymongo

ENTRYPOINT ["/bin/bash"]
