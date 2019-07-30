FROM python:slime

MAINTAINER KBase Metrics Developer
# -----------------------------------------

RUN pip install pymongo
RUN pip install pandas
RUN pip install requests

ENTRYPOINT [ "/bin/bash.sh" ]

CMD [ ]