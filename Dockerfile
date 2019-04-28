# Mongo-Connector container
#
# https://github.com/mongodb-labs/mongo-connector

FROM python:3.5-jessie

ENV DEBIAN_FRONTEND noninteractive
RUN pip install --upgrade pip

# Install mongo-connector
WORKDIR "/srv/riffyn/mongo-connector"

ADD mongo-connector.tar ./
RUN pip install /srv/riffyn/mongo-connector/dist/mongo_connector-*.whl
# RUN pip install mongo-connector==2.5.1
# RUN pip install 'elastic2-doc-manager[elastic2]'
ADD config ./config
ADD scripts/elasticsearch-configure.sh ./
ADD scripts/start-mongo-connector.sh ./
ENTRYPOINT [ "bash", "start-mongo-connector.sh"]
