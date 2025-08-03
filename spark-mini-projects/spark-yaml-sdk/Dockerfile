FROM jupyter/pyspark-notebook:spark-3.4.1

USER root

RUN pip install boto3

# Add Hadoop AWS dependencies
COPY jars/* /usr/local/spark/jars/

USER ${NB_UID}