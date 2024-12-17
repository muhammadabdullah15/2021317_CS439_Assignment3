FROM bitnami/spark:latest
RUN pip install --upgrade pip && pip install --default-timeout=3000 matplotlib pandas
WORKDIR /opt/spark-data
CMD ["/bin/bash"]
