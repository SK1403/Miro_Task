FROM python:3.9.7-alpine3.14
LABEL maintainer="Saddam Khan"

RUN pip install pyspark
RUN pip install pytest
RUN pip install airflow

COPY Miro/* /app/Miro
WORKDIR /app
CMD ["/bin/bash"]
RUN chmod a+x miro_etl_entrypoint.py

ENTRYPOINT ["./miro_etl_entrypoint.py"]