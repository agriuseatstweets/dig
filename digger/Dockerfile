FROM nandanrao/pyspark-notebook

RUN pip install --user \
        confluent-kafka \
        gcsfs \
        clize \
        environs

COPY . .

CMD ["python", "dig.py"]
