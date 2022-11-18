FROM python:3.9 

WORKDIR /src 

COPY /src . 

COPY /ssl /ssl

RUN pip install -r requirements.txt 

ENV PYTHONPATH=/src

CMD ["python", "domain/pipeline/domain_pipeline.py"]
