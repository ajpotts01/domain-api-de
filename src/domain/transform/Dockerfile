FROM python:3.9 

WORKDIR /transform 

COPY . /transform

COPY ./profiles.yml /root/.dbt/profiles.yml

RUN pip install -r requirements.txt 

CMD ["dbt", "run"]