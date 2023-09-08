FROM apache/airflow:2.7.0
RUN pip install --user --upgrade pip

RUN pip install pandas spotipy sqlalchemy