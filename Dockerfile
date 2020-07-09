FROM python:3.7.3

ADD ./ /

RUN pip install --no-cache-dir --upgrade tfx-0.23.0.dev0-py3-none-any.whl

CMD python tfx/examples/chicago_taxi_pipeline/taxi_pipeline_beam.py