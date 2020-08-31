FROM gcr.io/joonkim-experiments/tfx_dev:latest

WORKDIR /tfx-async

RUN pip install jupyter

COPY jupyter_notebook_config.py /root/.jupyter/

EXPOSE 8888

CMD ["jupyter" , "notebook", "--ip=0.0.0.0", "--allow-root"]