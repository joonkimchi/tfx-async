#!/bin/bash

docker build --no-cache -t gcr.io/joonkim-experiments/tfx_dev:latest . ; docker push gcr.io/joonkim-experiments/tfx_dev:latest