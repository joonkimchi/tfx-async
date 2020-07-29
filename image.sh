#!/bin/bash

docker build --no-cache -t gcr.io/joonkim-experiments/tfx_dev . ; docker push gcr.io/joonkim-experiments/tfx_dev