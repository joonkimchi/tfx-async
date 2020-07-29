#!/bin/bash

bazel run tfx:build_pip_package ; cd dist/ ; pip install --no-cache-dir --upgrade tfx-0.23.0.dev0-py3-none-any.whl ; python ~/TFX/project/tfx/tfx/examples/chicago_taxi_pipeline/taxi_pipeline_kubernetes.py