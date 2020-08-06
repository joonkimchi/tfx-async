#!/bin/bash

kubectl apply -f ~/TFX/project/tfx/tfx/orchestration/kubernetes/mysql/mysql-pv.yaml ; kubectl apply -f ~/TFX/project/tfx/tfx/orchestration/kubernetes/mysql/mysql-deployment.yaml