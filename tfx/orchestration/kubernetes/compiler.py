import os
import yaml

from tfx.orchestration import pipeline as tfx_pipeline
from tfx.orchestration import data_types
from tfx.orchestration.kubeflow import base_component
from tfx.orchestration.kubeflow import utils
from tfx.orchestration.kubeflow import node_wrapper
from tfx.orchestration.kubernetes import controller_entrypoint
from tfx.utils import kube_utils
from tfx.utils import json_utils

import datetime
import absl
import json
import pathlib

_TFX_DEV_IMAGE = 'gcr.io/joonkim-experiments/tfx_dev:latest'

# This entrypoint command assumes image is built using Dockerfile in this directory.
_CONTROLLER_ENTRYPOINT = ['python', '/tfx-async/tfx/orchestration/kubernetes/controller_entrypoint.py']

# Dictionary template for yaml file. Must modify volume mounting
# and service account based on personal credentials.
_replica_set_template = {'apiVersion': 'apps/v1', 'kind': 'Deployment',
                        'metadata': {'name': 'controller'},
                        'spec': 
                          {'replicas': 3,
                          'selector': {'matchLabels': {'app': 'controller'}},
                          'template': 
                            {'metadata': {'labels': {'app': 'controller'}}, 
                            'spec': {
                              'serviceAccountName':'async-ksa', 'automountServiceAccountToken': False,
                              'volumes': [{'name': 'async-ksa-token-2k2ls',
                                'secret': {
                                  'secretName': 'async-ksa-token-2k2ls'
                                }
                              }],
                              'containers': [{'name': 'controller',
                                'image': _TFX_DEV_IMAGE,
                                'command': _CONTROLLER_ENTRYPOINT,
                                'volumeMounts': [{
                                  'name': 'async-ksa-token-2k2ls',
                                  'mountPath': '/var/run/secrets/kubernetes.io/serviceaccount'
                                }]
                              }]
                            }}}
                        }

# Define root tfx directory. Must be modified based on working environment.
_TFX_DIR = "/home/joonkim/TFX/project/tfx"

def is_inside_cluster() -> bool:
  """Determines if kubernetes runner is executed from within a cluster.
  Can be patched for testing purpose.
  """

  return kube_utils.is_inside_cluster()

class ControllerCompiler:
  """Takes in a tfx pipeline to serialize every component and 
  pipeline info including pipeline name, pipeline root, run id,
  and beam pipeline args. Then populates an object with serialized 
  information. Uses object to create a yaml file that can be used to
  deploy a replica set controller that will launch components in cluster.
  """

  def __init__(self, pipeline: tfx_pipeline.Pipeline):
    """
    Args:
      pipeline: a TFX pipeline
    """

    self._pipeline = pipeline
    self._serialized_components = []
    self._pipeline_params = {'pipeline_name': pipeline.pipeline_info.pipeline_name, 
        'pipeline_root': pipeline.pipeline_info.pipeline_root}
    self.args_list = []

  def _parse_parameter_from_component(
      self, component: base_component.BaseComponent) -> None:
    """
    Args:
      component: a TFX component.
    """

    serialized_component = utils.replace_placeholder(
        json_utils.dumps(node_wrapper.NodeWrapper(component)))
    self._serialized_components.append(serialized_component)
  
  def _parse_parameter_for_all_components(self) -> None:
    """Loops through all components inside a pipeline and serializes them"""

    for component in self._pipeline.components:
      self._parse_parameter_from_component(component)
  
  def _parse_pipeline_parameter(self) -> None:
    """Picks out pipeline specific variables and serializes to add to yaml object"""

    self._pipeline_params['run_id'] = datetime.datetime.now().isoformat()
    self._pipeline_params['beam_pipeline_args'] = json.dumps(self._pipeline.beam_pipeline_args)
    self._pipeline_params['additional_pipeline_args'] = json.dumps(self._pipeline.additional_pipeline_args)

  def _build_args_list(self) -> None:

    self.args_list = [
      '--pipeline_name',
      self._pipeline_params['pipeline_name'],
      '--pipeline_root',
      self._pipeline_params['pipeline_root'],
      '--run_id',
      self._pipeline_params['run_id'],
      '--beam_pipeline_args',
      self._pipeline_params['beam_pipeline_args'],
      '--additional_pipeline_args',
      self._pipeline_params['additional_pipeline_args'],
      '--serialized_components',
      str(self._serialized_components),
    ]

  def _serialize_args(self) -> None:
    """Combines serializing args + serializing pipeline info +
    building args list for container spec"""

    self._parse_parameter_for_all_components()
    self._parse_pipeline_parameter()
    self._build_args_list()


  def _write_deployment_yaml(self) -> None:
    
    _replica_set_template['spec']['template']['spec']['containers'][0]['args'] = self.args_list
    
    with open(os.path.join(_TFX_DIR, "tfx/orchestration/kubernetes/controller.yaml"), "w") as f:
      deployment = yaml.dump(_replica_set_template, f)

  
  def _apply_deployment_yaml(self) -> None:

    if not is_inside_cluster():
      return
    
    with open(os.path.join(_TFX_DIR, "tfx/orchestration/kubernetes/controller.yaml")) as f:
      dep = yaml.safe_load(f)
      apps_api = kube_utils.make_apps_v1_api()
      resp = apps_api.create_namespaced_deployment(
          body=dep, namespace="kubernetes")
      absl.logging.info("Deployment created. status='%s'" % resp.metadata.name)


  def run_compiler(self) -> None:
    self._serialize_args()
    self._write_deployment_yaml()
    self._apply_deployment_yaml()
