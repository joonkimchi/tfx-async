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

template = ['ls', '-a']
_CONTROLLER_ENTRYPOINT = ['python', '/tfx-async/tfx/orchestration/kubernetes/controller_entrypoint.py']


test_template = {}
_replica_set_template = {'apiVersion': 'apps/v1', 'kind': 'Deployment',
                        'metadata': {'name': 'controller'},
                        'spec': 
                          {'replicas': 3, 'selector': {'matchLabels': {'app': 'controller'}},
                          'template': 
                            {'metadata': {'labels': {'app': 'controller'}}, 
                            'spec': {'containers': 
                                [{'name': 'controller',
                                'image': _TFX_DEV_IMAGE,
                                'command': _CONTROLLER_ENTRYPOINT}]
                            }}}
                        }

_TFX_DIR = "/home/joonkim/TFX/project/tfx"

def is_inside_cluster() -> bool:
  """Determines if kubernetes runner is executed from within a cluster.
  Can be patched for testing purpose.
  """
  return kube_utils.is_inside_cluster()

class ControllerCompiler:
  def __init__(self, pipeline: tfx_pipeline.Pipeline):
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
    for component in self._pipeline.components:
      self._parse_parameter_from_component(component)
  
  def _parse_pipeline_parameter(self) -> None:
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
      []
      #self._serialized_components,
    ]

  def _serialize_args(self) -> None:
    self._parse_parameter_for_all_components()
    self._parse_pipeline_parameter()
    self._build_args_list()
    return


  def _write_deployment_yaml(self) -> None:
    #_replica_set_template['spec']['template']['spec']['containers'][0]['args'] = self.args_list
    
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
      absl.logging.info(resp)
      absl.logging.info("Deployment created. status='%s'" % resp.metadata.name)


  def run_compiler(self) -> None:
    self._serialize_args()
    self._write_deployment_yaml()
    self._apply_deployment_yaml()
    return