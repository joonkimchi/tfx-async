# Copyright 2020 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Definition of Beam TFX runner."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import time
import re
import sys
import ast
from typing import Optional, Text, Type

from typing import Any, Callable, Dict, List, Optional, Text, cast

from kubernetes import client

from tfx import types
from ml_metadata.proto import metadata_store_pb2
from tfx.dsl.component.experimental import container_component
from tfx.dsl.component.experimental import executor_specs
from tfx.components.base import base_node
from tfx.components.base import executor_spec
from tfx.orchestration import data_types
from tfx.orchestration import metadata
from tfx.orchestration import pipeline as tfx_pipeline
from tfx.orchestration import tfx_runner
from tfx.orchestration.config import base_component_config
from tfx.orchestration.config import kubernetes_component_config
from tfx.orchestration.config import config_utils
from tfx.orchestration.config import pipeline_config
from tfx.orchestration.kubeflow import node_wrapper
from tfx.orchestration.kubeflow import utils
from tfx.orchestration.launcher import base_component_launcher_2
from tfx.orchestration.launcher import kubernetes_component_launcher
from tfx.orchestration.launcher import looped_kubernetes_launcher
from tfx.orchestration.launcher import looped_component_launcher
from tfx.orchestration.launcher import container_common
from tfx.orchestration.launcher import in_process_component_launcher
from tfx.utils import json_utils, kube_utils
from google.protobuf import json_format

import json
import pathlib
import logging
import argparse


_TFX_DEV_IMAGE = 'gcr.io/joonkim-experiments/tfx_dev:latest'

_CONTAINER_ENTRYPOINT = [
    'python', '/tfx-async/tfx/orchestration/kubernetes/container_entrypoint.py'
]

_WRAPPER_SUFFIX = ''

# K8s pod monitoring
def _pod_is_not_pending(resp: client.V1Pod):
  return resp.status.phase != kube_utils.PodPhase.PENDING.value


def _pod_is_done(resp: client.V1Pod):
  return kube_utils.PodPhase(resp.status.phase).is_done


def _pod_is_running(resp: client.V1Pod):
  return kube_utils.PodPhase(resp.status.phase).is_running


def _sanitize_pod_name(pod_name: Text) -> Text:
  pod_name = re.sub(r'[^a-z0-9-]', '-', pod_name.lower())
  pod_name = re.sub(r'^[-]+', '', pod_name)
  return re.sub(r'[-]+', '-', pod_name)

def _build_pod_manifest(pod_name: Text,
    component: base_node.BaseNode,
    component_launcher_class:
      Type[base_component_launcher_2.BaseComponentLauncher2],
    component_config: Optional[base_component_config.BaseComponentConfig],
    pipeline_name,
    pipeline_root,
    run_id,
    beam_pipeline_args,
    additional_pipeline_args
  ) -> Dict[Text, Any]:
  """Build a pod spec.

  Args:
    pod_name: The name of the pod.
    container_spec: The resolved executor container spec.

  Returns:
    The pod manifest in dictionary format.
  """
  # Build args
  component_launcher_class_path = '.'.join([
    component_launcher_class.__module__, component_launcher_class.__name__
  ])

  serialized_component = utils.replace_placeholder(
      json_utils.dumps(node_wrapper.NodeWrapper(component)))
    
  logging.info('**SERIALIZE INSIDE POD MANIFEST***')
  logging.info(serialized_component)

  arguments = [
      '--pipeline_name',
      pipeline_name,
      '--pipeline_root',
      pipeline_root,
      '--run_id',
      run_id,
      '--metadata_config',
      json_format.MessageToJson(
        message=get_default_kubernetes_metadata_config(),
        preserving_proto_field_name=True),
      '--beam_pipeline_args',
      json.dumps(beam_pipeline_args),
      '--additional_pipeline_args',
      json.dumps(additional_pipeline_args),
      '--component_launcher_class_path',
      component_launcher_class_path,
      '--serialized_component',
      serialized_component,
      '--component_config',
      json_utils.dumps(component_config),
  ]

  pod_manifest = {}

  pod_manifest.update({
      'apiVersion': 'v1',
      'kind': 'Pod',
  })
  # TODO(hongyes): figure out a better way to figure out type hints for nested
  # dict.
  metadata = pod_manifest.setdefault('metadata', {})  # type: Dict[Text, Any]
  metadata.update({'name': pod_name})
  spec = pod_manifest.setdefault('spec', {})  # type: Dict[Text, Any]
  spec.update({'restartPolicy': 'OnFailure', 'serviceAccount':'async-ksa', 'automountServiceAccountToken': False})
  containers = spec.setdefault('containers',
                                [])  # type: List[Dict[Text, Any]]
  container = None  # type: Optional[Dict[Text, Any]]
  container = {'name': kube_utils.ARGO_MAIN_CONTAINER_NAME}
  containers.append(container)
  container.update({
      'image': _TFX_DEV_IMAGE,
      'command': _CONTAINER_ENTRYPOINT,
      'args': arguments,
  })
  return pod_manifest

def _get_pod(core_api: client.CoreV1Api, pod_name: Text,
              namespace: Text) -> Optional[client.V1Pod]:
  """Get a pod from Kubernetes metadata API.

  Args:
    core_api: Client of Core V1 API of Kubernetes API.
    pod_name: The name of the POD.
    namespace: The namespace of the POD.

  Returns:
    The found POD object. None if it's not found.

  Raises:
    RuntimeError: When it sees unexpected errors from Kubernetes API.
  """
  try:
    return core_api.read_namespaced_pod(name=pod_name, namespace=namespace)
  except client.rest.ApiException as e:
    if e.status != 404:
      raise RuntimeError('Unknown error! \nReason: %s\nBody: %s' %
                          (e.reason, e.body))
    return None

def _wait_pod(core_api: client.CoreV1Api,
              pod_name: Text,
              namespace: Text,
              exit_condition_lambda: Callable[[client.V1Pod], bool],
              condition_description: Text,
              timeout_sec: int = 300) -> client.V1Pod:
  """Wait for a POD to meet an exit condition.

  Args:
    core_api: Client of Core V1 API of Kubernetes API.
    pod_name: The name of the POD.
    namespace: The namespace of the POD.
    exit_condition_lambda: A lambda which will be called intervally to wait
      for a POD to exit. The function returns True to exit.
    condition_description: The description of the exit condition which will be
      set in the error message if the wait times out.
    timeout_sec: The seconds for the function to wait. Defaults to 300s.

  Returns:
    The POD object which meets the exit condition.

  Raises:
    RuntimeError: when the function times out.
  """
  start_time = datetime.datetime.utcnow()
  while True:
    resp = _get_pod(core_api, pod_name, namespace)
    logging.info(resp.status.phase)
    if exit_condition_lambda(resp):
      return resp
    elapse_time = datetime.datetime.utcnow() - start_time
    if elapse_time.seconds >= timeout_sec:
      raise RuntimeError(
          'Pod "%s:%s" does not reach "%s" within %s seconds.' %
          (namespace, pod_name, condition_description, timeout_sec))
    # TODO(hongyes): add exponential backoff here.
    time.sleep(1)

def _build_pod_name(pipeline_name, component_id) -> Text:
  pod_name = '%s-%s' % (pipeline_name[:100], component_id[:50])
  return _sanitize_pod_name(pod_name)


def is_inside_cluster() -> bool:
  """Determines if kubernetes runner is executed from within a cluster.
  Can be patched for testing purpose.
  """
  return kube_utils.is_inside_cluster()


# Metadata config
def get_default_kubernetes_metadata_config(
) -> metadata_store_pb2.ConnectionConfig:
  """Returns the default metadata connection config for a kubernetes cluster.
  Returns:
    A config proto that will be serialized as JSON and passed to the running
    container so the TFX component driver is able to communicate with MLMD in
    a kubernetes cluster.
  """
  connection_config = metadata_store_pb2.ConnectionConfig()
  connection_config.mysql.host = 'mysql'
  connection_config.mysql.port = 3306
  connection_config.mysql.database = 'mysql'
  connection_config.mysql.user = 'root'
  connection_config.mysql.password = ''
  return connection_config


def deserialize_component(serialized_component):
  return json_utils.loads(serialized_component)  


# Driver code
def main():
  logging.basicConfig(stream=sys.stdout, level=logging.INFO)
  logging.getLogger().setLevel(logging.INFO)

  parser = argparse.ArgumentParser()
  parser.add_argument('--pipeline_name', type=str, required=True)
  parser.add_argument('--pipeline_root', type=str, required=True)
  parser.add_argument('--run_id', type=str, required=True)
  parser.add_argument('--beam_pipeline_args', type=str, required=True)
  parser.add_argument(
      '--additional_pipeline_args', type=str, required=True)
  parser.add_argument('--serialized_components', type=str, required=True)

  args = parser.parse_args()

  config = pipeline_config.PipelineConfig(
          supported_launcher_classes=[
              looped_component_launcher.LoopedComponentLauncher,
          ],
      )

  logging.info("Starting inside entrypoint")


  if not is_inside_cluster():
      return

  logging.info(args.serialized_components)

  listified_components = ast.literal_eval(args.serialized_components)
    
  # Runs component in topological order
  for serialized_component in listified_components:
    component = deserialize_component(serialized_component)
    logging.info('Launching %s' % component.id)
    (component_launcher_class,
      component_config) = config_utils.find_component_launch_info(
          config, component)

    # Do launching
    pod_name = _build_pod_name(args.pipeline_name, component.id)
    namespace = 'kubernetes'
    core_api = kube_utils.make_core_v1_api()
    pod_manifest = _build_pod_manifest(pod_name, 
                                      component,
                                      component_launcher_class,
                                      component_config,
                                      args.pipeline_name,
                                      args.pipeline_root,
                                      args.run_id,
                                      args.beam_pipeline_args,
                                      args.additional_pipeline_args)

    logging.info('Looking for pod "%s:%s".', namespace, pod_name)
    resp = _get_pod(core_api, pod_name, namespace)

    if not resp:
      logging.info('Pod "%s:%s" does not exist. Creating it...',
                  namespace, pod_name)
      try:
        resp = core_api.create_namespaced_pod(
            namespace=namespace, body=pod_manifest)
      except client.rest.ApiException as e:
        logging.info(e)
        raise RuntimeError(
            'Failed to created container executor pod!\nReason: %s\nBody: %s' %
            (e.reason, e.body))

    logging.info('Waiting for pod "%s:%s" to start.', namespace, pod_name)
    _wait_pod(
        core_api,
        pod_name,
        namespace,
        exit_condition_lambda=_pod_is_not_pending,
        condition_description='non-pending status')

    logging.info('Start log streaming for pod "%s:%s".', namespace, pod_name)
    try:
      logs = core_api.read_namespaced_pod_log(
          name=pod_name,
          namespace=namespace,
          container=kube_utils.ARGO_MAIN_CONTAINER_NAME,
          follow=True,
          _preload_content=False).stream()
    except client.rest.ApiException as e:
      raise RuntimeError(
          'Failed to stream the logs from the pod!\nReason: %s\nBody: %s' %
          (e.reason, e.body))

    # Add limited time for logging to launch next pods
    timeout = time.time() + 75
    for log in logs:
      if time.time() > timeout:
        break
      logging.info(log.decode().rstrip('\n'))

    resp = _wait_pod(
        core_api,
        pod_name,
        namespace,
        exit_condition_lambda=_pod_is_running,
        condition_description='running state')

    if resp.status.phase == kube_utils.PodPhase.FAILED.value:
      raise RuntimeError('Pod "%s:%s" failed with status "%s".' %
                        (namespace, pod_name, resp.status))

    logging.info('Pod "%s:%s" is running. Launching next component.', namespace, pod_name)
  logging.info('Finished launching all components, exiting')

if __name__ == '__main__':
  main()