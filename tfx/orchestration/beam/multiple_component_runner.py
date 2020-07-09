from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import datetime
import os
from typing import Any, Iterable, List, Optional, Text, Type

from multiprocessing import Process

import absl

from tfx.components.base import base_node
from tfx.orchestration import data_types
from tfx.orchestration import metadata
from tfx.orchestration import pipeline
from tfx.orchestration import tfx_runner
from tfx.orchestration.config import base_component_config
from tfx.orchestration.config import config_utils
from tfx.orchestration.config import pipeline_config
from tfx.orchestration.launcher import base_component_launcher
from tfx.orchestration.launcher import docker_component_launcher
from tfx.orchestration.launcher import looped_component_launcher
from tfx.orchestration.launcher import base_component_launcher_2

import time

class MultCompRunner(tfx_runner.TfxRunner):

    def __init__(self, config: Optional[pipeline_config.PipelineConfig] = None):
        if config is None:
            config = pipeline_config.PipelineConfig(
                supported_launcher_classes=[
                    looped_component_launcher.LoopedComponentLauncher,
                    docker_component_launcher.DockerComponentLauncher,
                ],
            )
        super(MultCompRunner, self).__init__(config)

    def _launch_component(self, launcher):
        launcher.launch()

    def run(self, tfx_pipeline: pipeline.Pipeline) -> None: 
        tfx_pipeline.pipeline_info.run_id = datetime.datetime.now().isoformat()
        for component in tfx_pipeline.components:
            component_id = component.id
            absl.logging.info(component_id)
            (component_launcher_class, 
            component_config) = config_utils.find_component_launch_info(self._config, component)

            driver_args = data_types.DriverArgs()
            metadata_connection = metadata.Metadata(
                tfx_pipeline.metadata_connection_config)

            self._component_launcher = component_launcher_class.create(
                component=component,
                pipeline_info=tfx_pipeline.pipeline_info,
                driver_args=driver_args,
                metadata_connection=metadata_connection,
                beam_pipeline_args=tfx_pipeline.beam_pipeline_args,
                additional_pipeline_args=tfx_pipeline.additional_pipeline_args,
                component_config=component_config)

            self._component_id = component.id
            
            absl.logging.info("before launchS")

            new_process = Process(target=self._launch_component, args=(self._component_launcher,))
            new_process.start()
            time.sleep(15)
        absl.logging.info("end of loop")