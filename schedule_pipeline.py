# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Orchestrate the pieces of the Censored Planet Data Pipeline.

This script is means to be run on a GCE machine.
To deploy to GCE use the deploy.sh script.
"""
import subprocess
import time

import schedule

from mirror.untar_files.sync import get_firehook_scanfile_mirror
from mirror.routeviews.sync import get_firehook_routeview_mirror
from pipeline.beam_tables import get_firehook_beam_pipeline_runner
from table.run_queries import rebuild_all_tables


def run_pipeline():
  """Steps of the pipeline to run nightly."""
  get_firehook_scanfile_mirror().sync()
  get_firehook_routeview_mirror().sync()
  get_firehook_beam_pipeline_runner().run_all_scan_types(True, 'prod')
  rebuild_all_tables()


def run():
  run_pipeline()  # run once when starting to catch new errors when deploying

  schedule.every().day.at('04:00').do(run_pipeline)

  while True:
    schedule.run_pending()
    wait = schedule.idle_seconds()
    print('Waiting {} seconds until the next run'.format(wait))
    time.sleep(wait)


if __name__ == '__main__':
  run()
