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
import sys
import time

import schedule

from mirror.untar_files.sync import get_firehook_scanfile_mirror
from mirror.routeviews.sync import get_firehook_routeview_mirror
from table.run_queries import rebuild_all_tables


def run_pipeline():
  """Steps of the pipeline to run nightly."""
  get_firehook_scanfile_mirror().sync()
  get_firehook_routeview_mirror().sync()

  # This is a very weird hack.
  # We execute the beam pipeline as a seperate process
  # because beam really doesn't like it when the main file for a pipeline
  # execution is not the same file the pipeline run call is made in.
  # It would require all the deps to be packaged and installed on the workers
  # which in our case requires packaging up many google cloud packages
  # which is slow (hangs basic worker machines) and wasteful.
  subprocess.run([sys.executable, '-m', 'pipeline.beam_tables', '--env=prod'],
                 check=True,
                 stdout=subprocess.PIPE)

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
