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
"""Beam pipeline runner.

Independant pipelines are run from here instead of directly from beam_tables.py
to allow for absolute imports.
"""

import argparse

from pipeline import beam_tables

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Run a beam pipeline over scans')
  parser.add_argument(
      '--full',
      action='store_true',
      default=False,
      help='Run over all files and not just the latest (rebuilds tables)')
  parser.add_argument(
      '--env',
      type=str,
      default='dev',
      choices=['dev', 'prod'],
      help='Whether to run over prod or dev data')
  parser.add_argument(
      '--scan_type',
      type=str,
      default='echo',
      choices=['all'] + list(beam_tables.SCAN_TYPES_TO_ZONES.keys()),
      help='Which type of scan to run over')
  args = parser.parse_args()

  incremental = not args.full

  runner = beam_tables.get_firehook_beam_pipeline_runner()

  if args.env == 'dev':
    runner.run_dev(args.scan_type, incremental)
  elif args.env == 'prod':
    runner.run_all_scan_types(incremental, 'prod')
