# Copyright 2020 Jigsaw Operations LLC
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

import argparse
import datetime
import unittest
from unittest.mock import call, patch, MagicMock

from freezegun import freeze_time

from pipeline import beam_tables
from pipeline import run_beam_tables


class RunBeamTablesTest(unittest.TestCase):

  def test_run_single_pipelines(self):
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    run_beam_tables.run_parallel_pipelines(mock_runner, 'base', ['echo'], True,
                                           datetime.date(2020, 1, 1),
                                           datetime.date(2020, 1, 2))

    mock_runner.run_beam_pipeline.assert_called_with('echo', True,
                                                     'append-base-echo-scan',
                                                     'base.echo_scan',
                                                     datetime.date(2020, 1, 1),
                                                     datetime.date(2020, 1, 2))

  def test_run_parallel_pipelines(self):
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    run_beam_tables.run_parallel_pipelines(mock_runner, 'laplante',
                                           ['http', 'https'], False)

    call1 = call('http', False, 'write-laplante-http-scan',
                 'laplante.http_scan', None, None)
    call2 = call('https', False, 'write-laplante-https-scan',
                 'laplante.https_scan', None, None)
    mock_runner.run_beam_pipeline.assert_has_calls([call1, call2],
                                                   any_order=True)

  @freeze_time('2020-01-15')
  def test_run_user_pipeline_full(self):
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    run_beam_tables.run_user_pipelines(mock_runner, 'laplante', ['echo'], False)

    mock_runner.run_beam_pipeline.assert_called_with(
        'echo', False, 'write-laplante-echo-scan', 'laplante.echo_scan',
        datetime.date(2020, 1, 1), datetime.date(2020, 1, 8))

  @freeze_time('2020-01-15')
  def test_run_user_pipeline_incremental(self):
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    run_beam_tables.run_user_pipelines(mock_runner, 'laplante', ['echo'], True)

    mock_runner.run_beam_pipeline.assert_called_with(
        'echo', True, 'append-laplante-echo-scan', 'laplante.echo_scan',
        datetime.date(2020, 1, 8), datetime.date(2020, 1, 15))

  def test_main(self):
    mock_runner = MagicMock(beam_tables.ScanDataBeamPipelineRunner)

    with patch('pipeline.run_beam_tables.get_firehook_beam_pipeline_runner',
               lambda: mock_runner):
      args = argparse.Namespace(full=False, scan_type='all', env='prod')
      run_beam_tables.main(args)

      call1 = call('echo', True, 'append-base-echo-scan', 'base.echo_scan',
                   None, None)
      call2 = call('discard', True, 'append-base-discard-scan',
                   'base.discard_scan', None, None)
      call3 = call('http', True, 'append-base-http-scan', 'base.http_scan',
                   None, None)
      call4 = call('https', True, 'append-base-https-scan', 'base.https_scan',
                   None, None)
      mock_runner.run_beam_pipeline.assert_has_calls(
          [call1, call2, call3, call4], any_order=True)


if __name__ == '__main__':
  unittest.main()
