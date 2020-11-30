#!/bin/bash

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

# Use this script to download data into a csv file for further manual processing
#
# This script requires installing the gcloud command line tool for bq.
# https://cloud.google.com/sdk/gcloud

GLOBIGNORE="*"

# Example query
# Downloads all HTTP scans from Sudan between 2018-12-01 and 2019-01-30
query="#standardSQL
       SELECT *
       FROM \`firehook-censoredplanet.merged_results.cleaned_error_scans\`
       WHERE country = 'SD' AND
             source = 'HTTP' AND
             '2018-12-01' < date AND date < '2019-01-30'
       "

bq --apilog=/tmp/query.log --format=csv query "${query}" > sudan_http_data.csv
