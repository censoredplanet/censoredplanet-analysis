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

CREATE OR REPLACE TABLE `firehook-censoredplanet.fortuna.net_as`
PARTITION BY date
CLUSTER BY netblock
AS (
  SELECT DISTINCT
    date,
    netblock,
    asn,
    as_full_name AS as_full_name
  FROM `firehook-censoredplanet.https_results.scan`
);
