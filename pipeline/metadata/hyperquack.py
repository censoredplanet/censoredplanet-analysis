"""Beam pipeline steps for hyperquack data"""

from __future__ import absolute_import

from typing import Tuple, Optional

import apache_beam as beam

from pipeline.metadata import flatten
from pipeline.metadata.schema import HyperquackRow, BigqueryRow
from pipeline.metadata.add_metadata import MetadataAdder
from pipeline.metadata.hyperquack_outcome import classify_hyperquack_outcome


def _get_scan_type_from_source(source: Optional[str]) -> str:
  """
  source: a source string like "CP_Quack-https-2022-03-02-23-42-06"

  scan_type: one of 'echo', 'discard', 'http', 'https'
  """
  if source is None:
    raise Exception("source field is None")
  if 'echo' in source:
    return 'echo'
  if 'discard' in source:
    return 'discard'
  if 'https' in source:
    return 'https'
  if 'http' in source:
    return 'http'
  raise Exception(f"Can't determine scan type for source {source}")


def _add_outcome(row: BigqueryRow) -> HyperquackRow:
  row.outcome = classify_hyperquack_outcome(
      row.error, _get_scan_type_from_source(row.source), row.received.status,
      row.success, row.received.is_known_blockpage, row.received.page_signature,
      row.received.headers)
  return row


def process_hyperquack_lines(
    lines: beam.pvalue.PCollection[Tuple[str, str]],
    metadata_adder: MetadataAdder) -> beam.pvalue.PCollection[HyperquackRow]:
  """Process hyperquack data."""

  # PCollection[HyperquackRow]
  rows = (
      lines | 'flatten json' >> beam.ParDo(
          flatten.FlattenMeasurement()).with_output_types(HyperquackRow))

  # PCollection[HyperquackRow]
  rows_with_ip_annotations = metadata_adder.annotate_row_ip(rows)

  # PCollection[HyperquackRow]
  rows_with_outcomes = (
      rows_with_ip_annotations |
      'add outcomes' >> beam.Map(_add_outcome).with_output_types(HyperquackRow))

  return rows_with_outcomes
