"""Beam pipeline steps for hyperquack data"""

from __future__ import absolute_import

from typing import Tuple

import apache_beam as beam

from pipeline.metadata import flatten
from pipeline.metadata.schema import HyperquackRow
from pipeline.metadata.add_metadata import MetadataAdder


def process_hyperquack_lines(
    lines: beam.pvalue.PCollection[Tuple[str, str]],
    metadata_adder: MetadataAdder) -> beam.pvalue.PCollection[HyperquackRow]:
  """Process hyperquack data."""

  rows = (
      lines | 'flatten json' >> beam.ParDo(
          flatten.FlattenMeasurement()).with_output_types(HyperquackRow))

  # PCollection[HyperquackRow]
  rows_with_ip_annotations = metadata_adder.annotate_row_ip(rows)

  return rows_with_ip_annotations
