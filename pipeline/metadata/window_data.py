"""Utility functions for adding timestamps/windowing to data."""

from __future__ import absolute_import

import datetime

import re
from typing import Tuple, Iterable, Any

import apache_beam as beam

from pipeline.metadata import flatten_base


class AddTimestampFromSourceDoFn(beam.DoFn):
  """Add beam timestamps to a row."""

  def process(
      self, element: Tuple[str, str]) -> Iterable[beam.window.TimestampedValue]:
    """Add a beam timestamp to PCollection row using the source date.

    Args:
      element: Tuple[filepath, line]
        filepath is like 'gs://firehook-scans/echo/CP_Quack-echo-2020-08-23-06-01-02/results.json'

    Yields the same row but as a beam timestamped value
    """
    source: str = flatten_base.source_from_filename(element[0])
    date_match = re.search(r'\d\d\d\d-\d\d-\d\d', source)
    if not date_match:
      raise Exception(f"Failed to parse date from source: {element[0]}")
    source_date_string = date_match.group(0)
    source_datetime = datetime.datetime.fromisoformat(source_date_string)
    unix_timestamp = source_datetime.timestamp()
    yield beam.window.TimestampedValue(element, unix_timestamp)


def window(
    lines: beam.PCollection[Tuple[str,
                                  str]]) -> beam.PCollection[Tuple[str, str]]:
  """Add timestamps to data and window it.

  Args:
    lines: pcollection of Tuple[filepath, line]

  Returns:
    identical Pcollection, with with added timestamps and windowing
  """
  timestamped_lines = lines | 'timestamp' >> beam.ParDo(
      AddTimestampFromSourceDoFn().with_output_types(Tuple[str, str]))

  # All data from the same source will have itentical timestamps.
  # So we window to a 1-sec interval
  windowed_lines = (
      timestamped_lines |
      'window lines' >> beam.WindowInto(beam.window.FixedWindows(1)))

  return windowed_lines


def unwindow(rows: beam.PCollection[Any]) -> beam.PCollection[Any]:
  return rows | 'combine windows' >> beam.CombineGlobally().without_defaults()
