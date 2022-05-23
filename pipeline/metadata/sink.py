"""JsonGzSink class to handle writing to GCS files."""

from typing import BinaryIO

from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.fileio import FileMetadata, FileSink
from apache_beam.coders import coders


class JsonGzSink(FileSink):
  """A sink to a GCS or local .json.gz file."""

  def __init__(self) -> None:
    """Initialize a JsonGzSink."""
    self.coder = coders.ToBytesCoder()
    self.file_handle: BinaryIO

  def create_metadata(self, destination: str,
                      full_file_name: str) -> FileMetadata:
    """Returns the file metadata as tuple (mime_type, compression_type)."""
    return FileMetadata(
        mime_type="application/json", compression_type=CompressionTypes.GZIP)

  def open(self, fh: BinaryIO) -> None:
    """Prepares the sink file for writing."""
    self.file_handle = fh

  def write(self, record: str) -> None:
    """Writes a single record."""
    self.file_handle.write(self.coder.encode(record) + b'\n')

  def flush(self) -> None:
    """Flushes the sink file."""
    # This method must be implemented for FileSink subclasses,
    # but calling self.file_handle.flush() here triggers a zlib error.
    pass  # pylint: disable=unnecessary-pass
