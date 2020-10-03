import apache_beam as beam
import apache_beam.transforms.window as window

from ais_tools.message import Message


class ToMessage(beam.DoFn):

    def process(self, element):
        """convert element to a Message
        """

        message = Message(element)
        yield message


class ReadFromBigQuery(beam.PTransform):
    """
    Read batch messages from BigQuery
    """

    def __init__(self, table):
        self.table = table

    def expand(self, pcoll):
        return (
            pcoll
            | "Read from BigQuery" >> beam.io.Read(beam.io.BigQuerySource(self.table))
            | "Convert to Message" >> beam.ParDo(ToMessage())
        )
