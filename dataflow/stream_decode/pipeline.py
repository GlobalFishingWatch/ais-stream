import logging
import ujson
from datetime import datetime

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.runners import PipelineState
from apache_beam.options.pipeline_options import StandardOptions
from stream_decode.options import DecodeOptions
from stream_decode.decode import DecodeNMEA


class GroupWindowsIntoBatches(beam.PTransform):
    """A composite transform that groups Pub/Sub messages based on publish
    time and outputs a list of dictionaries, where each contains one message
    and its publish timestamp.
    """

    def __init__(self, window_size):
        # Convert minutes into seconds.
        self.window_size = int(window_size * 60)

    def expand(self, pcoll):
        return (
            pcoll
            # Assigns window info to each Pub/Sub message based on its
            # publish timestamp.
            | "Window into Fixed Intervals"
            >> beam.WindowInto(window.FixedWindows(self.window_size))
            | "Add timestamps to messages" >> beam.ParDo(AddTimestamps())
            # Use a dummy key to group the elements in the same window.
            # Note that all the elements in one window must fit into memory
            # for this. If the windowed elements do not fit into memory,
            # please consider using `beam.util.BatchElements`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements
            | "Add Dummy Key" >> beam.Map(lambda elem: (None, elem))
            | "Groupby" >> beam.GroupByKey()
            | "Abandon Dummy Key" >> beam.MapTuple(lambda _, val: val)
        )


class AddTimestamps(beam.DoFn):
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        """Processes each incoming windowed element by extracting the Pub/Sub
        message and its publish timestamp into a dictionary. `publish_time`
        defaults to the publish timestamp returned by the Pub/Sub server. It
        is bound to each element by Beam at runtime.
        """

        element['publish_time'] = float(publish_time)
        yield element
        # yield {
        #     "message_body": element.decode("utf-8"),
        #     "publish_time": float(publish_time)
        # }



def load_schema(schema_json):
    schema = {"fields": ujson.loads(schema_json)}
    logging.info('loaded schema {}'.format(schema))
    return schema


def run(options):
    decode_options = options.view_as(DecodeOptions)
    standard_options = options.view_as(StandardOptions)
    schema = load_schema(decode_options.schema)

    pipeline = beam.Pipeline(options=options)
    (
        pipeline
        | "Read PubSub Messages" >> beam.io.ReadFromPubSub(topic=decode_options.input_topic)
        | "Parse NMEA" >> beam.ParDo(DecodeNMEA(schema))
        | "Window into" >> GroupWindowsIntoBatches(decode_options.window_size)
        | beam.FlatMap(lambda val: val)
        | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=decode_options.output_table,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )

#            | "Write to GCS" >> beam.ParDo(WriteBatchesToGCS(output_path))
    )

    result = pipeline.run()

    success_states = set([PipelineState.DONE])
    if standard_options.runner == 'DirectRunner':
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1


