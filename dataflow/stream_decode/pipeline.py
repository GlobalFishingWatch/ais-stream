import logging
import ujson

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.runners import PipelineState
from apache_beam.options.pipeline_options import StandardOptions
from stream_decode.options import DecodeOptions
from stream_decode.decode import DecodeNMEA
from stream_decode.pubsub import ReadFromPubSub
from stream_decode.bigquery import ReadFromBigQuery


def load_schema(schema_json):
    schema = {"fields": ujson.loads(schema_json)}
    logging.info('loaded schema {}'.format(schema))
    return schema


def run(options):
    decode_options = options.view_as(DecodeOptions)
    standard_options = options.view_as(StandardOptions)
    schema = load_schema(decode_options.schema)

    pipeline = beam.Pipeline(options=options)
    if decode_options.input_topic:
        messages = pipeline | "Read PubSub Messages" >> ReadFromPubSub(
                                        topic=decode_options.input_topic,
                                        window_size=decode_options.window_size)
    elif decode_options.input_table:
        messages = pipeline | "Read from BigQuery" >> ReadFromBigQuery(
                                        table=decode_options.input_table)
    else:
        logging.error('No input source specified.  Must specify one of input_topic or input_table')
        return 1

    parsed = messages | "Parse NMEA" >> beam.ParDo(DecodeNMEA(schema))
    parsed | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            table=decode_options.output_table,
            schema=schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )

    result = pipeline.run()

    success_states = set([PipelineState.DONE])
    if standard_options.runner == 'DirectRunner':
        result.wait_until_finish()
    else:
        success_states.add(PipelineState.RUNNING)

    logging.info('returning with result.state=%s' % result.state)
    return 0 if result.state in success_states else 1


