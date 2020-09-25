import ujson
import logging

import apache_beam as beam
from ais_tools.message import Message
from ais_tools.aivdm import AIVDM


class DecodeNMEA(beam.DoFn):
    decoder = AIVDM()

    def __init__(self, schema, source='ais-stream'):
        self.schema = schema
        self.source = source

    def process(self, element):
        """Decode the nmea element in the message body
        """

        logging.info('decoding element {}'.format(element))
        message = Message(element.decode("utf-8"))
        message.update(self.decoder.safe_decode(message.get('nmea')))
        message.add_source(self.source)
        message.add_uuid()

        schema_fields = {f['name'] for f in self.schema['fields']}
        extra_fields = {k: v for k, v in message.items() if k not in schema_fields}
        if extra_fields:
            message = {k: v for k, v in message.items() if k not in extra_fields}
            message['extra'] = ujson.dumps(extra_fields)

        yield message
