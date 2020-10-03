import apache_beam as beam
import apache_beam.transforms.window as window

from ais_tools.message import Message


class ToMessage(beam.DoFn):

    def process(self, element):
        """convert element to a Message
        """

        message = Message(element.decode("utf-8"))
        yield message


class AddPublishTime(beam.DoFn):
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        """Add the PubSub publish time to the message
        """

        element['publish_time'] = float(publish_time)
        yield element


class ReadFromPubSub(beam.PTransform):
    """
    Read streaming messages from Pubsub and perform time windowing
    """

    def __init__(self, topic, window_size):
        self.topic = topic

        # Convert minutes into seconds.
        self.window_size = int(window_size * 60)

    def expand(self, pcoll):
        return (
            pcoll
            | "Read PubSub Messages" >> beam.io.ReadFromPubSub(topic=self.topic)
            | "Convert to Message" >> beam.ParDo(ToMessage())
            | "Add published time" >> beam.ParDo(AddPublishTime())
            | "Window into Fixed Intervals"
            >> beam.WindowInto(window.FixedWindows(self.window_size))
            # Use a dummy key to group the elements in the same window.
            # Note that all the elements in one window must fit into memory
            # for this. If the windowed elements do not fit into memory,
            # please consider using `beam.util.BatchElements`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.transforms.util.html#apache_beam.transforms.util.BatchElements
            | "Add Dummy Key" >> beam.Map(lambda elem: (None, elem))
            | "Groupby" >> beam.GroupByKey()
            | "Abandon Dummy Key" >> beam.MapTuple(lambda _, val: val)
            | "Ungroup" >> beam.FlatMap(lambda val: val)
        )
