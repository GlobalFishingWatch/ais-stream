import sys

from pipe_tools.options import validate_options
from pipe_tools.options import LoggingOptions

from stream_decode.options import DecodeOptions
from stream_decode import pipeline


def run(args=None):
    options = validate_options(args=args, option_classes=[LoggingOptions, DecodeOptions])

    options.view_as(LoggingOptions).configure_logging()

    return pipeline.run(options)


if __name__ == '__main__':
    sys.exit(run(args=sys.argv))
