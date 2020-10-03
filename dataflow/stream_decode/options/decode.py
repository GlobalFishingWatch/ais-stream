from apache_beam.options.pipeline_options import PipelineOptions

from pipe_tools.options import ReadFileAction


class DecodeOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Use add_value_provider_argument for arguments to be templatable
        # Use add_argument as usual for non-templatable arguments

        required = parser.add_argument_group('Required')
        optional = parser.add_argument_group('Optional')

        optional.add_argument(
            "--input_topic",
            help="The Cloud Pub/Sub topic to read from.\n"
                 '"projects/<PROJECT_NAME>/topics/<TOPIC_NAME>".',
        )
        optional.add_argument(
            "--input_table",
            help="The Bigquery table to read from.\n"
                 '"<PROJECT_NAME>:<DATASET>.<TABLE>"',
        )
        required.add_argument(
            "--window_size",
            required=True,
            type=float,
            default=1.0,
            help="Output file's window size in number of minutes.",
        )
        # parser.add_argument(
        #     "--output_path",
        #     help="GCS Path of the output file including filename prefix.",
        # )
        required.add_argument(
            "--output_table",
            required=True,
            help="Bigquery table for output",
        )
        optional.add_argument(
            '--schema',
            action=ReadFileAction,
            help='bigquery (json) schema to use for writing messages.  Fields not in the schema will be '
                 'packed into a single "extra" field.  Pass JSON directly or use @path/to/file to load from a file.')
        optional.add_argument(
            '--wait',
            default=False,
            action='store_true',
            help='Wait until the job finishes before returning.')
