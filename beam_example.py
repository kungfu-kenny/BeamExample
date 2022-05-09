import re
import logging
import apache_beam as beam
from apache_beam.dataframe import convert
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from config import (
    header_bool,
    header_test,
    temp_input,
    temp_output,
    schema, 
    dataset,
    table_id,
    project_id,
    file_current,
)


class ExampleOptions(PipelineOptions):
   @classmethod
   def _add_argparse_args(cls, parser):
        parser.add_argument(
          '--input',
          default=file_current if not temp_input else temp_input,
          help='Path of the file to read from'
        )
        parser.add_argument(
          '--output',
          required=False,
          help='Output file to write results to.'
        )

def run(argv=None):
    pipeline_options = PipelineOptions(['--output', temp_output])

    with beam.Pipeline(options=pipeline_options) as pipeline:
        user_options = pipeline_options.view_as(ExampleOptions)
        values_new = (
            pipeline | beam.dataframe.io.read_csv(
                user_options.input,
                header=0
            )
        )
        values_send = (   
            convert.to_pcollection(values_new)
            | beam.Map(lambda x: dict(x._asdict()))
            | beam.Map(lambda x: {k:v for k, v in x.items() if k in header_test} if header_bool else x)
            | beam.Filter(lambda x: x.get('Age', 0) > 70)
            # | beam.Map(print)
        )
            
        values_send | beam.io.WriteToBigQuery(
            bigquery.TableReference(
                projectId=project_id,
                datasetId=dataset,
                tableId=table_id
            ),
            schema=schema,
            method="STREAMING_INSERTS",
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()