import os
import json
import logging
import argparse
import apache_beam as beam
from apache_beam.dataframe import convert
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from config import (
    temp_input,
    bucket,
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

def check_input(path:str) -> bool:
    _, ext = os.path.splitext(path)
    return ext.lower() == '.csv'

class SplitWords(beam.DoFn):
  def __init__(self):
      pass

  def process(self, text):
    for word in json.loads(text):
      yield word

def run(argv=None):
    pipeline_options = PipelineOptions(
            runner=argv.runner,
            project=argv.project,
            temp_location = f'gs://{argv.bucket}/temp',
            # staging_location = f'gs://{argv.bucket}/staging',
            # template_location=f'gs://{argv.bucket}/templates/test2.json',
            region=argv.region
    )
    with beam.Pipeline(options=pipeline_options) as pipeline:
        user_options = pipeline_options.view_as(ExampleOptions)
        if check_input(user_options.input):
            values_new = (
                pipeline | beam.dataframe.io.read_csv(
                    user_options.input,
                    header=0
                )
            )
            values_send = (   
                convert.to_pcollection(values_new)
                | beam.Map(lambda x: dict(x._asdict()))
                | beam.Map(lambda x: {k:v for k, v in x.items() if k in argv.header_list} if argv.header_list else x)
                | beam.Filter(lambda x: x.get('Age', 0) > 70)
                # | beam.Map(print)
            )
        else:
            
            values_new = (
                pipeline 
                | beam.io.ReadFromText(user_options.input)
                | beam.ParDo(SplitWords())
            )
            values_send = (
                values_new 
                | beam.Map(lambda x: {k:v for k, v in x.items() if k in argv.header_list} if argv.header_list else x)
                | beam.Filter(lambda x: x.get('Age', 0) > 70)
                # | beam.Map(print)
            )

        values_send | beam.io.WriteToBigQuery(
            bigquery.TableReference(
                projectId=argv.project,
                datasetId=argv.dataset,
                tableId=argv.table
            ),
            schema=schema,
            method="STREAMING_INSERTS",
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        help="Input PubSub subscription of the form ",
        required=False,
        default=file_current
    )
    parser.add_argument(
        '--project',
        required=False,
        default=project_id,
        help="ID of the used project",
    )
    parser.add_argument(
        "--header_list",
        required=False,
        nargs='+',
        default=[],
        help="Select columns which is going to be used",
    )
    parser.add_argument(
        "--runner",
        required=False,
        default='DataflowRunner'
    )
    parser.add_argument(
        '--region',
        required=False,
        default='us-central1',
        help='region where to operate'
    )
    parser.add_argument(
        '--dataset',
        required=False,
        default=dataset,
        help='selected dataset values'
    )
    parser.add_argument(
        '--table',
        required=False,
        default=table_id,
        help='data table where to insert values'
    )
    parser.add_argument(
        '--bucket',
        default=bucket,
        help='bucket of the user'
    )
    args, beam_args = parser.parse_known_args()
    logging.getLogger().setLevel(logging.INFO)
    run(args)