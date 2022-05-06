import logging
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from config import (
    schema, 
    dataset,
    table_id,
    project_id,
    file_current,
)

def make_dict(value:list):
    return {
        "Sex": value[0],
        "Age": value[1],
        "Year": value[2],
        "Month": value[3],
        "Day": value[4],
        "Street_Number": value[5],
        "Birthday": value[6],
        "Name": value[7],
        "Surname": value[8],
        "City": value[9],
        "Street": value[10],
        "State": value[11],
        "State_Voted": value[12],
    }

def run(argv=None):
    with beam.Pipeline() as pipeline:
        values = (
            pipeline
            | beam.io.ReadFromText(file_current, skip_header_lines=True)
            | beam.Map(lambda x: x.split(','))
            | beam.Filter(lambda x: int(x[1]) > 70)
            | beam.Map(lambda x: make_dict(x))
        ) 
        values | beam.io.WriteToBigQuery(
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