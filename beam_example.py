import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from config import (
    schema, 
    dataset,
    table_id,
    project_id,
    file_current,
    temp_location,
    staging_location
)


pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project=project_id,
    temp_location=temp_location,
    staging_location=staging_location,
    region='us-central1'
)

with beam.Pipeline(options=pipeline_options) as pipeline:
    values = (
        pipeline
        | beam.io.ReadFromText(file_current, skip_header_lines=True)
        | beam.Map(lambda x: x.split(','))
        | beam.Filter(lambda x: int(x[1]) > 70)
    )

    values | beam.io.WriteToBigQuery(
        bigquery.TableReference(
            projectId=project_id,
            datasetId=dataset,
            tableId=table_id
        ),
        schema=schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    )