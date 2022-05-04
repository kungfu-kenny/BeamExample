import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from config import file_current


with beam.Pipeline() as pipeline:
    values = (
        pipeline
        | beam.io.ReadFromText(file_current, skip_header_lines=True)
        | beam.Map(lambda x: x.split(','))
        | beam.Filter(lambda x: int(x[1]) > 70)
        | beam.Map(print)
    )

    # values | beam.io.gcp.bigquery.WriteToBigQuery(
    #     bigquery.TableReference(
    #         projectId='',
    #         datasetId='',
    #         tableId=''
    #     ),
    #     schema='',
    #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    # )
