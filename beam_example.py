import logging
import apache_beam as beam
from apache_beam.dataframe import convert
from apache_beam.io.gcp.internal.clients import bigquery
from config import (
    schema, 
    dataset,
    table_id,
    project_id,
    file_current,
)


def run(argv=None):
    with beam.Pipeline() as pipeline:
        values_new = pipeline | beam.dataframe.io.read_csv(file_current)
        values_send = (   
            convert.to_pcollection(values_new)
            | beam.Map(lambda x: dict(x._asdict()))
            | beam.Filter(lambda x: x["Age"] > 70)
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