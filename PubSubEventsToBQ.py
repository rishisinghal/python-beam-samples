import argparse
import json
import time
import logging

from apache_beam import io, Pipeline, Map
from apache_beam.options.pipeline_options import PipelineOptions
from typing import Any, Dict, List

# class TransformData(PTransform):
#
#     def expand(self, pcoll):
#
#         return (
#             pcoll
#             | "Window into fixed intervals"
#             >> WindowInto(FixedWindows(self.window_size))
#             | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
#             # Assign a random key to each windowed element based on the number of shards.
#             | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
#             # Group windowed elements by key. All the elements in the same window must fit
#             # memory for this. If not, you need to use `beam.util.BatchElements`.
#             | "Group by key" >> GroupByKey()
#         )


# class AddTimestamp(DoFn):
#     def process(self, element, publish_time=DoFn.TimestampParam):
#         """Processes each windowed element by extracting the message body and its
#         publish time into a tuple.
#         """
#         yield (
#             element.decode("utf-8"),
#             datetime.utcfromtimestamp(float(publish_time)).strftime(
#                 "%Y-%m-%d %H:%M:%S.%f"
#             ),
#         )


# class WriteToGCS(DoFn):
#     def __init__(self, output_path):
#         self.output_path = output_path
#
#     def process(self, key_value, window=DoFn.WindowParam):
#         ts_format = "%H:%M"
#         window_start = window.start.to_utc_datetime().strftime(ts_format)
#         window_end = window.end.to_utc_datetime().strftime(ts_format)
#         shard_id, batch = key_value
#         filename = "-".join([self.output_path, window_start, window_end, str(shard_id)])
#
#         with io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
#             for message_body, publish_time in batch:
#                 f.write(f"{message_body},{publish_time}\n".encode("utf-8"))

# Defines the BigQuery schema for the output table.
# SCHEMA = ",".join(
#     [
#         "url:STRING",
#         "num_reviews:INTEGER",
#         "score:FLOAT64",
#         "event_time:TIMESTAMP",
#         "test_struct:RECORD",
#
#     ]
# )

table_schema = {
    "fields": [
        {"name": "url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "num_reviews", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "score", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "event_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "test_struct", "type": "RECORD", "mode": "NULLABLE",
         "fields": [
             {"name": "level1", "type": "STRING", "mode": "NULLABLE"},
             {"name": "level2", "type": "STRING", "mode": "NULLABLE"}
         ]},
        {"name": "repeated_struct", "type": "RECORD", "mode": "REPEATED",
         "fields": [
            {"name": "key", "type": "STRING", "mode": "NULLABLE"},
            {"name": "value", "type": "STRING", "mode": "NULLABLE"}
         ]}
    ]
}

def parse_json_message(message: str) -> Dict[str, Any]:
    """Parse the input json message and add 'score' & 'processing_time' keys."""
    row = json.loads(message)
    return {
        "url": row["url"],
        "num_reviews": row["num_reviews"],
        "score": row["score"],
        "event_time": int(time.time()),
        "test_struct": {
            "level1": "lll1",
            "level2": "lll222",
        },
        "repeated_struct": [
            {
                "key": "k1",
                "value": "v1"
            },
            {
                "key": "k2",
                "value": "v2"
            },
        ]
    }

def run(input_topic, output_bq_table, pipeline_args=None):
    pipeline_options = PipelineOptions(pipeline_args, streaming=True, save_main_session=True)

    with Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read events from Pub/Sub" >> io.ReadFromPubSub(topic=input_topic).with_output_types(bytes)
            | "UTF-8 bytes to string" >> Map(lambda msg: msg.decode("utf-8"))
            | "Window into" >> Map(parse_json_message)
            | "Write to Big Query" >> io.WriteToBigQuery(output_bq_table,
                                                         schema=table_schema,
                                                         write_disposition=io.BigQueryDisposition.WRITE_APPEND,
                                                         create_disposition=io.BigQueryDisposition.CREATE_IF_NEEDED)
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from."
        '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
    )
    parser.add_argument(
        "--output_bq_table",
        help="Path of the output BigQuery table as PROJECT:DATASET.TABLE",
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_topic,
        known_args.output_bq_table,
        pipeline_args,
    )
