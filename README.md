# Sample(s) showing how to use Apache Beam with Python 

1. Set your `GOOGLE_APPLICATION_CREDENTIALS` environment variable to point to your service account key file.

   ```sh
   export GOOGLE_APPLICATION_CREDENTIALS=path/to/your/credentials.json
   ```

1. Create a Cloud Storage bucket if not aleardy exist for temp and failure records.

   ```bash
   export BUCKET_ID=your-gcs-bucket-id

   gsutil mb gs://$BUCKET_ID
   ```

## Setup

The following instructions will help you prepare your development environment.

1. Install the sample requirements.

  ```bash
  pip install -U -r requirements.txt
  ```

* [PubSubEventsToBQ.py](PubSubEventsToBQ.py)

The pipeline does the following:
1. Reads messages from a Pub/Sub topic.
1. Does the required transformation.
1. Writes the transformed elements to a BigQuery Table.

+ `--project`: sets the Google Cloud project ID to run the pipeline on
+ `--region`: sets the Dataflow [regional endpoint](https://cloud.google.com/dataflow/docs/concepts/regional-endpoints)
+ `--input_topic`: sets the input Pub/Sub topic to read messages from in projects/<PROJECT_ID>/topics/<TOPIC_ID> format
+ `--output_bq_table`: sets the output BigQuery table to write as PROJECT:DATASET.TABLE
+ `--runner`: specifies the runner to run the pipeline, if not set to `DataflowRunner`, `DirectRunner` is used
+ `--temp_location`: needed for executing the pipeline

```bash
python PubSubEventsToBQ.py \
  --project=$PROJECT_ID \
  --region=asia-southeast1 \
  --input_topic=projects/$PROJECT_ID/topics/$TOPIC_ID \
  --output_bq_table=PROJECT:DATASET.TABLE \
  --runner=DataflowRunner \
  --temp_location=gs://$BUCKET_ID/temp
```

After the job has been submitted, you can check its status in the [GCP Console Dataflow page].


[Apache Beam]: https://beam.apache.org/
[Google Cloud Pub/Sub]: https://cloud.google.com/pubsub/docs/
[Google Cloud Dataflow]: https://cloud.google.com/dataflow/docs/
[Google Cloud Scheduler]: https://cloud.google.com/scheduler/docs/
[App Engine]: https://cloud.google.com/appengine/docs/

[Cloud SDK]: https://cloud.google.com/sdk/docs/
[Cloud Shell]: https://console.cloud.google.com/cloudshell/editor/
[*New Project* page]: https://console.cloud.google.com/projectcreate
[Enable billing]: https://cloud.google.com/billing/docs/how-to/modify-project/
[*Create service account key* page]: https://console.cloud.google.com/apis/credentials/serviceaccountkey/
[GCP Console IAM page]: https://console.cloud.google.com/iam-admin/iam/
[Granting roles to service accounts]: https://cloud.google.com/iam/docs/granting-roles-to-service-accounts/
[Creating and managing service accounts]: https://cloud.google.com/iam/docs/creating-managing-service-accounts/

[Install Python and virtualenv]: https://cloud.google.com/python/setup/

[GCP Console create Dataflow job page]: https://console.cloud.google.com/dataflow/createjob/
[GCP Console Dataflow page]: https://console.cloud.google.com/dataflow/
[GCP Console Storage page]: https://console.cloud.google.com/storage/
