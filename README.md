# Beam Oslo City Bike Project
Beam project using public Oslo City Bike data

## Running the code

### Software Requirements

* Java 8 (Java 9 or above are not yet officially supported by Beam)
* Maven 3.5.x

### Running on your local machine
```bash
mvn compile exec:java \
      -Dexec.mainClass=com.mehmandarov.beam.OsloCityBike \
      -Dexec.args="--availabilityInputFile=src/main/resources/bikedata-availability-example.txt --output=bikedatalocal" \
      -Pdirect-runner
```

### Running on Google Cloud Platform

First you will need to create and download a GCP [credentials file][1]. 
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/example/path/to/your/file/sykkeldata-creds.json"
```

To run the code, use the following example. Make sure to update `--project`, `--stagingLocation`, `--output`, and `--tempLocation`. 
You will also need to make sure that all the Google Storage buckets are in the same region (EU, US, etc.) .

```bash
mvn -Pdataflow-runner compile exec:java \
      -Dexec.mainClass=com.mehmandarov.beam.OsloCityBike \
      -Dexec.args="--project=rm-cx-211107 \
      --stagingLocation=gs://my_oslo_bike_data/testing/ \
      --output=gs://my_oslo_bike_data/testing/output \
      --tempLocation=gs://my_oslo_bike_data/testing/ \
      --runner=DataflowRunner \
      --region=europe-west1"
```


[1]:https://cloud.google.com/dataflow/docs/quickstarts/quickstart-java-maven