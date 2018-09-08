# Beam Oslo City Bike Project
Beam project using public Oslo City Bike data

## Running the code

### Running on your local machine
```bash
mvn compile exec:java \
      -Dexec.mainClass=com.mehmandarov.beam.OsloCityBike \
      -Dexec.args="--inputFile=bikedata-example.txt --output=counts" \
      -Pdirect-runner
```

### Running on Google Cloud Platform

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
