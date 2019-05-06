package com.mehmandarov.beam;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class OsloCityBike {

    static class ExtractStationAvailabilityDataFromJSON extends DoFn<String, KV<Integer, LinkedHashMap>> {

        @ProcessElement
        public void processElement(@Element String jsonElement, OutputReceiver<KV<Integer, LinkedHashMap>> receiver) {

            try {
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, ArrayList> map = objectMapper.readValue(jsonElement, new TypeReference<Map<String, Object>>() {});

                for (Object o : map.get("stations")) {
                    LinkedHashMap stationDataItem = (LinkedHashMap) o;

                    stationDataItem.put("availability_bikes", ((LinkedHashMap) stationDataItem.get("availability")).get("bikes"));
                    stationDataItem.put("availability_locks", ((LinkedHashMap) stationDataItem.get("availability")).get("locks"));
                    stationDataItem.put("availability_overflow_capacity", ((LinkedHashMap) stationDataItem.get("availability")).get("overflow_capacity"));
                    stationDataItem.remove("availability");
                    stationDataItem.put("updated_at", map.get("updated_at"));

                    receiver.output(KV.of((Integer)stationDataItem.get("id"), stationDataItem));
                }

            } catch (IOException e) {
                e.printStackTrace();
            } catch (NullPointerException e) {
                e.printStackTrace();
            }
        }
    }

    static class ExtractStationMetaDataFromJSON extends DoFn<String, KV<Integer, LinkedHashMap>> {

        private static final Logger log = LoggerFactory.getLogger(OsloCityBike.class);

        @ProcessElement
        public void processElement(@Element String jsonElement, OutputReceiver<KV<Integer, LinkedHashMap>> receiver) {

            try {
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, ArrayList> map = objectMapper.readValue(jsonElement, new TypeReference<Map<String, Object>>() {});

                for (Object o : map.get("stations"))
                    if (o != null) {
                        LinkedHashMap stationMetaDataItem = (LinkedHashMap) o;

                        // simplify the metadata object a bit
                        stationMetaDataItem.put("station_center_lat",
                                ((LinkedHashMap) stationMetaDataItem.getOrDefault("center",
                                        new LinkedHashMap<String, LinkedHashMap>())).getOrDefault("latitude", ""));
                        stationMetaDataItem.put("station_center_lon",
                                ((LinkedHashMap) stationMetaDataItem.getOrDefault("center",
                                        new LinkedHashMap<String, LinkedHashMap>())).getOrDefault("longitude", ""));
                        stationMetaDataItem.remove("center");
                        stationMetaDataItem.remove("bounds");

                        receiver.output(KV.of((Integer) stationMetaDataItem.get("id"), stationMetaDataItem));
                    }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /** A SimpleFunction that attempts to convert any objects into a printable string. */
    public static class FormatAnythingAsTextFn extends SimpleFunction<Object, String> {
        @Override
        public String apply(Object input) {
            return input.toString();
        }
    }

    /** A SimpleFunction that converts station data into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<LinkedHashMap, String> {
        @Override
        public String apply(LinkedHashMap input) {
            return input.toString();
        }
    }

    /** A SimpleFunction that converts station data into an object that can be stored in BigQuery. */
    static class FormatMergedStationDataToBigQueryFn extends SimpleFunction<LinkedHashMap, TableRow>{

        @Override
        public TableRow apply(LinkedHashMap mergedStationData) {
            TableRow row = new TableRow()
                    .set("id", mergedStationData.get("id"))
                    .set("in_service", mergedStationData.get("in_service"))
                    .set("title", mergedStationData.get("title"))
                    .set("subtitle", mergedStationData.get("subtitle"))
                    .set("number_of_locks", mergedStationData.get("number_of_locks"))
                    .set("station_center_lat", mergedStationData.get("station_center_lat"))
                    .set("station_center_lon", mergedStationData.get("station_center_lon"))
                    .set("availability_bikes", mergedStationData.get("availability_bikes"))
                    .set("availability_locks", mergedStationData.get("availability_locks"))
                    .set("availability_overflow_capacity", mergedStationData.get("availability_overflow_capacity"))
                    .set("updated_at", mergedStationData.get("updated_at"));
            return row;
        }

        /** Defines the BigQuery schema for station availability. */
        static TableSchema getSchema() {
            List<TableFieldSchema> fields = new ArrayList<>();
            fields.add(new TableFieldSchema().setName("id").setType("INTEGER").setMode("REQUIRED"));
            fields.add(new TableFieldSchema().setName("in_service").setType("BOOL"));
            fields.add(new TableFieldSchema().setName("title").setType("STRING"));
            fields.add(new TableFieldSchema().setName("subtitle").setType("STRING"));
            fields.add(new TableFieldSchema().setName("number_of_locks").setType("INTEGER"));
            fields.add(new TableFieldSchema().setName("station_center_lat").setType("STRING"));
            fields.add(new TableFieldSchema().setName("station_center_lon").setType("STRING"));
            fields.add(new TableFieldSchema().setName("availability_bikes").setType("INTEGER"));
            fields.add(new TableFieldSchema().setName("availability_locks").setType("INTEGER"));
            fields.add(new TableFieldSchema().setName("availability_overflow_capacity").setType("BOOL"));
            fields.add(new TableFieldSchema().setName("updated_at").setType("TIMESTAMP").setMode("REQUIRED"));
            return new TableSchema().setFields(fields);
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * LinkedHashMap with station availability data.
     */
    public static class StationAvailabilityData extends PTransform<PCollection<String>, PCollection<KV<Integer, LinkedHashMap>>> {
        @Override
        public PCollection<KV<Integer, LinkedHashMap>> expand(PCollection<String> elements) {

            // Convert lines of text into LinkedHashMap.
            PCollection<KV<Integer, LinkedHashMap>> stations = elements.apply(
                    ParDo.of(new ExtractStationAvailabilityDataFromJSON()));

            return stations;
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * LinkedHashMap with station availability data.
     */
    public static class StationMetadata extends PTransform<PCollection<String>, PCollection<KV<Integer, LinkedHashMap>>> {
        @Override
        public PCollection<KV<Integer, LinkedHashMap>> expand(PCollection<String> elements) {

            // Convert lines of text into LinkedHashMap.
            PCollection<KV<Integer, LinkedHashMap>> stations = elements.apply(
                    ParDo.of(new ExtractStationMetaDataFromJSON()));

            return stations;
        }
    }

    /**
     * Options supported by {@link com.mehmandarov.beam.OsloCityBike}.
     *
     * <p> Defining your own configuration options. Here, you can add your own arguments
     * to be processed by the command-line parser, and specify default values for them. You can then
     * access the options values in your pipeline code.
     *
     * <p>Inherits standard configuration options.
     */
    public interface OsloCityBikeOptions extends PipelineOptions {

        /**
         * By default, the code reads from a public dataset containing a subset of
         * availability data for city bikes. Set this option to choose a different input file or glob
         * (i.e. partial names with *, like "*-availability.txt").
         */
        @Description("Path of the file with the availability data")
        //@Default.String("gs://my_oslo_bike_data/*-availability.txt")
        //@Default.String("src/main/resources/bikedata-availability-example.txt")
        @Default.String("src/main/resources/2018-07-30-data/2018-07-30-12*-availability.txt")
        String getAvailabilityInputFile();
        void setAvailabilityInputFile(String value);

        /**
         * By default, the code reads from a public dataset containing a subset of
         * bike station metadata for city bikes. Set this option to choose a different input file or glob
         * (i.e. partial names with *, like "*-stations.txt").
         */
        @Description("Path of the file with the availability data")
        //@Default.String("gs://my_oslo_bike_data/*-stations.txt")
        //@Default.String("src/main/resources/bikedata-stations-example.txt")
        @Default.String("src/main/resources/2018-07-30-data/2018-07-30-12*-stations.txt")
        String getStationMetadataInputFile();
        void setStationMetadataInputFile(String value);

        /**
         * Option to specify whether the pipeline writes files og BigQuery.
         */
        @Description("Output format for the processed data: [files|bq] ")
        @Default.String("files")
        @Validation.Required
        String getOutputFormat();
        void setOutputFormat(String value);

        /**
         * Set this required option to specify where to write the output for station availability data.
         */
        @Description("Path of the file containing station availability data")
        @Default.String("citybikes-stations-availability")
        @Validation.Required
        String getStationOutput();
        void setStationOutput(String value);

        /**
         * Set this required option to specify where to write the output for station metadata.
         */
        @Description("Path of the file containing station metadata")
        @Default.String("citybikes-stations-metadata")
        @Validation.Required
        String getMetadataOutput();
        void setMetadataOutput(String value);

        /**
         * Set this required option to specify where to write the output for
         * joined station metadata and availability data.
         */
        @Description("Path of the file containing joined station metadata and availability data")
        @Default.String("citybikes-stations-and-availability-data")
        @Validation.Required
        String getJoinedMetadataAndAvailabilityOutput();
        void setJoinedMetadataAndAvailabilityOutput(String value);

        /**
         * Set this required option to specify table name for joined station metadata and availability data in BigQuery.
         */
        @Description("Output table name in BigQuery")
        @Default.String("StationDataAndAvailability")
        @Validation.Required
        String getOutputTableName();
        void setOutputTableName(String value);

        /**
         * Set this required option to specify table name for availability data in BigQuery.
         */
        @Description("Dataset name")
        @Default.String("OsloCityBike")
        @Validation.Required
        String getOutputDataset();
        void setOutputDataset(String value);

        /**
         * Set this required option to specify table name for joined station metadata and availability data in BigQuery.
         */
        @Description("Output bucket when running a DataFlow job on GCP")
        @Default.String("gs://my_oslo_bike_data/testing/output")
        @Validation.Required
        String getOutput();
        void setOutput(String value);
    }

    static void processOsloCityBikeData(OsloCityBikeOptions options) {
        // Create a pipeline for station meta data
        Pipeline pipeline = Pipeline.create(options);

        PCollection <KV<Integer, LinkedHashMap>> stationMetadata = pipeline
                .apply("ReadLines: StationMetadataInputFiles", TextIO.read().from(options.getStationMetadataInputFile()))
                .apply(new StationMetadata());

        PCollection <KV<Integer, LinkedHashMap>> availabilityData = pipeline
                .apply("ReadLines: AvailabilityInputFiles", TextIO.read().from(options.getAvailabilityInputFile()))
                .apply(new StationAvailabilityData());

        final TupleTag<LinkedHashMap> metadataIdTag = new TupleTag<LinkedHashMap>(){};
        final TupleTag<LinkedHashMap> availabilityIdTag = new TupleTag<LinkedHashMap>(){};

        // Merge collection values into a CoGbkResult collection.
        PCollection<KV<Integer, CoGbkResult>> joinedCollection = KeyedPCollectionTuple.of(metadataIdTag, stationMetadata)
                                                                    .and(availabilityIdTag, availabilityData)
                                                                    .apply(CoGroupByKey.<Integer>create());

        // Process final results - each element in joinedCollection contains two lists.
        // We need to merge these two lists together.
        PCollection<LinkedHashMap> finalResultCollection =
                joinedCollection.apply(ParDo.of(
                        new  DoFn<KV<Integer, CoGbkResult>, LinkedHashMap>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                KV<Integer, CoGbkResult> e = c.element();

                                Iterable<LinkedHashMap> pt1Vals = e.getValue().getAll(availabilityIdTag);
                                for (LinkedHashMap pt1Val : pt1Vals) {
                                    LinkedHashMap<String, String> mergedMap = initMap();

                                    LinkedHashMap pt2Val = new LinkedHashMap<String, String>();
                                    if ((e.getValue().getAll(metadataIdTag)).iterator().hasNext()) {
                                        pt2Val = (e.getValue().getAll(metadataIdTag)).iterator().next();
                                    }
                                    mergedMap.putAll(pt1Val);
                                    mergedMap.putAll(pt2Val);
                                    c.output(mergedMap);
                                }
                            }

                            // Should be replaced with a proper object.
                            // Ok(-ish) for a demo, not OK in production code... :-)
                            private LinkedHashMap<String, String> initMap(){
                                LinkedHashMap<String,String> myMap = new LinkedHashMap<>();
                                myMap.put("id", "");
                                myMap.put("in_service", "");
                                myMap.put("title", "");
                                myMap.put("subtitle", "");
                                myMap.put("number_of_locks", "");
                                myMap.put("station_center_lat", "");
                                myMap.put("station_center_lon", "");
                                myMap.put("availability_bikes", "");
                                myMap.put("availability_locks", "");
                                myMap.put("availability_overflow_capacity", "");
                                myMap.put("updated_at", "");
                                return myMap;
                            }
                        }
                ));

        if (options.getOutputFormat().equalsIgnoreCase("bq")) {
            // Needed for BigQuery
            TableReference tableRef = new TableReference();
            tableRef.setDatasetId(options.getOutputDataset());
            tableRef.setProjectId(options.as(GcpOptions.class).getProject());
            tableRef.setTableId(options.getOutputTableName());

            finalResultCollection.apply(MapElements.via(new FormatMergedStationDataToBigQueryFn()))
                    .apply("WriteJoinedData to BigQuery", BigQueryIO.writeTableRows().to(tableRef)
                            .withSchema(FormatMergedStationDataToBigQueryFn.getSchema())
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    );
        } else if (options.getOutputFormat().equalsIgnoreCase("files")) {

            //availabilityData.apply(MapElements.via(new FormatAnythingAsTextFn()))
            //        .apply("WriteStationData", TextIO.write().to(options.getMetadataOutput()));
            // RETURNS:
            // KV{166, {id=166, availability_bikes=6, availability_locks=12, availability_overflow_capacity=false, updated_at=2018-08-01T14:35:01+00:00}}
            // ---

            //stationMetadata.apply(MapElements.via(new FormatAnythingAsTextFn()))
            //        .apply("WriteStationMetaData", TextIO.write().to(options.getMetadataOutput()));
            // RETURNS:
            // KV{157, {id=157, in_service=true, title=Nylandsveien, subtitle=mellom Norbygata og Urtegata, number_of_locks=30, station_center_lat=59.91562, station_center_lon=10.762248}}
            // ---

            //joinedCollection.apply(MapElements.via(new FormatAnythingAsTextFn()))
            //        .apply("WriteJoinedData", TextIO.write().to("tmp-JOINED-data.txt"));

            finalResultCollection.apply(MapElements.via(new FormatAsTextFn()))
                    .apply("WriteJoinedData", TextIO.write().to(options.getJoinedMetadataAndAvailabilityOutput()));
        }

        pipeline.run().waitUntilFinish();

    }

    public static void main(String[] args) {
        OsloCityBikeOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(OsloCityBikeOptions.class);

        processOsloCityBikeData(options);
    }
}


/*
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`
export GOOGLE_APPLICATION_CREDENTIALS="/Users/rm/Documents/development/gcp/beam/oslocitybike-beam/sykkeldata-creds.json"


mvn -Pdataflow-runner compile exec:java \
      -Dexec.mainClass=com.mehmandarov.beam.OsloCityBike \
      -Dexec.args="--project=rm-cx-211107 \
      --stagingLocation=gs://my_oslo_bike_data/testing/ \
      --tempLocation=gs://my_oslo_bike_data/testing/ \
      --output=gs://my_oslo_bike_data/testing/output \
      --outputFormat=bq \
      --availabilityInputFile=gs://my_oslo_bike_data/*-stations.txt \
      --stationMetadataInputFile=gs://my_oslo_bike_data/*-availability.txt \
      --runner=DataflowRunner \
      --region=europe-west1"


mvn -Pdataflow-runner compile exec:java \
      -Dexec.mainClass=com.mehmandarov.beam.OsloCityBike \
      -Dexec.args="--project=rm-cx-211107 \
      --stagingLocation=gs://my_oslo_bike_data/testing/ \
      --tempLocation=gs://my_oslo_bike_data/testing/ \
      --output=gs://my_oslo_bike_data/testing/output \
      --outputFormat=bq \
      --availabilityInputFile=src/main/resources/2018-08-data/2018-08-01-*-stations.txt \
      --stationMetadataInputFile=src/main/resources/2018-08-data/2018-08-01-*-availability.txt \
      --runner=DataflowRunner \
      --region=europe-west1"


mvn compile exec:java \
      -Dexec.mainClass=com.mehmandarov.beam.OsloCityBike \
      -Pdirect-runner
      --availabilityInputFile=gs://my_oslo_bike_data/*-stations.txt \
      --stationMetadataInputFile=gs://my_oslo_bike_data/*-availability.txt \


mvn compile exec:java \
      -Dexec.mainClass=com.mehmandarov.beam.OsloCityBike \
      -Dexec.args=" \
        --availabilityInputFile=src/main/resources/2018-08-data/2018-08-01-*-availability.txt
        --stationMetadataInputFile=src/main/resources/2018-08-data/2018-08-01-*-stations.txt
      " \

      -Pdirect-runner


mvn compile exec:java \
      -Dexec.mainClass=com.mehmandarov.beam.OsloCityBike \
      -Dexec.args="--availabilityInputFile=src/main/resources/bikedata-availability-example.txt --output=bikedatalocal" \
      -Pdirect-runner

 */

