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
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


public class OsloCityBike {

    static class ExtractFromJSON extends DoFn<String, LinkedHashMap> {

        @ProcessElement
        public void processElement(@Element String jsonElement, OutputReceiver<LinkedHashMap> receiver) {

            try {
                ObjectMapper objectMapper = new ObjectMapper();
                Map<String, ArrayList> map = objectMapper.readValue(jsonElement, new TypeReference<Map<String, Object>>() {});

                for (Object o : map.get("stations")) {
                    LinkedHashMap stationData = (LinkedHashMap) o;

                    LinkedHashMap o2 = (LinkedHashMap) stationData.get("availability");
                    stationData.put("availability_bikes", o2.get("bikes"));
                    stationData.put("availability_locks", o2.get("locks"));
                    stationData.put("availability_overflow_capacity", o2.get("overflow_capacity"));
                    stationData.remove("availability");


                    stationData.put("updated_at", map.get("updated_at"));
                    System.out.println(stationData);

                    receiver.output(stationData);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /** A SimpleFunction that converts station data into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<LinkedHashMap, String> {
        @Override
        public String apply(LinkedHashMap input) {
            return input.toString();
        }
    }

    static class FormatStationAvailabilityDataFn extends SimpleFunction<LinkedHashMap, TableRow>{

        @Override
        public TableRow apply(LinkedHashMap stationAvailability) {

            TableRow row = new TableRow()
                    .set("id", stationAvailability.get("id"))
                    .set("availability_bikes", stationAvailability.get("availability_bikes"))
                    .set("availability_locks", stationAvailability.get("availability_locks"))
                    .set("availability_overflow_capacity", stationAvailability.get("availability_overflow_capacity"))
                    .set("updated_at", stationAvailability.get("updated_at"));
            return row;
        }

        /**
         * Defines the BigQuery schema for Station Availability.
         */
        static TableSchema getSchema() {
            List<TableFieldSchema> fields = new ArrayList<>();
            fields.add(new TableFieldSchema().setName("id").setType("INTEGER"));
            fields.add(new TableFieldSchema().setName("availability_bikes").setType("INTEGER"));
            fields.add(new TableFieldSchema().setName("availability_locks").setType("INTEGER"));
            fields.add(new TableFieldSchema().setName("availability_overflow_capacity").setType("BOOL"));
            fields.add(new TableFieldSchema().setName("updated_at").setType("TIMESTAMP"));
            return new TableSchema().setFields(fields);
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     */
    public static class StationData extends PTransform<PCollection<String>, PCollection<LinkedHashMap>> {
        @Override
        public PCollection<LinkedHashMap> expand(PCollection<String> elements) {

            // Convert lines of text into individual words.
            PCollection<LinkedHashMap> stations = elements.apply(
                    ParDo.of(new ExtractFromJSON()));

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
         * By default, this example reads from a public dataset containing the text of
         * King Lear. Set this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://my_oslo_bike_data/*-availability.txt")
        //@Default.String("sykkeldata.txt")
        String getInputFile();
        void setInputFile(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Path of the file to write to")
        @Default.String("citybikes")
        @Validation.Required
        String getOutput();
        void setOutput(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Data set name")
        @Default.String("OsloCityBike")
        @Validation.Required
        String getOutputDataset();
        void setOutputDataset(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Output table name")
        @Default.String("StationAvailability")
        @Validation.Required
        String getOutputTableName();
        void setOutputTableName(String value);

        /**
         * Option to specify whether the pipeline writes files og BigQuery.
         */
        @Description("Output format for the processed data: [files|bq] ")
        @Default.String("files")
        @Validation.Required
        String getOutputFormat();
        void setOutputFormat(String value);
    }

    static void processOsloCityBikeData(OsloCityBikeOptions options) {
        Pipeline p = Pipeline.create(options);

        TableReference tableRef = new TableReference();
        tableRef.setDatasetId(options.getOutputDataset());
        tableRef.setProjectId(options.as(GcpOptions.class).getProject());
        tableRef.setTableId(options.getOutputTableName());

        if (options.getOutputFormat().equalsIgnoreCase("bq")) {
            p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                    .apply(new StationData())
                    .apply(MapElements.via(new FormatStationAvailabilityDataFn()))
                    .apply(BigQueryIO.writeTableRows().to(tableRef)
                            .withSchema(FormatStationAvailabilityDataFn.getSchema())
                            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                    );
        } else if (options.getOutputFormat().equalsIgnoreCase("files")) {
            p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                    .apply(new StationData())
                    .apply(MapElements.via(new FormatAsTextFn()))
                    .apply("WriteStationData", TextIO.write().to(options.getOutput()));
        }


        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        OsloCityBikeOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(OsloCityBikeOptions.class);

        processOsloCityBikeData(options);
    }
}


/*
export JAVA_HOME=`/usr/libexec/java_home -v 1.8`
export GOOGLE_APPLICATION_CREDENTIALS="/Users/rm/Documents/development/gcp/beam/word-count-beam/sykkeldata-creds.json"


mvn -Pdataflow-runner compile exec:java \
      -Dexec.mainClass=com.mehmandarov.beam.OsloCityBike \
      -Dexec.args="--project=rm-cx-211107 \
      --stagingLocation=gs://my_oslo_bike_data/testing/ \
      --output=gs://my_oslo_bike_data/testing/output \
      --tempLocation=gs://my_oslo_bike_data/testing/ \
      --runner=DataflowRunner \
      --region=europe-west1"

*/