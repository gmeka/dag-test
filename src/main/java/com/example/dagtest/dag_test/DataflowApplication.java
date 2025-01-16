package com.example.dagtest.dag_test;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@SpringBootApplication
public class DataflowApplication {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: java -jar beam-job.jar --inputFile=<path_to_input_file> --outputFile=<path_to_output_file>");
            System.exit(1);
        }

        // Define pipeline options
        FileOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(FileOptions.class);

        // Create the pipeline
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("ReadFile", TextIO.read().from(options.getInputFile()))
                .apply("CalculateLength", MapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> {
                            long length = line.length();
                            if (length > 100) {
                                return length + " Too large";
                            }
                            return String.valueOf(length);
                        }))
                .apply("WriteToFile", TextIO.write().to(options.getOutputFile()).withSuffix(".txt").withoutSharding());

        // Run the pipeline
        pipeline.run().waitUntilFinish();
    }

    // Define custom options with annotations for validation
    public interface FileOptions extends PipelineOptions {
        @Validation.Required
        @Default.String("input.txt")
        String getInputFile();
        void setInputFile(String value);

        @Validation.Required
        @Default.String("out")
        String getOutputFile();
        void setOutputFile(String value);
    }
}