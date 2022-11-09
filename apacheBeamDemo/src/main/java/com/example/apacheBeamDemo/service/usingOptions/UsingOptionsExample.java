package com.example.apacheBeamDemo.service.usingOptions;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class UsingOptionsExample {
    public void usingOptions(String ...args){
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipelineFile = Pipeline.create(options);
        PCollection<String> outPut = pipelineFile.apply(TextIO.read().from(options.getInputFile()));
        outPut.apply(TextIO.write().to(options.getOutputFile())
                .withNumShards(1)
                .withSuffix(options.getExtension()));
        pipelineFile.run();
    }
}
