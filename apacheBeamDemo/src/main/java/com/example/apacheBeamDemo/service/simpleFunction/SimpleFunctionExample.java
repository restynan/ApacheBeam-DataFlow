package com.example.apacheBeamDemo.service.simpleFunction;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;

public class SimpleFunctionExample {
    public void simpleFunction(){
        // Using simple Function=> map 1-M/male 2-F/female
        Pipeline pipelineFileUser = Pipeline.create();
        PCollection<String> pCollectionUserList = pipelineFileUser.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/user.csv"));

        PCollection<String> pCollectionUserUppercase = pCollectionUserList.apply(MapElements.via(new UserSimpleFunctionProcessor()));
        pCollectionUserUppercase.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/user_output_simple_fn.csv")
                .withNumShards(1)
                .withSuffix(".csv"));
        pipelineFileUser.run();
    }
}
