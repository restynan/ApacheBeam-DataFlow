package com.example.apacheBeamDemo.service.parDo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class ParDoExample {
    public void usingParDo(){
        //pardo  Ptransform to get customers that only belong to los angeles
        Pipeline pipelineFileParDo = Pipeline.create();
        PCollection<String> pCollectionListParDo = pipelineFileParDo.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/customer.csv"));

        PCollection<String> pCollectionLosAngeLes=  pCollectionListParDo.apply(ParDo.of(new CustomerFilterParDo()));
        pCollectionLosAngeLes.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/user_output_pardo.csv")
                .withHeader("ID,Name,Last Name,City")
                .withNumShards(1)
                .withSuffix(".csv"));
        pipelineFileParDo.run();
    }
}
