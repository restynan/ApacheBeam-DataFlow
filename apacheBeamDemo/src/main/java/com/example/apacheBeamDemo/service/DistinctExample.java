package com.example.apacheBeamDemo.service;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.values.PCollection;

public class DistinctExample {
    public void  getDistinct(){
        // get distinct entries
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollectionList = pipeline.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/Distinct.csv"));

        PCollection<String> uniqueCustomers =  pCollectionList.apply(Distinct.<String>create());
        uniqueCustomers.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/unique_customers.csv")
                .withNumShards(1)
                .withSuffix(".csv"));
        pipeline.run();
    }
}
