package com.example.apacheBeamDemo.service;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlattenExample {
    public void flatten(){
        //Flatten combine multiple pcollection into a single pcollection
        Pipeline pipelineFileFlatten = Pipeline.create();
        PCollection<String> pCollectionList1 = pipelineFileFlatten.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/customer_1.csv"));
        PCollection<String> pCollectionList2 = pipelineFileFlatten.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/customer_2.csv"));
        PCollection<String> pCollectionList3 = pipelineFileFlatten.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/customer_3.csv"));
        PCollectionList<String> pList = PCollectionList.of(pCollectionList1).and(pCollectionList2).and(pCollectionList3);
        PCollection<String> merged = pList.apply(Flatten.pCollections());
        merged.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/merged_output.csv")
                .withHeader("ID,Name,Last Name,City")
                .withNumShards(1)
                .withSuffix(".csv"));
        pipelineFileFlatten.run();
    }
}
