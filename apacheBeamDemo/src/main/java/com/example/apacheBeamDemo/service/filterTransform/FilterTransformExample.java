package com.example.apacheBeamDemo.service.filterTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;

public class FilterTransformExample {
    public void filterTransform(){
        Pipeline pipelineFileFilterLosAngeLes = Pipeline.create();
        PCollection<String> pCollectionListFilterLosAngeLes = pipelineFileFilterLosAngeLes.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/customer.csv"));

        PCollection<String> pCollectionFilterLosAngeLes=  pCollectionListFilterLosAngeLes.apply(Filter.by(new FilterTransformProcessor()));
        pCollectionFilterLosAngeLes.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/user_output_filter.csv")
                .withHeader("ID,Name,Last Name,City")
                .withNumShards(1)
                .withSuffix(".csv"));
        pipelineFileFilterLosAngeLes.run();

    }
}
