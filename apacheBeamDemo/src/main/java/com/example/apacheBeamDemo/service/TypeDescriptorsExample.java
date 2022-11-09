package com.example.apacheBeamDemo.service;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class TypeDescriptorsExample {
    public void usingTypeDescriptors(){

        // convert names to upper case
        Pipeline pipelineFile = Pipeline.create();
        PCollection<String> pCollectionNamesList = pipelineFile.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/lowerCaseNames.csv"));

        PCollection<String> pCollectionNamesUppercase = pCollectionNamesList.apply(MapElements.into(TypeDescriptors.strings()).via((String name) -> name.toUpperCase()));
        pCollectionNamesUppercase.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/names_uppercase.csv")
                .withNumShards(1)
                .withSuffix(".csv"));
        pipelineFile.run();

    }
}
