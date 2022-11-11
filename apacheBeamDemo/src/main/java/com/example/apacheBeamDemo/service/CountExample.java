package com.example.apacheBeamDemo.service;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

public class CountExample implements Serializable {
    public void getCount(){
        // count entries
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollectionList = pipeline.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/Count.csv"));

        PCollection<Long> pCustomerCount =  pCollectionList.apply(Count.globally());
        pCustomerCount.apply( ParDo.of( new DoFn<Long,Void>() {
                                           @ProcessElement
                                           public void processElement(ProcessContext c) {
                                               System.out.println("Count : " + c.element());

                                           }
                                       }
                )
        );
        pipeline.run();
    }
}
