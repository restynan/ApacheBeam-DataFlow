package com.example.apacheBeamDemo.service.groupByKey;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;

import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class GroupByKeyExample {

    public void getCustomersGroupedByKey(){
        //GroupByKey
        Pipeline pipeline = Pipeline.create();

        //step 1
        PCollection<String> pCollectionList = pipeline.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/GroupByKey_data.csv"));

        // step 2 convert string to key value
        PCollection<KV<String,Integer>> kvOrder =  pCollectionList.apply(ParDo.of(new ConvertInputToKeyValuePair()));

        //step 3 :Apply  groupByKey and build KV<String, Iterable<Integer>>
        PCollection<KV<String,Iterable<Integer>>> kvIdOrderMapping =  kvOrder.apply(GroupByKey.<String,Integer> create());

        //Step 4: Convert KV<String, Iterable<Integer>> to String and write to a file
        PCollection<String> idSumString =  kvIdOrderMapping.apply(ParDo.of( new SumIntegerValues()));





        idSumString.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/groupByKey.csv")
                .withHeader("ID,SUM")
                .withNumShards(1)
                .withSuffix(".csv"));
        pipeline.run();
    }
}
