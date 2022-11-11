package com.example.apacheBeamDemo.service.groupByKey;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ConvertInputToKeyValuePair extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext c){
        String input =  c.element();
        String [] inputArray = input.split(",");
        c.output(KV.of(inputArray[0], Integer.valueOf(inputArray[3])));
    }
}
