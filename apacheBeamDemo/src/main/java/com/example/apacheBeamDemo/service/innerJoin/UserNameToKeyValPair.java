package com.example.apacheBeamDemo.service.innerJoin;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class UserNameToKeyValPair extends DoFn<String, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext c){
        String input =  c.element();
        String [] inputArray = input.split(",");
        c.output(KV.of(inputArray[0], inputArray[1]));
    }

}
