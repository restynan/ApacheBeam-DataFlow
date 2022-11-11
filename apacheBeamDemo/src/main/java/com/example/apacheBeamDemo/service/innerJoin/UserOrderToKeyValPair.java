package com.example.apacheBeamDemo.service.innerJoin;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class UserOrderToKeyValPair extends DoFn<String, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext c){
        String input =  c.element();
        String [] arr = input.split(",");
        String key = arr[0];
        String value = arr[1]+","+arr[2]+","+arr[3];
        c.output(KV.of(key, value));
    }
}
