package com.example.apacheBeamDemo.service.groupByKey;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class SumIntegerValues  extends DoFn<KV<String, Iterable<Integer>>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
        String key = c.element().getKey();
        Iterable<Integer> values = c.element().getValue();
        Integer sum = 0;
        for(Integer item : values){
            sum += item;
        }
        c.output(key+","+sum);

    }
}
