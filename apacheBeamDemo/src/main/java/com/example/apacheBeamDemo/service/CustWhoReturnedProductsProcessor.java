package com.example.apacheBeamDemo.service;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class CustWhoReturnedProductsProcessor extends DoFn< String, KV<String,String>> {

    @ProcessElement
    public void processElement(ProcessContext c){
        String arr[] = c.element().split(",");
        c.output(KV.of(arr[0], arr[1]));
    }

}
