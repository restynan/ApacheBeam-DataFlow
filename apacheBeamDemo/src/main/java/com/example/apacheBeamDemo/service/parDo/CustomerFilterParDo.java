package com.example.apacheBeamDemo.service.parDo;

import org.apache.beam.sdk.transforms.DoFn;

public class CustomerFilterParDo  extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c){
        String line = c.element();
        String arr[] = line.split(",");

        if(arr[3].equals("Los Angeles")){
            c.output(line);
        }
    }


}
