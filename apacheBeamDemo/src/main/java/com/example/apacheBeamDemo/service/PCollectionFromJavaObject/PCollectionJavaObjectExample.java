package com.example.apacheBeamDemo.service.PCollectionFromJavaObject;

import com.example.apacheBeamDemo.model.Customer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PCollectionJavaObjectExample {


    public void pCollectionJavaObject(){
        Pipeline customerPipeline = Pipeline.create();
        PCollection<Customer> customerOutPut = customerPipeline.apply(Create.of(new CustomerService().getCustomers()));
        PCollection<String> customerPCollectionStringOutPut =customerOutPut.apply(MapElements.into(TypeDescriptors.strings()).via((Customer cust)->cust.getName()));
        customerPCollectionStringOutPut.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/customer_obj.csv")
                .withNumShards(1)
                .withSuffix(".csv"));

        customerPipeline.run();
    }
}
