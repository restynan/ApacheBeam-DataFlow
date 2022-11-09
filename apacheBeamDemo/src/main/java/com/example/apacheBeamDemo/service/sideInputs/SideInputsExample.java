package com.example.apacheBeamDemo.service.sideInputs;

import com.example.apacheBeamDemo.service.sideInputs.CustWhoReturnedProductsProcessor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.io.Serializable;
import java.util.Map;


public class SideInputsExample implements Serializable {

    public void customersWhoDidNotReturnProducts() {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String, String>> pReturn = pipeline.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/return.csv"))
                .apply(ParDo.of(new CustWhoReturnedProductsProcessor()));

        // create map
        PCollectionView<Map<String, String>> pReturnedProductsMap = pReturn.apply(View.asMap());

        PCollection<String> pCustomerList = pipeline.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/cust_order.csv"));

        PCollection<String> pCustomer = pCustomerList.apply(ParDo.of(
                new DoFn<String, String>() {
                    @ProcessElement
                    public void process(ProcessContext c) {
                        Map<String, String> pSideInputView = c.sideInput(pReturnedProductsMap);
                        String arr[] = c.element().split(",");
                        String customerName = pSideInputView.get(arr[0]);
                       // System.out.println(customerName);

                        //if customer name is missing in the map/ equal to null then those customers didnot return the product

                        if(customerName ==null){
                            System.out.println(c.element());
                            c.output(c.element());
                        }

                    }
                }).withSideInputs(pReturnedProductsMap));
        pCustomer.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/Cust_Did_Not_Return.csv")
                .withNumShards(1)
                .withSuffix(".csv"));

        pipeline.run();
    }
}
