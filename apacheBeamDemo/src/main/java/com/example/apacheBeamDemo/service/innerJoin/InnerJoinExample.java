package com.example.apacheBeamDemo.service.innerJoin;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.io.Serializable;

public class InnerJoinExample implements Serializable {
    public void usingInnerJoin(){
        // count entries
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pUserOrderList = pipeline.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/user_order.csv"));
        PCollection<String> pUserNameList = pipeline.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/user_name.csv"));

        //1. convert to Key value pairs
        PCollection<KV<String,String>> kvUserOrder  = pUserOrderList.apply( ParDo.of(new UserOrderToKeyValPair()));
        PCollection<KV<String,String>> kvUserName  = pUserNameList.apply( ParDo.of(new UserNameToKeyValPair()));

        //create TupleTag object
        TupleTag<String> userOrderTuple = new TupleTag<>();
        TupleTag<String> userNameTuple = new TupleTag<>();

        //combine data sets using GoGroup
        PCollection<KV<String, CoGbkResult>> result = KeyedPCollectionTuple.of(userOrderTuple,kvUserOrder )
                        .and(userNameTuple,kvUserName )
                        .apply(CoGroupByKey.<String>create());

        //iterate the CoGbkResult result and build a String

        PCollection<String> output = result.apply(ParDo.of( new DoFn<KV<String,CoGbkResult>,String>(){
            @ProcessElement
            public void processElement(ProcessContext c){
                String key = c.element().getKey();
                CoGbkResult valueObject = c.element().getValue();

                Iterable<String> userOrderTable = valueObject.getAll(userOrderTuple);
                Iterable<String> userNameTable = valueObject.getAll(userNameTuple);

                for(String order : userOrderTable){
                    for(String user :userNameTable){
                        c.output(key+","+order+","+user);
                    }
                }
            }
        }


        ));
        output.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/inner_join_output.csv")
                .withNumShards(1)
                .withSuffix(".csv"));


        pipeline.run();

    }
}
