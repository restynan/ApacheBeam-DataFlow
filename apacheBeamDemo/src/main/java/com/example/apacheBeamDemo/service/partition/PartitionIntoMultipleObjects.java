package com.example.apacheBeamDemo.service.partition;

import com.example.apacheBeamDemo.service.partition.MyCityPartition;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class PartitionIntoMultipleObjects {
    public void  partition(){
        // All elements that contain Los Angeles should be mapped into another Pcollection and elements with Phoenix into another

        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollectionList = pipeline.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/Partition.csv"));

        PCollectionList<String> partition = pCollectionList.apply(Partition.of(3, new MyCityPartition()));
        PCollection<String> partition_0 = partition.get(0);
        PCollection<String> partition_1 = partition.get(1);
        PCollection<String> partition_2 = partition.get(2);

        partition_0.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/p0.csv")
                .withNumShards(1)
                .withSuffix(".csv"));
        partition_1.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/p1.csv")
                .withNumShards(1)
                .withSuffix(".csv"));
        partition_2.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/p2.csv")
                .withNumShards(1)
                .withSuffix(".csv"));

        pipeline.run();

    }
}
