package com.example.apacheBeamDemo;

import com.example.apacheBeamDemo.service.CountExample;
import com.example.apacheBeamDemo.service.DistinctExample;
import com.example.apacheBeamDemo.service.groupByKey.GroupByKeyExample;
import com.example.apacheBeamDemo.service.innerJoin.InnerJoinExample;
import com.example.apacheBeamDemo.service.usingOptions.MyOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApacheBeamDemoApplication implements CommandLineRunner {

	public static void main(String[] args) {

		SpringApplication.run(ApacheBeamDemoApplication.class, args);

	}

	@Override
	public void run(String... args) throws Exception {
/*
	//Creating a PCollection from a file system

		Pipeline pipelineFile = Pipeline.create();
		PCollection<String> outPut = pipelineFile.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/user.csv"));
		outPut.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/user_output_1.csv")
									.withNumShards(1)
									.withSuffix(".csv"));
		pipelineFile.run();



		//Creating a PCollection from a Java Object
		PCollectionJavaObjectExample pObject = new PCollectionJavaObjectExample();
		pObject.pCollectionJavaObject();


		//using TypeDescriptors convert names to upper case
		TypeDescriptorsExample typeDescriptorsExample = new TypeDescriptorsExample();
		typeDescriptorsExample.usingTypeDescriptors();

		// Using simple Function=> map 1-M/male 2-F/female
		SimpleFunctionExample simpleObject = new SimpleFunctionExample();
		simpleObject.simpleFunction();

		//pardo  Ptransform to get customers that only belong to los angeles
		ParDoExample parDoObj = new ParDoExample();
		parDoObj.usingParDo();

		// how to achieve filtration with the help of Filter transform API
		FilterTransformExample filterObj = new FilterTransformExample();
		filterObj.filterTransform();

		//Flatten combine multiple pcollection into a single pcollection
		FlattenExample flattenObj = new FlattenExample();
		flattenObj.flatten();


		//Partition one PCollection  broken into multiple Objects
		PartitionIntoMultipleObjects partitionObj = new PartitionIntoMultipleObjects();
		partitionObj.partition();



		//Side inputs ===>Return List of customers who never returned the products
		SideInputsExample custObj = new SideInputsExample();
		custObj.customersWhoDidNotReturnProducts();

		// Aggregation transformation
		//use Distinct to return unique customers
		DistinctExample distinctExample = new DistinctExample();
		distinctExample.getDistinct();


		// How to count PCollection
		CountExample countExample = new CountExample();
		countExample.getCount();

		// Group by key -> group values associated with a particular key
		GroupByKeyExample groupByKeyExample = new GroupByKeyExample();
		groupByKeyExample.getCustomersGroupedByKey();
     */


		InnerJoinExample innerJoinExample = new InnerJoinExample();
		innerJoinExample.usingInnerJoin();



	}


}
