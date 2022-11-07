package com.example.apacheBeamDemo;

import com.example.apacheBeamDemo.model.Customer;
import com.example.apacheBeamDemo.service.CustomerService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApacheBeamDemoApplication implements CommandLineRunner {
	@Autowired
	CustomerService customerService;

	public static void main(String[] args) {
		SpringApplication.run(ApacheBeamDemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		//Creating a PCollection from a file system
		Pipeline pipelineFile = Pipeline.create();
		PCollection<String> outPut = pipelineFile.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/input.csv"));
		outPut.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/data/output.csv")
									.withNumShards(1)
									.withSuffix(".csv"));
		pipelineFile.run();
        
		//Creating a PCollection from a Java Object
		Pipeline customerPipeline = Pipeline.create();
		PCollection<Customer> customerOutPut = customerPipeline.apply(Create.of(customerService.getCustomers()));
		PCollection<String> customerPCollectionStringOutPut =customerOutPut.apply(MapElements.into(TypeDescriptors.strings()).via((Customer cust)->cust.getName()));
		customerPCollectionStringOutPut.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/data/customer.csv")
				.withNumShards(1)
				.withSuffix(".csv"));

		customerPipeline.run();








		System.out.println("Done");

	}
}
