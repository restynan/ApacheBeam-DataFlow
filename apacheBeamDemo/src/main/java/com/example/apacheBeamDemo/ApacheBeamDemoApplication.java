package com.example.apacheBeamDemo;

import com.example.apacheBeamDemo.model.Customer;
import com.example.apacheBeamDemo.service.CustomerService;
import com.example.apacheBeamDemo.service.MyOptions;
import com.example.apacheBeamDemo.service.User;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
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

	/*	//Creating a PCollection from a file system
		MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
		Pipeline pipelineFile = Pipeline.create(options);
		PCollection<String> outPut = pipelineFile.apply(TextIO.read().from(options.getInputFile()));
		outPut.apply(TextIO.write().to(options.getOutputFile())
									.withNumShards(1)
									.withSuffix(options.getExtension()));
		pipelineFile.run();
*/
		//Creating a PCollection from a Java Object

		Pipeline customerPipeline = Pipeline.create();
		PCollection<Customer> customerOutPut = customerPipeline.apply(Create.of(customerService.getCustomers()));
		PCollection<String> customerPCollectionStringOutPut =customerOutPut.apply(MapElements.into(TypeDescriptors.strings()).via((Customer cust)->cust.getName()));
		customerPCollectionStringOutPut.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/data/customer.csv")
				.withNumShards(1)
				.withSuffix(".csv"));

		customerPipeline.run();

		//using TypeDescriptors
		Pipeline pipelineFile = Pipeline.create();
		PCollection<String> pCollectionNamesList = pipelineFile.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/lowerCaseNames.csv"));

		PCollection<String> pCollectionNamesUppercase = pCollectionNamesList.apply(MapElements.into(TypeDescriptors.strings()).via((String name) -> name.toUpperCase()));
		pCollectionNamesUppercase.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/data/names_uppercase.csv")
				.withNumShards(1)
				.withSuffix(".csv"));
		pipelineFile.run();

		// Using simple Function
		Pipeline pipelineFileUser = Pipeline.create();
		PCollection<String> pCollectionUserList = pipelineFileUser.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/user.csv"));

		PCollection<String> pCollectionUserUppercase = pCollectionUserList.apply(MapElements.via(new User()));
		pCollectionUserUppercase.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/data/user_output.csv")
				.withNumShards(1)
				.withSuffix(".csv"));
		pipelineFileUser.run();







		System.out.println("Done");

	}
}
