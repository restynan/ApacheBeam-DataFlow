package com.example.apacheBeamDemo;

import com.example.apacheBeamDemo.model.Customer;
import com.example.apacheBeamDemo.service.CustomerFilterParDo;
import com.example.apacheBeamDemo.service.CustomerService;
import com.example.apacheBeamDemo.service.UserSimpleFunction;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
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

		// Using simple Function=> map 1-M/male 2-F/female
		Pipeline pipelineFileUser = Pipeline.create();
		PCollection<String> pCollectionUserList = pipelineFileUser.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/user.csv"));

		PCollection<String> pCollectionUserUppercase = pCollectionUserList.apply(MapElements.via(new UserSimpleFunction()));
		pCollectionUserUppercase.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/data/user_output.csv")
				.withNumShards(1)
				.withSuffix(".csv"));
		pipelineFileUser.run();
*/
		//pardo  Ptransform to get customers that only belong to los angeles
		Pipeline pipelineFileParDo = Pipeline.create();
		PCollection<String> pCollectionListParDo = pipelineFileParDo.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/customer_pardo.csv"));

		PCollection<String> pCollectionLosAngeLes=  pCollectionListParDo.apply(ParDo.of(new CustomerFilterParDo()));
		pCollectionLosAngeLes.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/user_output.csv")
						.withHeader("ID,Name,Last Name,City")
				.withNumShards(1)
				.withSuffix(".csv"));
		pipelineFileParDo.run();






		System.out.println("Done");

	}
}
