package com.example.apacheBeamDemo;

import com.example.apacheBeamDemo.service.*;
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

		//pardo  Ptransform to get customers that only belong to los angeles
		Pipeline pipelineFileParDo = Pipeline.create();
		PCollection<String> pCollectionListParDo = pipelineFileParDo.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/customer_pardo.csv"));

		PCollection<String> pCollectionLosAngeLes=  pCollectionListParDo.apply(ParDo.of(new CustomerFilterParDo()));
		pCollectionLosAngeLes.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/user_output.csv")
						.withHeader("ID,Name,Last Name,City")
				.withNumShards(1)
				.withSuffix(".csv"));
		pipelineFileParDo.run();

		// how to achieve filtration with the help of Filter transform API
		Pipeline pipelineFileFilterLosAngeLes = Pipeline.create();
		PCollection<String> pCollectionListFilterLosAngeLes = pipelineFileFilterLosAngeLes.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/customer_pardo.csv"));

		PCollection<String> pCollectionFilterLosAngeLes=  pCollectionListFilterLosAngeLes.apply(Filter.by(new FilterTransformExample()));
		pCollectionFilterLosAngeLes.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/user_output.csv")
				.withHeader("ID,Name,Last Name,City")
				.withNumShards(1)
				.withSuffix(".csv"));
		pipelineFileFilterLosAngeLes.run();





		//Flatten combine multiple pcollection into a single pcollection
		Pipeline pipelineFileFlatten = Pipeline.create();
		PCollection<String> pCollectionList1 = pipelineFileFlatten.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/customer_1.csv"));
		PCollection<String> pCollectionList2 = pipelineFileFlatten.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/customer_2.csv"));
		PCollection<String> pCollectionList3 = pipelineFileFlatten.apply(TextIO.read().from("/Users/restynasimbwa/apacheBeam/data/customer_3.csv"));
		PCollectionList<String> pList = PCollectionList.of(pCollectionList1).and(pCollectionList2).and(pCollectionList3);
		PCollection<String> merged = pList.apply(Flatten.pCollections());
		merged.apply(TextIO.write().to("/Users/restynasimbwa/apacheBeam/output_data/merged_output.csv")
				.withHeader("ID,Name,Last Name,City")
				.withNumShards(1)
				.withSuffix(".csv"));
		pipelineFileFlatten.run();

		//Partion one PCollection  broken into multiple Objects
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
*/

		//Side inputs ===>Return List of customers who never returned the products
		CustomersWhoDidNotReturnProducts custObj = new CustomersWhoDidNotReturnProducts();
		custObj.customersWhoDidNotReturnProducts();
	}


}
