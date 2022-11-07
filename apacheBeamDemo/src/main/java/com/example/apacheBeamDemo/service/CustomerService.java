package com.example.apacheBeamDemo.service;

import com.example.apacheBeamDemo.model.Customer;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CustomerService {

    public List<Customer> getCustomers(){
       Customer customer1 = Customer.builder().id(101).name("John").build();
       Customer customer2 = Customer.builder().id(102).name("Peter").build();
        Customer customer3 = Customer.builder().id(103).name("Kevin").build();
        return List.of(customer1, customer2, customer3);
    }

}
