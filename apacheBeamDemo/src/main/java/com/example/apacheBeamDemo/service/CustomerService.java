package com.example.apacheBeamDemo.service;

import com.example.apacheBeamDemo.model.Customer;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class CustomerService {

    public List<Customer> getCustomers(){
       Customer customer1 = Customer.builder().id(101).name("John").build();
       Customer customer2 = Customer.builder().id(102).name("Peter").build();
        Customer customer3 = Customer.builder().id(103).name("Kevin").build();
        List <Customer> customerList =new ArrayList<>();
        customerList.add(customer1);
        customerList.add(customer2);
        customerList.add(customer3);
        return customerList;
    }

}
