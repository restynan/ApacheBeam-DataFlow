package com.example.apacheBeamDemo.service;

import org.apache.beam.sdk.transforms.SimpleFunction;

public class User extends SimpleFunction<String, String> {
    //create our custom logic for apply function
    @Override
    public String apply(String input){

        String arr[] = input.split(",");
        String SId= arr[0];
        String UId= arr[1];
        String Uname= arr[2];
        String VId= arr[3];
        String duration= arr[4];
        String startTime= arr[5];
        String sex= arr[6];

        String ouput="";
        if(sex.equals("1")) {
            ouput=SId+","+UId+","+","+Uname+","+VId+","+duration+","+startTime+","+"M";
        }else if(sex.equals("2")) {
            ouput=SId+","+UId+","+","+Uname+","+VId+","+duration+","+startTime+","+"F";
        }
        else {
            ouput=input;
        }

        return ouput;
    }

}
