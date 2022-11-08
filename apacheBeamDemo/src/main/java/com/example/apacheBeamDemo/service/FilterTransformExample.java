package com.example.apacheBeamDemo.service;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class FilterTransformExample  implements SerializableFunction <String, Boolean>{

    @Override
    public Boolean apply(String input) {
        return input.contains("Los Angeles");
    }
}
