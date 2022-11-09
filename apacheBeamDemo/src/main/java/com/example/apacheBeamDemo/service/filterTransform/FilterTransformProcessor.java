package com.example.apacheBeamDemo.service.filterTransform;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class FilterTransformProcessor implements SerializableFunction <String, Boolean>{

    @Override
    public Boolean apply(String input) {
        return input.contains("Los Angeles");
    }
}
