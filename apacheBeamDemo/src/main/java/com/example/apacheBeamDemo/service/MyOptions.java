package com.example.apacheBeamDemo.service;

import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {
    void setInputFile(String file);
    String getInputFile();

    void setOutPutFile(String file);
    String getOutputFile();

    void setExtension(String Extension);
    String getExtension();
}
