package com.example.loganalytics.event;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class LogEventFieldSpecification implements Serializable { // should be Flink POJO instead
    private static final long serialVersionUID = 1L;

    private String fieldName;
    private String featureName;
    private Boolean isRequired;
}