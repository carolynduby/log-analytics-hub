package com.example.loganalytics.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogEventFieldSpecification {
    private String fieldName;
    private String featureName;
    private Boolean isRequired;
}