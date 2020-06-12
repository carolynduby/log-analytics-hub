package com.example.loganalytics.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class LogEventFieldSpecification {

    public String fieldName;
    public String featureName;
    public Boolean isRequired;
}