package com.example.loganalytics.profile;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventFieldSpecification;

import java.util.List;

public class MultiValueProfile extends Profile {

    public MultiValueProfile(String resultName, LogEventFieldSpecification fieldSpecification, ProfileAccumulator accumulator) {
        super(resultName, accumulator, fieldSpecification);
    }

    @Override
    public void add(LogEvent event) {
        @SuppressWarnings("unchecked") List<String> fieldValueList = event.getField(fieldSpecification, List.class);
        if (fieldValueList != null) {
            for(String fieldValue : fieldValueList) {
                accumulator.add(fieldValue);
            }
        }
    }

}
