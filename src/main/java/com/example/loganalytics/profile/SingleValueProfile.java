package com.example.loganalytics.profile;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventFieldSpecification;

public class SingleValueProfile extends Profile {

    public SingleValueProfile(String resultName, LogEventFieldSpecification fieldSpecification, ProfileAccumulator accumulator) {
        super(resultName, accumulator, fieldSpecification);
    }

    @Override
    public void add(LogEvent logEvent) {
        String fieldValue = logEvent.getField(fieldSpecification, String.class);
        if (fieldValue != null) {
            accumulator.add(fieldValue);
        }
    }


}
