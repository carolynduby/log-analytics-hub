package com.example.loganalytics.profile;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventFieldSpecification;

public abstract class Profile {

    private final String resultName;
    protected final ProfileAccumulator accumulator;
    protected final LogEventFieldSpecification fieldSpecification;

    public Profile(String resultName, ProfileAccumulator accumulator, LogEventFieldSpecification fieldSpecification) {
        this.resultName = resultName;
        this.accumulator = accumulator;
        this.fieldSpecification = fieldSpecification;
    }

    abstract void add(LogEvent value);

    public void merge(Profile other) {
        accumulator.merge(other.accumulator);
    }

    public double getResult() {
        return accumulator.getResult();
    }

    public String getResultName() {
        return resultName;
    }
}