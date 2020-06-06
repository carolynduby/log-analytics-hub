package com.example.loganalytics.profile;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper=true)
@EqualsAndHashCode(callSuper = true)
public class CountProfileAccumulator<T> extends ProfileAccumulator<T> {
    private int counter = 0;

    public CountProfileAccumulator(String resultName) {
        super(resultName);
    }

    @Override
    public void add(T logEvent) {
        counter += 1;
    }

    @Override
    public void merge(ProfileAccumulator<T> other) {
        if (this != other && other instanceof CountProfileAccumulator) {
            counter += ((CountProfileAccumulator<T>) other).counter;
        }
    }

    @Override
    public double getResult() {
        return counter;
    }
}
