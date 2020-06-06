package com.example.loganalytics.profile;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.function.Function;

@ToString(callSuper=true)
@EqualsAndHashCode(callSuper = true)
public class MaxProfileAccumulator<T> extends ProfileAccumulator<T> {
    private double max = 0.0;
    private final Function<T, Double> fieldAccessor;

    public MaxProfileAccumulator(String resultName, Function<T, Double> fieldAccessor) {
        super(resultName);
        this.fieldAccessor = fieldAccessor;
    }

    @Override
    public void add(T event) {
        Double fieldValue = fieldAccessor.apply(event);
        if (fieldValue != null) {
            max = Math.max(fieldValue, max);
        }
    }

    @Override
    public void merge(ProfileAccumulator<T> other) {
        if (other != this && other instanceof MaxProfileAccumulator) {
            max = Math.max(max, ((MaxProfileAccumulator<T>) other).max);
        }
    }

    @Override
    public double getResult() {
        return max;
    }
}
