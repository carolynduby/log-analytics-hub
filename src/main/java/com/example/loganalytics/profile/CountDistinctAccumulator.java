package com.example.loganalytics.profile;

import org.apache.datasketches.theta.*;

public class CountDistinctAccumulator implements ProfileAccumulator {
    private final Union union = SetOperation.builder().buildUnion();

    public void add(String value) {
        union.update(value);
    }

    @Override
    public void merge(ProfileAccumulator other) {
        if (other instanceof CountDistinctAccumulator) {
            union.update(((CountDistinctAccumulator)other).union.getResult());
        }
    }

    public double getResult() {
        return union.getResult().getEstimate();
    }
}
