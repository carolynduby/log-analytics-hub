package com.example.loganalytics.profile;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Union;

@ToString(callSuper=true)
@EqualsAndHashCode(callSuper = true)
public abstract class CountDistinctAccumulator<T> extends ProfileAccumulator<T> {
    private final Union union = SetOperation.builder().buildUnion();


    public CountDistinctAccumulator(String resultName) {
        super(resultName);
    }

    public abstract void add(T event);

    protected void addFieldValue(String fieldValue) {
        if (fieldValue != null) {
            union.update(fieldValue);
        }
    }

    @Override
    public void merge(ProfileAccumulator<T> other) {
        if (this != other && other instanceof CountDistinctAccumulator) {
            union.update(((CountDistinctAccumulator<T>)other).union.getResult());
        }
    }

    public double getResult() {
        return union.getResult().getEstimate();
    }
}
