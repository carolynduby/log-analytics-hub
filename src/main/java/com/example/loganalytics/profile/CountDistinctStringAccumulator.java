package com.example.loganalytics.profile;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.function.Function;

@ToString(callSuper=true)
@EqualsAndHashCode(callSuper = true)
public class CountDistinctStringAccumulator<T> extends CountDistinctAccumulator<T> {

    private final Function<T, String> fieldAccessor;

    public CountDistinctStringAccumulator(String resultName, Function<T, String> fieldAccessor) {
        super(resultName);
        this.fieldAccessor = fieldAccessor;
    }

    @Override
    public void add(T event) {
        addFieldValue(fieldAccessor.apply(event));
    }


}
