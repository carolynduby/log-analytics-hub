package com.example.loganalytics.profile;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;
import java.util.function.Function;

@ToString(callSuper=true)
@EqualsAndHashCode(callSuper = true)
public class CountDistinctStringListAccumulator<T> extends CountDistinctAccumulator<T> {

    private final Function<T, List<String>> fieldAccessor;

    public CountDistinctStringListAccumulator(String resultName, Function<T, List<String>> fieldAccessor) {
        super(resultName);
        this.fieldAccessor = fieldAccessor;
    }

    public void add(T event) {
        List<String>  value = fieldAccessor.apply(event);
        if (value != null) {
            for(String element : value) {
                addFieldValue(element);
            }
        }
    }

}
