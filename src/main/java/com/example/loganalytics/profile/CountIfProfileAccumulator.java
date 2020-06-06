package com.example.loganalytics.profile;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.function.Function;
import java.util.function.Predicate;

@ToString(callSuper=true)
@EqualsAndHashCode(callSuper = true)
public class CountIfProfileAccumulator<T> extends CountProfileAccumulator<T> {
    private final Function<T, String> fieldAccessor;
    private final Predicate<String> countPredicate;

    public CountIfProfileAccumulator(String resultName, Function<T, String> fieldAccessor, Predicate<String> countPredicate) {
        super(resultName);
        this.fieldAccessor = fieldAccessor;
        this.countPredicate = countPredicate;
    }

    @Override
    public void add(T event) {
        String fieldValue = fieldAccessor.apply(event);
        if (fieldValue != null) {
            if (countPredicate.test(fieldValue)) {
                super.add(event);
            }
        }
    }
}
