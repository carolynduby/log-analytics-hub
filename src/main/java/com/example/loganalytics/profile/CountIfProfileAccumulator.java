package com.example.loganalytics.profile;

import java.util.function.Predicate;

public class CountIfProfileAccumulator extends CountProfileAccumulator {
    private final Predicate<String> countPredicate;

    public CountIfProfileAccumulator(Predicate<String> countPredicate) {
        this.countPredicate = countPredicate;
    }

    @Override
    public void add(String fieldValue) {
        if (countPredicate.test(fieldValue)) {
            super.add(fieldValue);
        }
    }
}
