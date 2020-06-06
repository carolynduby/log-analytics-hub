package com.example.loganalytics.profile;

import lombok.ToString;

@ToString
public abstract class ProfileAccumulator<T> {

    private final String resultName;

    public ProfileAccumulator(String resultName) {
        this.resultName = resultName;
    }

    public String getResultName() {
        return resultName;
    }

    public abstract void add(T event);

    public abstract void merge(ProfileAccumulator<T> other);

    public abstract double getResult();


}
