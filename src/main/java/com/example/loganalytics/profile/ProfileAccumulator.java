package com.example.loganalytics.profile;

public interface ProfileAccumulator {
    void add(String value);
    void merge(ProfileAccumulator other);
    double getResult();
}
