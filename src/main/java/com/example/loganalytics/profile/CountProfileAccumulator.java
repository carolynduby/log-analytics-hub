package com.example.loganalytics.profile;

public class CountProfileAccumulator implements ProfileAccumulator {
    private int counter = 0;

    @Override
    public void add(String value) {
        counter += 1;
    }

    @Override
    public void merge(ProfileAccumulator other) {
        if (other instanceof CountProfileAccumulator) {
            counter += ((CountProfileAccumulator) other).counter;
        }
    }

    @Override
    public double getResult() {
        return counter;
    }
}
