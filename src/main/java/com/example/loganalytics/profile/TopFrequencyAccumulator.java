package com.example.loganalytics.profile;

import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;

public class TopFrequencyAccumulator implements ProfileAccumulator {

    private final ItemsSketch<String> itemsSketch = new ItemsSketch<>(32);

    @Override
    public void add(String value) {
        itemsSketch.update(value);
    }

    @Override
    public void merge(ProfileAccumulator other) {
        if (other instanceof TopFrequencyAccumulator) {
            itemsSketch.merge(((TopFrequencyAccumulator) other).itemsSketch);
        }
    }

    @Override
    public double getResult() {
        ItemsSketch.Row<String>[] frequencies = itemsSketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);
        long topFrequency = 0;

        if (frequencies.length > 0) {
            topFrequency = frequencies[0].getEstimate();
        }

        return topFrequency;
    }
}
