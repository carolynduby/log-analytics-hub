package com.example.loganalytics.profile;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;

import java.util.function.Function;

@ToString(callSuper=true)
@EqualsAndHashCode(callSuper = true)
public class TopFrequencyAccumulator<T> extends ProfileAccumulator<T> {

    private final ItemsSketch<String> itemsSketch = new ItemsSketch<>(32);
    private final Function<T, String> fieldAccessor;

    public TopFrequencyAccumulator(String resultName, Function<T, String> fieldAccessor) {
        super(resultName);
        this.fieldAccessor = fieldAccessor;
    }

    @Override
    public void add(T logEvent) {
        String fieldValue = fieldAccessor.apply(logEvent);
        if (fieldValue != null) {
            itemsSketch.update(fieldValue);
        }
    }

    @Override
    public void merge(ProfileAccumulator<T> other) {
        if (this != other && other instanceof TopFrequencyAccumulator) {
            itemsSketch.merge(((TopFrequencyAccumulator<T>) other).itemsSketch);
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
