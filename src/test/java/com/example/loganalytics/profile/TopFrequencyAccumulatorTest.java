package com.example.loganalytics.profile;

import org.junit.Assert;
import org.junit.Test;

public class TopFrequencyAccumulatorTest {

    @Test
    public void testTopFrequency() {
        TopFrequencyAccumulator topFreq1 = new TopFrequencyAccumulator();
        Assert.assertEquals(0, topFreq1.getResult(), 0.1);

        int freqOfA = 5;
        addString(freqOfA, "A", topFreq1);
        topFreq1.add("B");

        Assert.assertEquals(freqOfA, topFreq1.getResult(), 0.1);

        TopFrequencyAccumulator topFreq2 = new TopFrequencyAccumulator();
        addString(freqOfA+1, "C", topFreq2);
        topFreq1.merge(topFreq2);
        Assert.assertEquals(freqOfA+1, topFreq1.getResult(), 0.1);
    }

    private void addString(int frequency, String term, TopFrequencyAccumulator acc) {
        for(int i = 0; i < frequency; i++) {
            acc.add(term);
        }
    }
}
