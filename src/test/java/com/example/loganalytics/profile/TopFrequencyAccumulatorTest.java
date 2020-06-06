package com.example.loganalytics.profile;

import org.junit.Assert;
import org.junit.Test;

public class TopFrequencyAccumulatorTest extends ProfileAccumulatorTest {

    @Test
    public void testTopFrequency() {
        TopFrequencyAccumulator<TestClass> topFreq1 = new TopFrequencyAccumulator<>(TEST_RESULT_NAME, TestClass::getField);
        Assert.assertEquals(0, topFreq1.getResult(), 0.1);

        int freqOfA = 5;
        addString(freqOfA, "A", topFreq1);
        addString(1, "B", topFreq1);

        Assert.assertEquals(freqOfA, topFreq1.getResult(), 0.1);

        TopFrequencyAccumulator<TestClass> topFreq2 = new TopFrequencyAccumulator<>(TEST_RESULT_NAME, TestClass::getField);
        addString(freqOfA+1, "C", topFreq2);
        topFreq1.merge(topFreq2);
        Assert.assertEquals(freqOfA+1, topFreq1.getResult(), 0.1);

        topFreq2.add(new TestClass(null));
        Assert.assertEquals(freqOfA+1, topFreq1.getResult(), 0.1);
    }

    @Test
    public void testMergeMatchingStrings() {
        TopFrequencyAccumulator<TestClass> topFreq1 = new TopFrequencyAccumulator<>(TEST_RESULT_NAME, TestClass::getField);
        Assert.assertEquals(0, topFreq1.getResult(), 0.1);

        int freqOfA = 5;
        addString(freqOfA, "A", topFreq1);
        addString(1, "B", topFreq1);

        Assert.assertEquals(freqOfA, topFreq1.getResult(), 0.1);

        TopFrequencyAccumulator<TestClass> topFreq2 = new TopFrequencyAccumulator<>(TEST_RESULT_NAME, TestClass::getField);
        addString(freqOfA+1, "A", topFreq2);
        topFreq1.merge(topFreq2);
        Assert.assertEquals((2 * freqOfA)+1, topFreq1.getResult(), 0.1);
    }

    @Test
    public void testMergeWithSelf() {
        TopFrequencyAccumulator<TestClass> accumulator = new TopFrequencyAccumulator<>(TEST_RESULT_NAME, TestClass::getField);
        accumulator.add(new TestClass("A"));
        Assert.assertEquals(1.0, accumulator.getResult(), 0.1);

        accumulator.merge(accumulator);
        Assert.assertEquals(1.0, accumulator.getResult(), 0.1);
    }

    @Test
    public void testMergeWithNull() {
        TopFrequencyAccumulator<TestClass> accumulator = new TopFrequencyAccumulator<>(TEST_RESULT_NAME, TestClass::getField);
        accumulator.add(new TestClass("A"));
        Assert.assertEquals(1.0, accumulator.getResult(), 0.1);

        accumulator.merge(null);
        Assert.assertEquals(1.0, accumulator.getResult(), 0.1);
    }

    @Test
    public void testMergeWithWrongType() {
        TopFrequencyAccumulator<TestClass> accumulator = new TopFrequencyAccumulator<>(TEST_RESULT_NAME, TestClass::getField);
        accumulator.add(new TestClass("A"));
        Assert.assertEquals(1.0, accumulator.getResult(), 0.1);

        // attempt to merge with the wrong type of profile
        CountProfileAccumulator<TestClass> wrongType = new CountProfileAccumulator<>(TEST_RESULT_NAME);
        wrongType.add(new TestClass("B"));

        accumulator.merge(wrongType);
        Assert.assertEquals(1.0, accumulator.getResult(), 0.1);
    }

    private void addString(int frequency, String term, TopFrequencyAccumulator<TestClass> acc) {
        for(int i = 0; i < frequency; i++) {
            acc.add(new TestClass(term));
        }
    }
}
