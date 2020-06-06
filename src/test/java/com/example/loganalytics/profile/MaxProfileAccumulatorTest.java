package com.example.loganalytics.profile;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

public class MaxProfileAccumulatorTest {

    @Data
    @AllArgsConstructor
    static class TestClass {
        Double number;
    }

    private static final String TEST_RESULT_NAME = "test_result";

    @Test
    public void testMaximum() {
        MaxProfileAccumulator<TestClass> accumulator = new MaxProfileAccumulator<>(TEST_RESULT_NAME, TestClass::getNumber);

        Assert.assertEquals(0.0, accumulator.getResult(), 0.1);
        Assert.assertEquals(TEST_RESULT_NAME, accumulator.getResultName());

        verifyMaximum(accumulator, 5.0, 5.0);

        verifyMaximum(accumulator, 1.0, 5.0);

        verifyMaximum(accumulator, 10.0, 10.0);
    }

    @Test
    public void testNullFieldValue() {
        MaxProfileAccumulator<TestClass> accumulator = new MaxProfileAccumulator<>(TEST_RESULT_NAME, TestClass::getNumber);

        verifyMaximum(accumulator, null, 0.0);
    }

    @Test
    public void testMerge() {
        MaxProfileAccumulator<TestClass> accumulator1 = new MaxProfileAccumulator<>(TEST_RESULT_NAME, TestClass::getNumber);
        MaxProfileAccumulator<TestClass> accumulator2 = new MaxProfileAccumulator<>(TEST_RESULT_NAME, TestClass::getNumber);

        Assert.assertEquals(0.0, accumulator1.getResult(), 0.1);
        Assert.assertEquals(0.0, accumulator2.getResult(), 0.1);

        verifyMaximum(accumulator1, 1.0, 1.0);
        verifyMaximum(accumulator2, 5.0, 5.0);

        accumulator1.merge(accumulator2);

        Assert.assertEquals(5.0, accumulator1.getResult(), 0.1);
        Assert.assertEquals(5.0, accumulator2.getResult(), 0.1);
    }

    @Test
    public void testMergeWithSelf() {
        MaxProfileAccumulator<TestClass> accumulator = new MaxProfileAccumulator<>(TEST_RESULT_NAME, TestClass::getNumber);
        verifyMaximum(accumulator, 1.0, 1.0);

        accumulator.merge(accumulator);

        Assert.assertEquals(1.0, accumulator.getResult(), 0.1);
    }

    @Test
    public void testMergeWithInvalidType() {
        MaxProfileAccumulator<TestClass> accumulator = new MaxProfileAccumulator<>(TEST_RESULT_NAME, TestClass::getNumber);
        verifyMaximum(accumulator, 1.0, 1.0);

        CountProfileAccumulator<TestClass> countAccumulator = new CountProfileAccumulator<>(TEST_RESULT_NAME);
        accumulator.merge(countAccumulator);

        Assert.assertEquals(1.0, accumulator.getResult(), 0.1);
    }

    void verifyMaximum(MaxProfileAccumulator<TestClass> accumulator, Double fieldValue, Double expectedMaxValue) {
        accumulator.add(new TestClass(fieldValue));
        Assert.assertEquals(expectedMaxValue, accumulator.getResult(), 0.1);
    }
}
