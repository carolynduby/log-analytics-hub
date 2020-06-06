package com.example.loganalytics.profile;

import org.junit.Assert;
import org.junit.Test;

import java.util.function.Predicate;

public class CountIfProfileAccumulatorTest extends ProfileAccumulatorTest {
    private static final String MATCHING_STRING = "match";
    private static final Predicate<String> STRING_MATCHES = s -> s != null && s.equals(MATCHING_STRING);

    @Test
    public void testCount() {

        String matchingString = "match";

        double expectedCount = 0.0;

        CountIfProfileAccumulator<TestClass> accumulator = new CountIfProfileAccumulator<>(TEST_RESULT_NAME, TestClass::getField, STRING_MATCHES);
        Assert.assertEquals(expectedCount, accumulator.getResult(), 0.1);

        testAdd(accumulator, "not a match", expectedCount);
        testAdd(accumulator, matchingString, ++expectedCount);
        testAdd(accumulator, "", expectedCount);
        testAdd(accumulator, null, expectedCount);
    }

    private void testAdd(CountIfProfileAccumulator<TestClass> accumulator, String stringToAdd, double expectedCount) {
        accumulator.add(new TestClass(stringToAdd));
        Assert.assertEquals(expectedCount, accumulator.getResult(), 0.1);
    }

    @Test
    public void testMerge() {
        CountIfProfileAccumulator<TestClass> accumulator1 = new CountIfProfileAccumulator<>(TEST_RESULT_NAME, TestClass::getField, STRING_MATCHES);
        double expectedAccumulator1Count = 0.0;
        CountIfProfileAccumulator<TestClass> accumulator2 = new CountIfProfileAccumulator<>(TEST_RESULT_NAME, TestClass::getField, STRING_MATCHES);
        double expectedAccumulator2Count = 0.0;

        Assert.assertEquals(expectedAccumulator1Count, accumulator1.getResult(), 0.1);
        Assert.assertEquals(expectedAccumulator2Count, accumulator2.getResult(), 0.1);

        // increment both counts
        testAdd(accumulator1, MATCHING_STRING, ++expectedAccumulator1Count);
        testAdd(accumulator2, MATCHING_STRING, ++expectedAccumulator2Count);

        // merge two counts together
        expectedAccumulator1Count += expectedAccumulator2Count;
        testMerge(accumulator1, expectedAccumulator1Count, accumulator2, expectedAccumulator2Count);

        // merge profile with itself - no increment
        testMerge(accumulator1, expectedAccumulator1Count, accumulator1, expectedAccumulator1Count);

        // merge profile with null - no increment
        testMerge(accumulator1, expectedAccumulator1Count, null, 0.0);
    }

    private void testMerge(CountIfProfileAccumulator<TestClass> accumulator1, double expectedAccumulator1Count, CountIfProfileAccumulator<TestClass> accumulator2, double expectedAccumulator2Count) {
        accumulator1.merge(accumulator2);
        Assert.assertEquals(expectedAccumulator1Count, accumulator1.getResult(), 0.1);
        if (accumulator2 != null) {
            Assert.assertEquals(expectedAccumulator2Count, accumulator2.getResult(), 0.1);
        }
    }

}
