package com.example.loganalytics.profile;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class CountDistinctStringAccumulatorTest extends ProfileAccumulatorTest {

    @Test
    public void testAdd() {
        String duplicateString = "duplicate";
        String uniqueString = "unique";
        CountDistinctStringAccumulator<TestClass> accumulator = testCountDistinctString(Arrays.asList(duplicateString, uniqueString, duplicateString));

        // add a null value - should not change value
        double result = accumulator.getResult();
        accumulator.add(new TestClass(null));

        Assert.assertEquals(result, accumulator.getResult(), 0.1);
    }

    @Test
    public void testAddNullString() {
        String duplicateString = "duplicate";
        String nullString = null;
        testCountDistinctString(Arrays.asList(duplicateString, null, duplicateString));
    }

    @Test
    public void testMerge() {

        String duplicateString1 = "duplicate_1";
        String duplicateString2 = "duplicate_2";
        String uniqueString = "unique";
        CountDistinctStringAccumulator<TestClass> acc1 = testCountDistinctString(Arrays.asList(duplicateString1, duplicateString2));
        CountDistinctStringAccumulator<TestClass> acc2 = testCountDistinctString(Arrays.asList(duplicateString1, duplicateString2, uniqueString));

        acc1.merge(acc2);

        Assert.assertEquals(3.0, acc1.getResult(), 0.1);
    }

    @Test
    public void testMergeEmptyThis() {

        CountDistinctStringAccumulator<TestClass> emptyAcc = testCountDistinctString(Collections.emptyList());
        CountDistinctStringAccumulator<TestClass> nonEmptyAcc = testCountDistinctString(Arrays.asList("First", "Second"));
        double nonEmptyAccResult = nonEmptyAcc.getResult();

        emptyAcc.merge(nonEmptyAcc);
        Assert.assertEquals(nonEmptyAccResult, emptyAcc.getResult(), 0.1);
    }

    @Test
    public void testMergeNullOther() {

        CountDistinctStringAccumulator<TestClass> emptyAcc = testCountDistinctString(Collections.emptyList());

        emptyAcc.merge(null);
        Assert.assertEquals(0.0, emptyAcc.getResult(), 0.1);
    }

    @Test
    public void testMergeEmptyOther() {
        CountDistinctStringAccumulator<TestClass> emptyAcc = testCountDistinctString(Collections.emptyList());
        CountDistinctStringAccumulator<TestClass> nonEmptyAcc = testCountDistinctString(Arrays.asList("First", "Second"));
        double nonEmptyAccResult = nonEmptyAcc.getResult();

        nonEmptyAcc.merge(emptyAcc);
        Assert.assertEquals(nonEmptyAccResult, nonEmptyAcc.getResult(), 0.1);
    }

    @Test
    public void testMergeOtherWithWrongType() {
        CountProfileAccumulator<TestClass> wrongTypeAcc = new CountProfileAccumulator<>("wrong_type");
        wrongTypeAcc.add(new TestClass("A"));

        CountDistinctStringAccumulator<TestClass> nonEmptyAcc = testCountDistinctString(Arrays.asList("First", "Second"));
        double nonEmptyAccResult = nonEmptyAcc.getResult();

        nonEmptyAcc.merge(wrongTypeAcc);
        Assert.assertEquals(nonEmptyAccResult, nonEmptyAcc.getResult(), 0.1);
    }

    @Test
    public void testBothEmpty() {
        CountDistinctStringAccumulator<TestClass> empty1 = testCountDistinctString(Collections.emptyList());
        CountDistinctStringAccumulator<TestClass>  empty2= testCountDistinctString(Collections.emptyList());

        empty1.merge(empty2);
        Assert.assertEquals(0.0, empty1.getResult(), 0.1);
    }

    private CountDistinctStringAccumulator<TestClass> testCountDistinctString(List<String> strings) {
        CountDistinctStringAccumulator<TestClass>   acc = new CountDistinctStringAccumulator<>(TEST_RESULT_NAME, TestClass::getField);
        for(String nextString : strings) {
            acc.add(new TestClass(nextString));
        }
        double uniqueStringCount = strings.stream().filter(Objects::nonNull).distinct().count();
        Assert.assertEquals(uniqueStringCount, acc.getResult(), 0.1);
        return acc;
    }
}
