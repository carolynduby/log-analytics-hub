package com.example.loganalytics.profile;

import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class CountDistinctStringListAccumulatorTest {

    private static final String TEST_RESULT_NAME = "test_result";

    @AllArgsConstructor
    @Data
    static class StringListTestEvent {
        private List<String> listField;
    }

    @Test
    public void testAdd() {
        String duplicateString = "duplicate";
        String uniqueString = "unique";
        testCountDistinctStringList(Arrays.asList(duplicateString, uniqueString, duplicateString));
    }

    @Test
    public void testMerge() {

        String duplicateString1 = "duplicate_1";
        String duplicateString2 = "duplicate_2";
        String uniqueString = "unique";
        CountDistinctStringListAccumulator<StringListTestEvent> acc1 = testCountDistinctStringList(Arrays.asList(duplicateString1, duplicateString2));
        CountDistinctStringListAccumulator<StringListTestEvent> acc2 = testCountDistinctStringList(Arrays.asList(duplicateString1, duplicateString2, uniqueString));

        acc1.merge(acc2);

        Assert.assertEquals(3.0, acc1.getResult(), 0.1);
    }

    @Test
    public void testMergeEmptyThis() {

        CountDistinctStringListAccumulator<StringListTestEvent> emptyAcc = testCountDistinctStringList(Collections.emptyList());
        CountDistinctStringListAccumulator<StringListTestEvent> nonEmptyAcc = testCountDistinctStringList(Arrays.asList("First", "Second"));
        double nonEmptyAccResult = nonEmptyAcc.getResult();

        emptyAcc.merge(nonEmptyAcc);
        Assert.assertEquals(nonEmptyAccResult, emptyAcc.getResult(), 0.1);
    }

    @Test
    public void testMergeEmptyOther() {
        CountDistinctStringListAccumulator<StringListTestEvent> emptyAcc = testCountDistinctStringList(Collections.emptyList());
        CountDistinctStringListAccumulator<StringListTestEvent> nonEmptyAcc = testCountDistinctStringList(Arrays.asList("First", "Second"));
        double nonEmptyAccResult = nonEmptyAcc.getResult();

        nonEmptyAcc.merge(emptyAcc);
        Assert.assertEquals(nonEmptyAccResult, nonEmptyAcc.getResult(), 0.1);
    }

    @Test
    public void testBothEmpty() {
        CountDistinctStringListAccumulator<StringListTestEvent> empty1 = testCountDistinctStringList(Collections.emptyList());
        CountDistinctStringListAccumulator<StringListTestEvent> empty2 = testCountDistinctStringList(Collections.emptyList());

        empty1.merge(empty2);
        Assert.assertEquals(0.0, empty1.getResult(), 0.1);
    }

    private CountDistinctStringListAccumulator<StringListTestEvent> testCountDistinctStringList(List<String> strings) {
        CountDistinctStringListAccumulator<StringListTestEvent>   acc = new CountDistinctStringListAccumulator<>(TEST_RESULT_NAME, StringListTestEvent::getListField);
        acc.add(new StringListTestEvent(strings));
        Assert.assertEquals(Sets.newHashSet(strings).size(), acc.getResult(), 0.1);
        return acc;
    }

}
