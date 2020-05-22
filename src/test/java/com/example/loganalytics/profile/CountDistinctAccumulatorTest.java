package com.example.loganalytics.profile;

import org.junit.Assert;
import org.junit.Test;

public class CountDistinctAccumulatorTest {

    @Test
    public void testAdd() {
        CountDistinctAccumulator   acc = new CountDistinctAccumulator();
        acc.add("First");
        acc.add("Second");
        acc.add("First");

        Assert.assertEquals(2.0, acc.getResult(), 0.1);
    }

    @Test
    public void testMerge() {
        CountDistinctAccumulator acc1 = new CountDistinctAccumulator();
        CountDistinctAccumulator acc2 = new CountDistinctAccumulator();

        acc1.add("First");
        acc2.add("First");
        acc1.add("Second");
        acc2.add("Second");
        acc2.add("Third");
        Assert.assertEquals(2.0, acc1.getResult(), 0.1);
        Assert.assertEquals(3.0, acc2.getResult(), 0.1);
        acc1.merge(acc2);

        Assert.assertEquals(3.0, acc1.getResult(), 0.1);
    }

    @Test
    public void testMergeEmptyThis() {
        CountDistinctAccumulator acc1 = new CountDistinctAccumulator();
        CountDistinctAccumulator acc2 = new CountDistinctAccumulator();
        acc2.add("First");
        acc2.add("Second");
        Assert.assertEquals(0.0, acc1.getResult(), 0.1);
        Assert.assertEquals(2.0, acc2.getResult(), 0.1);

        acc1.merge(acc2);
        Assert.assertEquals(2.0, acc1.getResult(), 0.1);
    }

    @Test
    public void testMergeEmptyOther() {
        CountDistinctAccumulator acc1 = new CountDistinctAccumulator();
        CountDistinctAccumulator acc2 = new CountDistinctAccumulator();
        acc2.add("First");
        acc2.add("Second");
        Assert.assertEquals(0.0, acc1.getResult(), 0.1);
        Assert.assertEquals(2.0, acc2.getResult(), 0.1);

        acc2.merge(acc1);
        Assert.assertEquals(2.0, acc2.getResult(), 0.1);
    }

    @Test
    public void testBothEmpty() {
        CountDistinctAccumulator acc1 = new CountDistinctAccumulator();
        CountDistinctAccumulator acc2 = new CountDistinctAccumulator();
        Assert.assertEquals(0.0, acc1.getResult(), 0.1);
        Assert.assertEquals(0.0, acc2.getResult(), 0.1);

        acc2.merge(acc1);
        Assert.assertEquals(0.0, acc2.getResult(), 0.1);
    }
}
