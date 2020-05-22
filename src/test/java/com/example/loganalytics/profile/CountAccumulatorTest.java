package com.example.loganalytics.profile;

import org.junit.Assert;
import org.junit.Test;

public class CountAccumulatorTest {

    @Test
    public void testCount() {
        CountProfileAccumulator accumulator = new CountProfileAccumulator();

        Assert.assertEquals(0.0, accumulator.getResult(), 0.1);

        accumulator.add("not used");
        Assert.assertEquals(1.0, accumulator.getResult(), 0.1);

        accumulator.add(null);
        Assert.assertEquals(2.0, accumulator.getResult(), 0.1);

        accumulator.add("");
        Assert.assertEquals(3.0, accumulator.getResult(), 0.1);
    }

    @Test
    public void testMerge() {
        CountProfileAccumulator accumulator1 = new CountProfileAccumulator();
        CountProfileAccumulator accumulator2 = new CountProfileAccumulator();

        Assert.assertEquals(0.0, accumulator1.getResult(), 0.1);
        Assert.assertEquals(0.0, accumulator2.getResult(), 0.1);

        accumulator1.add("not used");
        accumulator2.add("not uses");

        Assert.assertEquals(1.0, accumulator1.getResult(), 0.1);
        Assert.assertEquals(1.0, accumulator2.getResult(), 0.1);

        accumulator1.merge(accumulator2);

        Assert.assertEquals(2.0, accumulator1.getResult(), 0.1);
        Assert.assertEquals(1.0, accumulator2.getResult(), 0.1);

    }
}
