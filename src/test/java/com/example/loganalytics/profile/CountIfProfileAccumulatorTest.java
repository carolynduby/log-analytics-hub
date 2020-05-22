package com.example.loganalytics.profile;

import org.junit.Assert;
import org.junit.Test;

import java.util.function.Predicate;

public class CountIfProfileAccumulatorTest {

    @Test
    public void testCount() {

        Predicate<String> stringEqualsA = s -> s != null && s.equals("A");

        CountIfProfileAccumulator accumulator = new CountIfProfileAccumulator(stringEqualsA);

        Assert.assertEquals(0.0, accumulator.getResult(), 0.1);

        accumulator.add("not an a");
        Assert.assertEquals(0.0, accumulator.getResult(), 0.1);

        accumulator.add("A");
        Assert.assertEquals(1.0, accumulator.getResult(), 0.1);

        accumulator.add("");
        Assert.assertEquals(1.0, accumulator.getResult(), 0.1);
    }

    @Test
    public void testMerge() {
        CountProfileAccumulator accumulator1 = new CountProfileAccumulator();
        CountProfileAccumulator accumulator2 = new CountProfileAccumulator();

        Assert.assertEquals(0.0, accumulator1.getResult(), 0.1);
        Assert.assertEquals(0.0, accumulator2.getResult(), 0.1);

        accumulator1.add("not used");
        accumulator2.add("not used");

        Assert.assertEquals(1.0, accumulator1.getResult(), 0.1);
        Assert.assertEquals(1.0, accumulator2.getResult(), 0.1);

        accumulator1.merge(accumulator2);

        Assert.assertEquals(2.0, accumulator1.getResult(), 0.1);
        Assert.assertEquals(1.0, accumulator2.getResult(), 0.1);

    }

}
