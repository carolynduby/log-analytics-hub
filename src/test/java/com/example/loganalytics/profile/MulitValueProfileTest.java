package com.example.loganalytics.profile;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventFieldSpecification;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class MulitValueProfileTest {
    @Test
    public void testProfile() {

        CountDistinctAccumulator accumulator = new CountDistinctAccumulator();

        String fieldName = "test_field";
        String testProfileName = "test_profile";
        MultiValueProfile multiValueProfile = new MultiValueProfile(testProfileName, new LogEventFieldSpecification(fieldName, "TEST", false), accumulator);

        Assert.assertEquals(0.0, multiValueProfile.getResult(), 0.1);
        Assert.assertEquals(testProfileName, multiValueProfile.getResultName());

        // add an event that increments the profile
        LogEvent logEvent = new LogEvent();

        logEvent.setField(fieldName, Lists.newArrayList("Test String 1", "Test String 2"));

        multiValueProfile.add(logEvent);

        Assert.assertEquals(2.0, multiValueProfile.getResult(), 0.1);

        // add an event that does not define the field and thus does not increment the profile
        multiValueProfile.add(new LogEvent());
        Assert.assertEquals(2.0, multiValueProfile.getResult(), 0.1);

    }
}
