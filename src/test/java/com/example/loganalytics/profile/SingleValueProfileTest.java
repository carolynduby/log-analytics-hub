package com.example.loganalytics.profile;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventFieldSpecification;
import org.junit.Assert;
import org.junit.Test;

public class SingleValueProfileTest {

    @Test
    public void testProfile() {

        CountDistinctAccumulator accumulator = new CountDistinctAccumulator();

        String fieldName = "test_field";
        String profileName = "test_profile";
        SingleValueProfile singleValueProfile = new SingleValueProfile(profileName, new LogEventFieldSpecification(fieldName, "TEST", false), accumulator);

        Assert.assertEquals(0.0, singleValueProfile.getResult(), 0.1);
        Assert.assertEquals(profileName, singleValueProfile.getResultName());
        // add an event that increments the profile
        LogEvent logEvent = new LogEvent();

        logEvent.setField(fieldName, "Test value");

        singleValueProfile.add(logEvent);

        Assert.assertEquals(1.0, singleValueProfile.getResult(), 0.1);

        // add an event that does not define the field and thus does not increment the profile
        singleValueProfile.add(new LogEvent());
        Assert.assertEquals(1.0, singleValueProfile.getResult(), 0.1);

        SingleValueProfile secondProfile = new SingleValueProfile(profileName, new LogEventFieldSpecification(fieldName, "TEST", false), new CountDistinctAccumulator());
        secondProfile.add(logEvent);

        logEvent.setField(fieldName, "Another value");
        secondProfile.add(logEvent);
        singleValueProfile.merge(secondProfile);
        Assert.assertEquals(2.0, singleValueProfile.getResult(), 0.1);

    }
}
