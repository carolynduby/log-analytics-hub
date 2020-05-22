package com.example.loganalytics.profile;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.pipeline.profiles.ProfileEvent;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

public class ProfileGroupTest {

    @Test
    public void testCountDistinct() {
        String groupName = "test_group";
        String entityKeyFieldName = "test.key";
        ProfileGroup profileGroup = new ProfileGroup(groupName, entityKeyFieldName);

        Assert.assertEquals(groupName, profileGroup.getProfileGroupName());
        Assert.assertEquals(entityKeyFieldName, profileGroup.getEntityKeyFieldSpecification().getFieldName());

        String testFieldName = "test.field";
        String countDistinctMeasurement = "distinct";
        profileGroup.addCountDistinct(countDistinctMeasurement, testFieldName, false);

        String entityKey =  "the key";
        Map<String, Object> fields = new HashMap<>();
        fields.put(testFieldName, "string1");
        addLogEvent(entityKeyFieldName, entityKey, profileGroup, fields);

        Map<String, Double> expectedMeasurements = new HashMap<>();
        expectedMeasurements.put(countDistinctMeasurement, 1.0);

        verifyProfileEvent(profileGroup, entityKey, expectedMeasurements);
    }

    @Test
    public void testCountIf() {
        String groupName = "test_group";
        String entityKeyFieldName = "test.key";
        ProfileGroup profileGroup = new ProfileGroup(groupName, entityKeyFieldName);

        Assert.assertEquals(groupName, profileGroup.getProfileGroupName());
        Assert.assertEquals(entityKeyFieldName, profileGroup.getEntityKeyFieldSpecification().getFieldName());

        String testFieldName = "test.field";
        String measurement = "countif";
        String matchingFieldName = "match";
        profileGroup.addCountIf(measurement, testFieldName, Predicate.isEqual(matchingFieldName));

        // add a log event with predicate = true
        // increments countif
        String entityKey =  "the key";
        Map<String, Object> fields = new HashMap<>();
        fields.put(testFieldName, matchingFieldName);
        addLogEvent(entityKeyFieldName, entityKey, profileGroup, fields);

        Map<String, Double> expectedMeasurements = new HashMap<>();
        expectedMeasurements.put(measurement, 1.0);

        verifyProfileEvent(profileGroup, entityKey, expectedMeasurements);

        // add a log event with predicat = false
        // does not increment countif
        fields = new HashMap<>();
        fields.put(testFieldName, "doesnt match");
        addLogEvent(entityKeyFieldName, entityKey, profileGroup, fields);

        verifyProfileEvent(profileGroup, entityKey, expectedMeasurements);
    }

    @Test
    public void testTopFrequency() {
        String groupName = "test_group";
        String entityKeyFieldName = "test.key";
        ProfileGroup profileGroup = new ProfileGroup(groupName, entityKeyFieldName);

        Assert.assertEquals(groupName, profileGroup.getProfileGroupName());
        Assert.assertEquals(entityKeyFieldName, profileGroup.getEntityKeyFieldSpecification().getFieldName());

        String testFieldName = "test.field";
        String measurement = "countif";
        String matchingFieldName = "match";
        profileGroup.addTopFrequency(measurement, testFieldName);

        // add a log event with predicate = true
        // increments countif
        String entityKey =  "the key";
        Map<String, Object> fields = new HashMap<>();
        fields.put(testFieldName, matchingFieldName);
        addLogEvent(entityKeyFieldName, entityKey, profileGroup, fields);
        addLogEvent(entityKeyFieldName, entityKey, profileGroup, fields);

        Map<String, Double> expectedMeasurements = new HashMap<>();
        expectedMeasurements.put(measurement, 2.0);

        verifyProfileEvent(profileGroup, entityKey, expectedMeasurements);
    }

    @Test
    public void testRatios() {
        String groupName = "test_group";
        String entityKeyFieldName = "test.key";
        ProfileGroup profileGroup = new ProfileGroup(groupName, entityKeyFieldName);

        Assert.assertEquals(groupName, profileGroup.getProfileGroupName());
        Assert.assertEquals(entityKeyFieldName, profileGroup.getEntityKeyFieldSpecification().getFieldName());

        String testFieldName = "test.field";
        String testListField = "list.field";
        String numerator = "numerator";
        String denominator = "denominator";
        String ratio = "ratio";
        profileGroup.addCount(numerator, testFieldName).
                    addCountDistinct(denominator, testListField, true).
                    addRatio(ratio, numerator, denominator);

        // verify ratios work before events are added
        ProfileEvent profileEvent = profileGroup.getProfileEventResult();
        Assert.assertEquals(0.0, profileEvent.getMeasurements().get(ratio), 0.1);
        Assert.assertEquals(0.0, profileEvent.getMeasurements().get(numerator), 0.1);
        Assert.assertEquals(0.0, profileEvent.getMeasurements().get(denominator), 0.1);

        // set the numerator and denominator and verify ratio is calculated correctly
        String entityKey =  "the key";
        Map<String, Object> fields = new HashMap<>();
        fields.put(testFieldName, "string1");
        fields.put(testListField, Lists.newArrayList("element 1", "element 2"));

        addLogEvent(entityKeyFieldName, entityKey, profileGroup, fields);

        Map<String, Double> expectedMeasurements = new HashMap<>();
        expectedMeasurements.put(numerator, 1.0);
        expectedMeasurements.put(denominator, 2.0);
        expectedMeasurements.put(ratio, 0.5);

        verifyProfileEvent(profileGroup, entityKey, expectedMeasurements);
    }

    @Test
    public void testMergeTwoNonEmptyProfileGroups() {
        String mergedMeasurementName = "merged";
        ProfileGroup profileGroup1 = createMergeProfileGroup(mergedMeasurementName);
        ProfileGroup profileGroup2 = createMergeProfileGroup(mergedMeasurementName);
        profileGroup1.merge(profileGroup2);
        ProfileEvent profileEvent = profileGroup1.getProfileEventResult();
        Assert.assertEquals(2.0, profileEvent.getMeasurements().get(mergedMeasurementName), 0.1);
    }

    private ProfileGroup createMergeProfileGroup(String mergedMeasurementName) {
        String groupName = "test_group";
        String entityKeyFieldName = "test.key";
        String countFieldName = "count.field";
        ProfileGroup profileGroup = new ProfileGroup(groupName, entityKeyFieldName);
        profileGroup.addCount(mergedMeasurementName, countFieldName);
        LogEvent logEvent = new LogEvent();
        logEvent.setField(countFieldName, "a string value");
        profileGroup.add(logEvent);

        return profileGroup;
    }


    @Test
    public void testMergeTwoEmptyProfileGroups() {
        String groupName = "test_group";
        String entityKeyFieldName = "test.key";
        ProfileGroup profileGroup1 = new ProfileGroup(groupName, entityKeyFieldName);
        ProfileGroup profileGroup2 = new ProfileGroup(groupName, entityKeyFieldName);
        profileGroup1.merge(profileGroup2);
        Assert.assertEquals(0, profileGroup1.getMembers().size());
    }

    @Test(expected=IllegalStateException.class)
    public void testRatioNumeratorDoesNotExist() {
        String profileGroupName = "my_profile_group";
        String entityKeyName = "key";
        ProfileGroup profileGroup = new ProfileGroup(profileGroupName, entityKeyName);
        profileGroup.addRatio("result", "doesnt_exist", "doesnt matter");
    }

    @Test(expected=IllegalStateException.class)
    public void testRatioDenominatorDoesNotExist() {
        String profileGroupName = "my_profile_group";
        String entityKeyName = "key";
        ProfileGroup profileGroup = new ProfileGroup(profileGroupName, entityKeyName);
        String numeratorFieldName = "numerator";
        profileGroup.addCount(numeratorFieldName, "key");
        profileGroup.addRatio("result", numeratorFieldName, "doesnt exist");
    }

    private void addLogEvent(String entityKeyFieldName, String entityKey, ProfileGroup profileGroup,Map<String, Object> fields) {
        LogEvent logEvent = new LogEvent(fields);
        logEvent.setField(entityKeyFieldName, entityKey);
        profileGroup.add(logEvent);
        Assert.assertEquals(entityKey, profileGroup.getEntityKey());
    }

    private void verifyProfileEvent(ProfileGroup profileGroup, String entityKey, Map<String, Double> expectedMeasurements) {
        ProfileEvent profileEvent = profileGroup.getProfileEventResult();
        Assert.assertEquals(expectedMeasurements, profileEvent.getMeasurements());
        Assert.assertEquals(entityKey, profileEvent.getEntityKey());
    }

}
