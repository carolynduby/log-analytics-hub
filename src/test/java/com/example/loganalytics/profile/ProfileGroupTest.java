package com.example.loganalytics.profile;

import com.example.loganalytics.event.ProfileEvent;
import com.example.loganalytics.event.serialization.TimeseriesEvent;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;

public class ProfileGroupTest {

    private static final String PROFILE_GROUP_NAME = "test_group";
    private static final String TEST_ENTITY_KEY = "test_key";
    private static final String TEST_ENTITY_MEASUREMENT = "test_measurement";
    private static final String NUMERATOR_NAME = "numerator";
    private static final String DENOMINATOR_NAME = "denominator";

    @EqualsAndHashCode(callSuper = true)
    @AllArgsConstructor
    @Data
    static class ProfileGroupTestEvent extends TimeseriesEvent {
        private String keyField;
        private String stringField;
        private List<String> listField;
        private long timestamp;

    }

    @Test
    public void testProfileGroupName() {
        ProfileGroup<ProfileGroupTestEvent> profileGroup = new ProfileGroup<>(PROFILE_GROUP_NAME, ProfileGroupTestEvent::getKeyField);
        Assert.assertEquals(PROFILE_GROUP_NAME, profileGroup.getProfileGroupName());
    }

    @Test
    public void testCountDistinctString() {
        ProfileGroup<ProfileGroupTestEvent> profileGroup = new ProfileGroup<>(PROFILE_GROUP_NAME, ProfileGroupTestEvent::getKeyField);
        profileGroup.addCountDistinctString(TEST_ENTITY_MEASUREMENT, ProfileGroupTestEvent::getStringField);

        addEventToProfileGroup(profileGroup, TEST_ENTITY_KEY, "string 1", Collections.emptyList());
        verifyProfileEvent(profileGroup, TEST_ENTITY_KEY, 1.0);

        addEventToProfileGroup(profileGroup, TEST_ENTITY_KEY, "string 2", Collections.emptyList());
        addEventToProfileGroup(profileGroup, TEST_ENTITY_KEY, "string 2", Collections.emptyList());
        verifyProfileEvent(profileGroup, TEST_ENTITY_KEY, 2.0);
    }

    @Test
    public void testCountDistinctStringList() {
        ProfileGroup<ProfileGroupTestEvent> profileGroup = new ProfileGroup<>(PROFILE_GROUP_NAME, ProfileGroupTestEvent::getKeyField);
        profileGroup.addCountDistinctStringList(TEST_ENTITY_MEASUREMENT, ProfileGroupTestEvent::getListField);

        addEventToProfileGroup(profileGroup, TEST_ENTITY_KEY, null, Arrays.asList("string 1", "string 2", "string 2"));

        verifyProfileEvent(profileGroup, TEST_ENTITY_KEY, 2.0);
    }

    @Test
    public void testCountIf() {
        ProfileGroup<ProfileGroupTestEvent> profileGroup = new ProfileGroup<>(PROFILE_GROUP_NAME, ProfileGroupTestEvent::getKeyField);

        final String matchingString = "match";
        profileGroup.addCountIf(TEST_ENTITY_MEASUREMENT, ProfileGroupTestEvent::getStringField,  Predicate.isEqual(matchingString));

        // event with false predicate does not increment
        addEventToProfileGroup(profileGroup, TEST_ENTITY_KEY, "no match", Collections.emptyList());
        verifyProfileEvent(profileGroup, TEST_ENTITY_KEY, 0.0);

        // event with true predicate increments
        addEventToProfileGroup(profileGroup, TEST_ENTITY_KEY, matchingString, Collections.emptyList());
        verifyProfileEvent(profileGroup, TEST_ENTITY_KEY, 1.0);
    }


    @Test
    public void testTopFrequency() {
        ProfileGroup<ProfileGroupTestEvent> profileGroup = new ProfileGroup<>(PROFILE_GROUP_NAME, ProfileGroupTestEvent::getKeyField);

        final String matchingString = "match";
        profileGroup.addTopFrequency(TEST_ENTITY_MEASUREMENT, ProfileGroupTestEvent::getStringField);

        // add the same string multiple times
        int topFrequency = 5;
        for(int count = 0; count < topFrequency; count++) {
            addEventToProfileGroup(profileGroup, TEST_ENTITY_KEY, matchingString, Collections.emptyList());
            verifyProfileEvent(profileGroup, TEST_ENTITY_KEY,  count + 1.0);
        }

        // add different string, top frequency should not change
        addEventToProfileGroup(profileGroup, TEST_ENTITY_KEY, "no match", Collections.emptyList());
        verifyProfileEvent(profileGroup, TEST_ENTITY_KEY, (double)topFrequency);
    }


    @Test
    public void testRatios() {
        ProfileGroup<ProfileGroupTestEvent> profileGroup = new ProfileGroup<>(PROFILE_GROUP_NAME, ProfileGroupTestEvent::getKeyField);

        profileGroup.addCount(NUMERATOR_NAME).
                addCountDistinctStringList(DENOMINATOR_NAME, ProfileGroupTestEvent::getListField).
                addRatio(TEST_ENTITY_MEASUREMENT, NUMERATOR_NAME, DENOMINATOR_NAME);

       addEventToProfileGroup(profileGroup, TEST_ENTITY_KEY, null, Arrays.asList("string 1", "string 2"));
       verifyRatioProfileEvent(profileGroup, 1.0, 2.0);
    }


    @Test
    public void testMergeTwoNonEmptyProfileGroups() {
        ProfileGroup<ProfileGroupTestEvent> profileGroup1 = createMergeProfileGroup();
        ProfileGroup<ProfileGroupTestEvent> profileGroup2 = createMergeProfileGroup();
        profileGroup1.merge(profileGroup2);
        verifyProfileEvent(profileGroup1, TEST_ENTITY_KEY, 2.0);
        verifyProfileEvent(profileGroup2, TEST_ENTITY_KEY, 1.0);
    }

    private ProfileGroup<ProfileGroupTestEvent> createMergeProfileGroup() {

        ProfileGroup<ProfileGroupTestEvent> profileGroup = new ProfileGroup<>(PROFILE_GROUP_NAME, ProfileGroupTestEvent::getKeyField);
        profileGroup.addCount(TEST_ENTITY_MEASUREMENT);
        addEventToProfileGroup(profileGroup, TEST_ENTITY_KEY, "A", Collections.emptyList());

        return profileGroup;
    }


    @Test
    public void testMergeTwoEmptyProfileGroups() {

        ProfileGroup<ProfileGroupTestEvent> profileGroup1 = new ProfileGroup<>(PROFILE_GROUP_NAME, ProfileGroupTestEvent::getKeyField);
        ProfileGroup<ProfileGroupTestEvent> profileGroup2 = new ProfileGroup<>(PROFILE_GROUP_NAME, ProfileGroupTestEvent::getKeyField);
        profileGroup1.merge(profileGroup2);
        Assert.assertEquals(0, profileGroup1.getMembers().size());
    }

    @Test
    public void testRatioNumeratorDoesNotExist() {
        ProfileGroup<ProfileGroupTestEvent> profileGroup = new ProfileGroup<>(PROFILE_GROUP_NAME, ProfileGroupTestEvent::getKeyField);
        profileGroup.addRatio(TEST_ENTITY_MEASUREMENT, NUMERATOR_NAME, "doesnt matter");
        ProfileEvent profileEvent = profileGroup.getProfileEventResult();
        Collection<String> errors  = profileEvent.getErrors();
        Assert.assertEquals(1, errors.size());
        String expectedErrorMessage = String.format(ProfileGroup.RATIO_NUMERATOR_DOES_NOT_EXIST_ERROR_MESSAGE, NUMERATOR_NAME);
        Assert.assertTrue(errors.iterator().next().endsWith(expectedErrorMessage));
    }


    @Test
    public void testRatioDenominatorDoesNotExist() {
        ProfileGroup<ProfileGroupTestEvent> profileGroup = new ProfileGroup<>(PROFILE_GROUP_NAME, ProfileGroupTestEvent::getKeyField);
        profileGroup.addCount(NUMERATOR_NAME).
                addRatio(TEST_ENTITY_MEASUREMENT, NUMERATOR_NAME, DENOMINATOR_NAME);
        ProfileEvent profileEvent = profileGroup.getProfileEventResult();
        Collection<String> errors  = profileEvent.getErrors();
        Assert.assertEquals(1, errors.size());
        String expectedErrorMessage = String.format(ProfileGroup.RATIO_DENOMINATOR_DOES_NOT_EXIST_ERROR_MESSAGE, DENOMINATOR_NAME);
        Assert.assertTrue(errors.iterator().next().endsWith(expectedErrorMessage));
    }

    @Test
    public void testRatioDivideByZero() {
        ProfileGroup<ProfileGroupTestEvent> profileGroup = new ProfileGroup<>(PROFILE_GROUP_NAME, ProfileGroupTestEvent::getKeyField);
        profileGroup.addCount(NUMERATOR_NAME).addCount(DENOMINATOR_NAME).
                addRatio(TEST_ENTITY_MEASUREMENT, NUMERATOR_NAME, DENOMINATOR_NAME);
        ProfileEvent profileEvent = profileGroup.getProfileEventResult();
        Collection<String> errors  = profileEvent.getErrors();
        Assert.assertEquals(1, errors.size());
        String expectedErrorMessage = String.format(ProfileGroup.RATIO_DENOMINATOR_IS_ZERO_ERROR_MESSAGE, DENOMINATOR_NAME);
        Assert.assertTrue(errors.iterator().next().endsWith(expectedErrorMessage));
    }

    private void addEventToProfileGroup(ProfileGroup<ProfileGroupTestEvent> profileGroup, String entityKey, String stringField, List<String> listField) {
        ProfileGroupTestEvent logEvent = new ProfileGroupTestEvent(entityKey, stringField, listField, Instant.now().getEpochSecond());
        profileGroup.add(logEvent);
        Assert.assertEquals(entityKey, profileGroup.getEntityKey());
    }

    private void verifyProfileEvent(ProfileGroup<ProfileGroupTestEvent> profileGroup, String entityKey, Double expectedMeasurementValue) {
        Map<String, Object> expectedFields = initFields(PROFILE_GROUP_NAME, TEST_ENTITY_KEY, TEST_ENTITY_MEASUREMENT, expectedMeasurementValue);
        ProfileEvent profileEvent = profileGroup.getProfileEventResult();
        Assert.assertEquals(expectedFields, profileEvent.getFields());
        Assert.assertEquals(entityKey, profileEvent.getEntityKey());
    }

    private Map<String, Object> initFields(String profileType, String entityKey, String measurementName, Double measurementValue) {
        Map<String, Object> fields = new HashMap<>();

        fields.put(ProfileEvent.PROFILE_TYPE_FIELD_NAME, profileType);
        fields.put(ProfileEvent.PROFILE_ENTITY_KEY_FILED_NAME, entityKey);
        fields.put(ProfileEvent.PROFILE_MEASUREMENT_FIELD_NAME_PREFIX.concat(measurementName), measurementValue);

        return fields;
    }

    private void verifyRatioProfileEvent(ProfileGroup<ProfileGroupTestEvent> profileGroup, Double numerator, Double denominator) {
        Map<String, Object> expectedFields = initRatioFields(TEST_ENTITY_KEY, TEST_ENTITY_MEASUREMENT, numerator, denominator);
        ProfileEvent profileEvent = profileGroup.getProfileEventResult();
        Assert.assertEquals(expectedFields, profileEvent.getFields());
        Assert.assertEquals(ProfileGroupTest.TEST_ENTITY_KEY, profileEvent.getEntityKey());
    }

    private Map<String, Object> initRatioFields(String entityKey, String measurementName, Double numerator, Double denominator) {
        Map<String, Object> fields = new HashMap<>();

        fields.put(ProfileEvent.PROFILE_TYPE_FIELD_NAME, ProfileGroupTest.PROFILE_GROUP_NAME);
        fields.put(ProfileEvent.PROFILE_ENTITY_KEY_FILED_NAME, entityKey);
        fields.put(ProfileEvent.PROFILE_MEASUREMENT_FIELD_NAME_PREFIX.concat(NUMERATOR_NAME), numerator);
        fields.put(ProfileEvent.PROFILE_MEASUREMENT_FIELD_NAME_PREFIX.concat(DENOMINATOR_NAME), denominator);
        fields.put(ProfileEvent.PROFILE_MEASUREMENT_FIELD_NAME_PREFIX.concat(measurementName), numerator / denominator);

        return fields;
    }

}
