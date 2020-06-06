package com.example.loganalytics.profile;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;

public class ProfileGroupMemberAccessorTest {

    @Data
    @AllArgsConstructor
    private static class TestEvent {
        private final String field;
    }

    @Test
    public void testAccessMember() {
        String profileName = "count_profile";
        ProfileGroup<TestEvent>  profileGroup = new ProfileGroup<>("group_name", TestEvent::getField);
        profileGroup.addCount(profileName);

        profileGroup.add(new TestEvent("A"));

        ProfileGroupMemberAccessor<TestEvent> accessor = new ProfileGroupMemberAccessor<>(profileName);
        Assert.assertEquals(1.0, accessor.apply(profileGroup), 0.1);
    }

    @Test
    public void testAccessMemberDoesNotExist() {
        // access a member that doesn't exist
        ProfileGroup<TestEvent>  profileGroup = new ProfileGroup<>("group_name", TestEvent::getField);

        ProfileGroupMemberAccessor<TestEvent> accessor = new ProfileGroupMemberAccessor<>("not defined");
        Assert.assertNull(accessor.apply(profileGroup));

    }

}
