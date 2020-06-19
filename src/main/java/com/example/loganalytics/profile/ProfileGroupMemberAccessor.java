package com.example.loganalytics.profile;

import com.example.loganalytics.event.serialization.TimeseriesEvent;
import lombok.ToString;

import java.util.Optional;
import java.util.function.Function;

@ToString(callSuper=true)
public class ProfileGroupMemberAccessor<T extends TimeseriesEvent> implements Function<ProfileGroup<T>, Double> {

    private final String memberName;

    public ProfileGroupMemberAccessor(String memberName) {
        this.memberName = memberName;
    }

    @Override
    public Double apply(ProfileGroup<T> profileGroup) {
        Optional<ProfileAccumulator<T>> profileAccumulator = profileGroup.getMembers().stream().filter(profile -> profile.getResultName().equals(memberName)).findFirst();
        Double memberValue = null;

        if (profileAccumulator.isPresent()) {
            memberValue = profileAccumulator.get().getResult();
        }
        return memberValue;
    }
}
