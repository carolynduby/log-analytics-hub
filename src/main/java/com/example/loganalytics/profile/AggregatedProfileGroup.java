package com.example.loganalytics.profile;

import com.example.loganalytics.event.ProfileEvent;
import com.example.loganalytics.event.serialization.TimeseriesEvent;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ToString(callSuper=true)
@EqualsAndHashCode(callSuper = true)
public class AggregatedProfileGroup<PARENT_T extends TimeseriesEvent> extends ProfileGroup<ProfileGroup<PARENT_T>> {
    private static final Logger LOG = LoggerFactory.getLogger(AggregatedProfileGroup.class);
    private final ProfileGroup<PARENT_T> parentProfileGroup;

    public AggregatedProfileGroup(String profileGroupName, ProfileGroup<PARENT_T> parentProfileGroup) {
        super(profileGroupName, ProfileGroup::getEntityKey);
        this.parentProfileGroup = parentProfileGroup;
    }

    public Double getMemberResult(String name) {
        Double result = super.getMemberResult(name);
        if (result == null) {
            result = parentProfileGroup.getMemberResult(name);
        }
        return result;
    }

    public void add(ProfileGroup<PARENT_T> event) {
        LOG.debug("Adding to group {} event {}", getProfileGroupName(), event.toString());
        super.add(event);
        parentProfileGroup.merge(event);
    }

    public void merge(AggregatedProfileGroup<PARENT_T>  other) {
        if (this != other && other != null) {
            super.merge(other);
            parentProfileGroup.merge(other.parentProfileGroup);
        }
    }

    public ProfileEvent getProfileEventResult() {
        ProfileEvent profileEvent = super.getProfileEventResult();

        for(ProfileAccumulator<PARENT_T> parent : parentProfileGroup.getMembers()) {
            profileEvent.setMeasurement(parent.getResultName(), parent.getResult());
        }

        return profileEvent;
    }
}
