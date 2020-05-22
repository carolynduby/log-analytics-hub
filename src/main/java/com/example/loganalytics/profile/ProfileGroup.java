package com.example.loganalytics.profile;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventFieldSpecification;
import com.example.loganalytics.pipeline.profiles.ProfileEvent;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

@Data
public class ProfileGroup {
    private static final Logger LOG = LoggerFactory.getLogger(ProfileGroup.class);

    private String profileGroupName;
    private LogEventFieldSpecification entityKeyFieldSpecification;
    private String entityKey = "global";
    private List<Profile> members = new ArrayList<>();
    private List<ProfileRatioResult> ratios = new ArrayList<>();

    public ProfileGroup(String profileGroupName, String entityKeyFieldName) {
        this.profileGroupName = profileGroupName;
        this.entityKeyFieldSpecification = new LogEventFieldSpecification(entityKeyFieldName, profileGroupName, false);
    }

    public void addMember(Profile profile) {
        members.add(profile);
    }

    public ProfileGroup addCountDistinct(String resultName, String fieldName, boolean isMultivalued) {
        CountDistinctAccumulator accumulator = new CountDistinctAccumulator();
        addProfile(resultName, fieldName, accumulator, isMultivalued);

        return this;
    }

    public ProfileGroup addCountIf(String resultName, String fieldName, Predicate<String> countPredicate) {
        CountIfProfileAccumulator accumulator = new CountIfProfileAccumulator(countPredicate);
        addProfile(resultName, fieldName, accumulator, false);

        return this;
    }

    public ProfileGroup addCount(String resultName, String fieldName) {
        CountProfileAccumulator accumulator = new CountProfileAccumulator();
        addProfile(resultName, fieldName, accumulator, false);

        return this;
    }

    public ProfileGroup addTopFrequency(String resultName, String fieldName) {
        TopFrequencyAccumulator accumulator = new TopFrequencyAccumulator();
        addProfile(resultName, fieldName, accumulator, false);

        return this;
    }

    public ProfileGroup addRatio(String resultName, String numeratorProfileName, String denominatorProfileName) {
        Profile numerator = members.stream().filter(profile -> profile.getResultName().equals(numeratorProfileName)).findFirst().
                orElseThrow(() -> new IllegalStateException(String.format("Ratio result %s.%s is not defined", this.profileGroupName, numeratorProfileName)));
        Profile denominator = members.stream().filter(profile -> profile.getResultName().equals(denominatorProfileName)).findFirst().
                orElseThrow(() -> new IllegalStateException(String.format("Ratio result %s.%s is not defined", this.profileGroupName, denominatorProfileName)));

        ratios.add(new ProfileRatioResult(resultName, numerator, denominator));

        return this;
    }

    private void addProfile(String resultName, String fieldName, ProfileAccumulator accumulator, boolean isMultivalued) {
        LogEventFieldSpecification fieldSpecification = new LogEventFieldSpecification(fieldName, profileGroupName, false);
        if (isMultivalued) {
            addMember(new MultiValueProfile(resultName, fieldSpecification, accumulator));
        } else {
            addMember(new SingleValueProfile(resultName, fieldSpecification, accumulator));
        }
    }

    public String getProfileGroupName() {
        return profileGroupName;
    }

    public String getEntityKey() {
        return entityKey;
    }

    public void add(LogEvent logEvent) {
        LOG.debug("Adding to group {} event {}", profileGroupName, logEvent.toString());
        String entityKeyFieldValue = logEvent.getField(entityKeyFieldSpecification, String.class);
        if (entityKeyFieldValue != null) {
            entityKey = entityKeyFieldValue;
        }
        for(Profile profile : members) {
            profile.add(logEvent);
        }
    }

    public void merge(ProfileGroup other) {
        if (this != other) {
            Iterator<Profile> thisMembers = members.iterator();
            Iterator<Profile> otherMembers = other.members.iterator();

            while (thisMembers.hasNext() && otherMembers.hasNext()) {
                Profile thisProfile = thisMembers.next();
                Profile otherProfile = otherMembers.next();
                thisProfile.merge(otherProfile);
            }
        }
    }

    public ProfileEvent getProfileEventResult() {
        LOG.info("Getting result for profileGroup {}.{}",profileGroupName, entityKey);
        ProfileEvent profileEvent = new ProfileEvent(profileGroupName, entityKey);
        for(Profile profile : members) {
            profileEvent.setMeasurement(profile.getResultName(), profile.getResult());
        }

        for(ProfileRatioResult ratio : ratios) {
            profileEvent.setMeasurement(ratio.getResultName(), ratio.getResult());
        }

        return profileEvent;
    }
}
