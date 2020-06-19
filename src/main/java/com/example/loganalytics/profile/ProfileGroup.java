package com.example.loganalytics.profile;

import com.example.loganalytics.event.*;
import com.example.loganalytics.event.serialization.TimeseriesEvent;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

@ToString
@EqualsAndHashCode(callSuper = true)
@Data
public class ProfileGroup<T extends  TimeseriesEvent> extends TimeseriesEvent {
    private static final Logger LOG = LoggerFactory.getLogger(ProfileGroup.class);
    public static final String RATIO_NUMERATOR_DOES_NOT_EXIST_ERROR_MESSAGE = "Ratio numerator %s does not exist.";
    public static final String RATIO_DENOMINATOR_DOES_NOT_EXIST_ERROR_MESSAGE = "Ratio denominator %s does not exist.";
    public static final String RATIO_DENOMINATOR_IS_ZERO_ERROR_MESSAGE = "Ratio denominator %s is zero.";

    private String profileGroupName;
    private Function<T,String> entityKeyFieldAccessor;
    private String entityKey = "global";
    private List<ProfileAccumulator<T>> members = new ArrayList<>();
    private List<ProfileRatioResult> ratios = new ArrayList<>();
    private long beginPeriodTimeStamp = Long.MAX_VALUE;
    private long endPeriodTimestamp = 0;

    public ProfileGroup(String profileGroupName, Function<T, String> entityKeyFieldAccessor) {
        this.profileGroupName = profileGroupName;
        this.entityKeyFieldAccessor = entityKeyFieldAccessor;
    }

    public ProfileGroup<T> addCountDistinctStringList(String resultName, Function<T, List<String>> fieldAccessor) {
        members.add(new CountDistinctStringListAccumulator<>(resultName, fieldAccessor));

        return this;
    }

    public ProfileGroup<T> addCountDistinctString(String resultName, Function<T, String> fieldAccessor) {
        members.add(new CountDistinctStringAccumulator<>(resultName, fieldAccessor));

        return this;
    }

    public ProfileGroup<T> addCountIf(String resultName, Function<T, String> fieldAccessor, Predicate<String> countPredicate) {
        members.add(new CountIfProfileAccumulator<>(resultName, fieldAccessor, countPredicate));

        return this;
    }

    public ProfileGroup<T> addCount(String resultName) {
        members.add(new CountProfileAccumulator<>(resultName));

        return this;
    }

    public ProfileGroup<T> addMaximum(String resultName, Function<T, Double> fieldAccessor) {
        members.add(new MaxProfileAccumulator<>(resultName, fieldAccessor));

        return this;
    }

    public ProfileGroup<T> addTopFrequency(String resultName, Function<T, String> fieldAccessor) {
        members.add(new TopFrequencyAccumulator<>(resultName, fieldAccessor));

        return this;
    }

    public ProfileGroup<T> addRatio(String resultName, String numeratorProfileName, String denominatorProfileName) {
        ratios.add(new ProfileRatioResult(resultName, numeratorProfileName, denominatorProfileName));

        return this;
    }

    public Double getMemberResult(String name) {

        Optional<ProfileAccumulator<T>> accumulator = members.stream().filter(profile -> profile.getResultName().equals(name)).findFirst();
        Double result = null;
        if (accumulator.isPresent()) {
            result = accumulator.get().getResult();
        }
        return result;
    }

    public String getProfileGroupName() {
        return profileGroupName;
    }

    public String getEntityKey() {
        return entityKey;
    }

    public void add(T event) {
        LOG.debug("Adding to group {} event {}", profileGroupName, event.toString());

        endPeriodTimestamp = Math.max(endPeriodTimestamp,event.getEndTimestamp());
        beginPeriodTimeStamp = Math.min(beginPeriodTimeStamp, event.getBeginTimestamp());

        String entityKeyFieldValue = entityKeyFieldAccessor.apply(event);
        if (entityKeyFieldValue != null) {
            entityKey = entityKeyFieldValue;
        }
        for(ProfileAccumulator<T> profile : members) {
            profile.add(event);
        }
    }

    public void merge(ProfileGroup<T> other) {
        if (this != other) {

            this.endPeriodTimestamp = Math.max(this.endPeriodTimestamp, other.endPeriodTimestamp);
            this.beginPeriodTimeStamp = Math.min(this.beginPeriodTimeStamp, other.endPeriodTimestamp);

            Iterator<ProfileAccumulator<T>> thisMembers = members.iterator();
            Iterator<ProfileAccumulator<T>> otherMembers = other.members.iterator();

            while (thisMembers.hasNext() && otherMembers.hasNext()) {
                ProfileAccumulator<T> thisProfile = thisMembers.next();
                ProfileAccumulator<T> otherProfile = otherMembers.next();
                thisProfile.merge(otherProfile);
            }
        }
    }

    public ProfileEvent getProfileEventResult() {
        LOG.info("Getting result for profileGroup {}.{}",profileGroupName, entityKey);
        ProfileEvent profileEvent = new ProfileEvent(profileGroupName, entityKey, beginPeriodTimeStamp, endPeriodTimestamp);

        for(ProfileAccumulator<T> profile : members) {
            profileEvent.setMeasurement(profile.getResultName(), profile.getResult());
        }

        for(ProfileRatioResult ratio : ratios) {
            addRatioToProfileEvent(profileEvent, ratio);
        }

        return profileEvent;
    }

    private void addRatioToProfileEvent(ProfileEvent profileEvent, ProfileRatioResult ratio) {
        Double numeratorResult = getMemberResult(ratio.getNumerator());
        Double denominatorResult = getMemberResult(ratio.getDenominator());
        String ratioName = ratio.getResultName();

        if (numeratorResult == null) {
            profileEvent.reportError(ratioName, profileGroupName, String.format(RATIO_NUMERATOR_DOES_NOT_EXIST_ERROR_MESSAGE, ratio.getNumerator()));
        } else if (denominatorResult == null) {
            profileEvent.reportError(ratioName, profileGroupName, String.format(RATIO_DENOMINATOR_DOES_NOT_EXIST_ERROR_MESSAGE, ratio.getDenominator()));
        } else {
            if (denominatorResult == 0.0) {
                profileEvent.reportError(ratioName, profileGroupName, String.format(RATIO_DENOMINATOR_IS_ZERO_ERROR_MESSAGE, ratio.getDenominator()));
            } else {
                profileEvent.setMeasurement(ratioName, numeratorResult / denominatorResult);
            }
        }
    }


    @JsonIgnore
    @Override
    public long getTimestamp() {
        return endPeriodTimestamp;
    }

    @JsonIgnore
    @Override
    public long getBeginTimestamp() {
        return beginPeriodTimeStamp;
    }

    @JsonIgnore
    @Override
    public long getEndTimestamp() {
        return endPeriodTimestamp;
    }
}
