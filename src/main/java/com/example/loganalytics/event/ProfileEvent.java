package com.example.loganalytics.event;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.Instant;

@EqualsAndHashCode(callSuper = true)
@Data
public class ProfileEvent extends LogEvent {
    public static final String PROFILE_TYPE_FIELD_NAME = "profile.type";
    public static final  String PROFILE_ENTITY_KEY_FILED_NAME = "profile.entity_key";
    public static final String PROFILE_MEASUREMENT_FIELD_NAME_PREFIX = "profile.measurement.";
    public static final String PROFILE_BEGIN_PERIOD_TIMESTAMP = "profile.begin_period";
    public static final String PROFILE_END_PERIOD_TIMESTAMP = "profile.end.period";


    public ProfileEvent(String profileType, String entityKey, long beginPeriodTimestamp, long endPeriodTimestamp) {
        setField(TIMESTAMP_FIELD, Instant.now().toEpochMilli());
        setField(PROFILE_TYPE_FIELD_NAME, profileType);
        setField(PROFILE_ENTITY_KEY_FILED_NAME, entityKey);
        setField(PROFILE_BEGIN_PERIOD_TIMESTAMP, beginPeriodTimestamp);
        setField(PROFILE_END_PERIOD_TIMESTAMP, endPeriodTimestamp);
    }

    public void setMeasurement(String name, Double value) {
        setField(PROFILE_MEASUREMENT_FIELD_NAME_PREFIX.concat(name), value);
    }

    @JsonIgnore
    public String getEntityKey() {
        return (String)getField(PROFILE_ENTITY_KEY_FILED_NAME);
    }

}
