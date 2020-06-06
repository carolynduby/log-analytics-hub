package com.example.loganalytics.event;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class ProfileEvent extends LogEvent {
    public static final String PROFILE_TYPE_FIELD_NAME = "profile.type";
    public static final  String PROFILE_ENTITY_KEY_FILED_NAME = "profile.entity_key";
    public static final String PROFILE_MEASUREMENT_FIELD_NAME_PREFIX = "profile.measurement.";

    public ProfileEvent(String profileType, String entityKey) {
        setField(PROFILE_TYPE_FIELD_NAME, profileType);
        setField(PROFILE_ENTITY_KEY_FILED_NAME, entityKey);
    }

    public void setMeasurement(String name, Double value) {
        setField(PROFILE_MEASUREMENT_FIELD_NAME_PREFIX.concat(name), value);
    }

    @JsonIgnore
    public String getEntityKey() {
        return (String)getField(PROFILE_ENTITY_KEY_FILED_NAME);
    }

}
