package com.example.loganalytics.pipeline.profiles;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class ProfileEvent {
    private String profileType;
    private String entityKey;
    private Map<String, Double> measurements = new HashMap<>();

    public ProfileEvent(String profileType, String entityKey) {
        this.profileType = profileType;
        this.entityKey = entityKey;
    }

    public void setMeasurement(String name, Double value) {
        if (value != null) {
            measurements.put(name, value);
        }
    }

    public String getEntityKey() {
        return entityKey;
    }

}
