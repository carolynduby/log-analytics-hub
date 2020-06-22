package com.example.loganalytics.event.serialization;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;

public abstract class TimeseriesEvent {

    @JsonIgnore
    public long getBeginTimestamp()  {
        return getTimestamp();
    }

    @JsonIgnore
    public long getEndTimestamp() {
        return getTimestamp();
    }

    @JsonIgnore
    public abstract long getTimestamp();

    @JsonIgnore
    public static long getCurrentTime() {
        return Instant.now().toEpochMilli();
    }
}
