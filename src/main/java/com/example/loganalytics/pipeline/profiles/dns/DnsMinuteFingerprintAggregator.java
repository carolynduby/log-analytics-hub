package com.example.loganalytics.pipeline.profiles.dns;


import com.example.loganalytics.event.DnsRequestEvent;
import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.profile.ProfileGroup;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.flink.api.common.functions.AggregateFunction;

@EqualsAndHashCode
@ToString
public class DnsMinuteFingerprintAggregator implements AggregateFunction<LogEvent, ProfileGroup<LogEvent>, ProfileGroup<LogEvent>> {

    @Override
    public ProfileGroup<LogEvent> createAccumulator() {
        return DnsRequestEvent.createDnsFingerPrintMinuteProfileGroup();
    }

    @Override
    public ProfileGroup<LogEvent> add(LogEvent logEvent, ProfileGroup<LogEvent> profileGroup) {
        profileGroup.add(logEvent);
        return profileGroup;
    }

    @Override
    public ProfileGroup<LogEvent> getResult(ProfileGroup<LogEvent> profileGroup) {
        return profileGroup;
    }

    @Override
    public ProfileGroup<LogEvent> merge(ProfileGroup<LogEvent> profileGroup1, ProfileGroup<LogEvent> profileGroup2) {
        profileGroup1.merge(profileGroup2);

        return profileGroup1;
    }
}
