package com.example.loganalytics.pipeline.profiles.dns;


import com.example.loganalytics.event.DnsRequest;
import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.pipeline.profiles.ProfileEvent;
import com.example.loganalytics.profile.ProfileGroup;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.flink.api.common.functions.AggregateFunction;

@EqualsAndHashCode
@ToString
public class DnsFingerprintAggregator implements AggregateFunction<LogEvent, ProfileGroup, ProfileEvent> {

    @Override
    public ProfileGroup createAccumulator() {
        return DnsRequest.createDnsFingerPrintProfileGroup();
    }

    @Override
    public ProfileGroup add(LogEvent logEvent, ProfileGroup profileGroup) {
        profileGroup.add(logEvent);
        return profileGroup;
    }

    @Override
    public ProfileEvent getResult(ProfileGroup profileGroup) {
        return profileGroup.getProfileEventResult();
    }

    @Override
    public ProfileGroup merge(ProfileGroup profileGroup1, ProfileGroup profileGroup2) {
        profileGroup1.merge(profileGroup2);

        return profileGroup1;
    }
}
