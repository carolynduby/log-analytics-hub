package com.example.loganalytics.pipeline.profiles.dns;

import com.example.loganalytics.event.DnsRequestEvent;
import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.ProfileEvent;
import com.example.loganalytics.profile.AggregatedProfileGroup;
import com.example.loganalytics.profile.ProfileGroup;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.flink.api.common.functions.AggregateFunction;

@EqualsAndHashCode
@ToString
public class DnsHourProfileAggregator implements AggregateFunction<ProfileGroup<LogEvent>, AggregatedProfileGroup<LogEvent>, ProfileEvent> {
    @Override
    public AggregatedProfileGroup<LogEvent> createAccumulator() {
        return DnsRequestEvent.createDnsFingerPrintHourProfileGroup();
    }

    @Override
    public AggregatedProfileGroup<LogEvent> add(ProfileGroup<LogEvent> logEvent, AggregatedProfileGroup<LogEvent> profileGroup) {
        profileGroup.add(logEvent);
        return profileGroup;
    }

    @Override
    public ProfileEvent getResult(AggregatedProfileGroup<LogEvent> profileGroup) {
        return profileGroup.getProfileEventResult();
    }

    @Override
    public AggregatedProfileGroup<LogEvent> merge(AggregatedProfileGroup<LogEvent> acc1, AggregatedProfileGroup<LogEvent> acc2) {
        acc1.merge(acc2);
        return acc1;
    }

}
