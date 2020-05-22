package com.example.loganalytics.pipeline.profiles.dns;

import com.example.loganalytics.pipeline.profiles.ProfileEvent;
import org.apache.flink.api.common.functions.AggregateFunction;


public class DnsHourlyProfileAggregator implements AggregateFunction<ProfileEvent, DnsHourlyProfileAccumulator, ProfileEvent> {
    @Override
    public DnsHourlyProfileAccumulator createAccumulator() {
        return new DnsHourlyProfileAccumulator();
    }

    @Override
    public DnsHourlyProfileAccumulator add(ProfileEvent profileEvent, DnsHourlyProfileAccumulator dnsHourlyProfileAccumulator) {
        return dnsHourlyProfileAccumulator.add(profileEvent);
    }

    @Override
    public ProfileEvent getResult(DnsHourlyProfileAccumulator dnsHourlyProfileAccumulator) {
        return dnsHourlyProfileAccumulator.getResult();
    }

    @Override
    public DnsHourlyProfileAccumulator merge(DnsHourlyProfileAccumulator dnsHourlyProfileAccumulator1, DnsHourlyProfileAccumulator dnsHourlyProfileAccumulator2) {
        return dnsHourlyProfileAccumulator1.merge(dnsHourlyProfileAccumulator2);
    }
}
