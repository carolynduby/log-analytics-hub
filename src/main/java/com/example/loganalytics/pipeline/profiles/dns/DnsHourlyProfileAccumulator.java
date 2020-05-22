package com.example.loganalytics.pipeline.profiles.dns;

import com.example.loganalytics.event.DnsRequest;
import com.example.loganalytics.pipeline.profiles.ProfileEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DnsHourlyProfileAccumulator {
    private static final Logger LOG = LoggerFactory.getLogger(DnsHourlyProfileAccumulator.class);
    private String entityKey = "";
    private double requestsCountSum = 0.0;
    private double numEvents = 0.0;
    private double maxRequestCount = 0.0;


    DnsHourlyProfileAccumulator add(ProfileEvent profileEvent) {
        LOG.info("Adding hourly event {}", profileEvent);
        LOG.info("Before req_count = {}, num_events = {}, max_req = {}", requestsCountSum, numEvents, maxRequestCount);
        entityKey = profileEvent.getEntityKey();
        Double requestCount = profileEvent.getMeasurements().get(DnsRequest.DnsFingerprintProfileMeasurements.P1.name());
        requestsCountSum += requestCount;
        numEvents++;
        maxRequestCount = Math.max(maxRequestCount, requestCount);

        return this;
    }

    DnsHourlyProfileAccumulator merge(DnsHourlyProfileAccumulator other) {
        LOG.info("Merging {}", entityKey);
        this.requestsCountSum += other.requestsCountSum;
        this.numEvents += other.numEvents;

        return this;
    }

    ProfileEvent getResult() {
        ProfileEvent profileEvent = new ProfileEvent("dns_hourly", entityKey);
        profileEvent.setMeasurement(DnsRequest.DnsFingerprintProfileMeasurements.P4.name(), requestsCountSum / numEvents);
        profileEvent.setMeasurement(DnsRequest.DnsFingerprintProfileMeasurements.P5.name(), maxRequestCount);
        LOG.info("Result {}", profileEvent);
        return profileEvent;
    }

}
