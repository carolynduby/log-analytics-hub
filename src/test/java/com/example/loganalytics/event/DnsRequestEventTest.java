package com.example.loganalytics.event;

import com.example.loganalytics.log.sources.LogSource;
import com.example.loganalytics.log.sources.LogSources;
import com.example.loganalytics.log.sources.LogSourcesTest;
import com.example.loganalytics.profile.AggregatedProfileGroup;
import com.example.loganalytics.profile.ProfileGroup;
import com.example.loganalytics.profile.ProfileGroupTest;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@SuppressWarnings("SpellCheckingInspection")
public class DnsRequestEventTest {

    private static final Logger LOG = LoggerFactory.getLogger(DnsRequestEventTest.class);
    private static final String NXDOMAIN_EVENT=   "{\"dns\": {\"ts\":1588709649.439005,\"uid\":\"Cmnfun1hYw3CQeCVHk\",\"id.orig_h\":\"172.31.1.6\",\"id.orig_p\":28000,\"id.resp_h\":\"172.31.1.6\",\"id.resp_p\":53,\"proto\":\"udp\",\"trans_id\":24430,\"query\":\"asia.api.targetingmantra.com\",\"qclass\":1,\"qclass_name\":\"C_INTERNET\",\"qtype\":1,\"qtype_name\":\"A\",\"rcode\":3,\"rcode_name\":\"NXDOMAIN\",\"AA\":false,\"TC\":false,\"RD\":true,\"RA\":false,\"Z\":0,\"rejected\":true}}";
    private static final String VALID_JSON_LINE = "{\"dns\": {\"ts\":1588709647.572953,\"uid\":\"Cwfli23YvRiruLucOh\",\"id.orig_h\":\"172.31.1.6\",\"id.orig_p\":13512,\"id.resp_h\":\"172.31.3.121\",\"id.resp_p\":53,\"proto\":\"udp\",\"trans_id\":41874,\"rtt\":0.002012968063354492,\"query\":\"track-front-prod-825173558.us-east-1.elb.amazonaws.com\",\"qclass\":1,\"qclass_name\":\"C_INTERNET\",\"qtype\":1,\"qtype_name\":\"A\",\"rcode\":0,\"rcode_name\":\"NOERROR\",\"AA\":false,\"TC\":false,\"RD\":true,\"RA\":true,\"Z\":0,\"answers\":[\"23.21.126.97\",\"50.17.222.11\",\"107.22.253.140\",\"54.243.169.159\",\"23.23.144.131\",\"23.23.222.251\",\"174.129.202.115\",\"23.21.127.5\"],\"TTLs\":[30.0,30.0,30.0,30.0,30.0,30.0,30.0,30.0],\"rejected\":false}}";
    private static final String MXQUERY_EVENT =   "{\"dns\": {\"ts\":1588709648.884982,\"uid\":\"CxsMYoLlK4WHUgbcb\",\"id.orig_h\":\"172.31.1.6\",\"id.orig_p\":17831,\"id.resp_h\":\"172.31.3.121\",\"id.resp_p\":53,\"proto\":\"udp\",\"trans_id\":43007,\"query\":\"a2047.dspl.akamai.net\",\"qclass\":1,\"qclass_name\":\"C_INTERNET\",\"qtype\":15,\"qtype_name\":\"MX\",\"rcode\":0,\"rcode_name\":\"NOERROR\",\"AA\":false,\"TC\":false,\"RD\":true,\"RA\":false,\"Z\":0,\"rejected\":false}}";
    private static final String PTRQUERY_EVENT =  "{\"dns\": {\"ts\":1588709649.896947,\"uid\":\"C17Cvi3CBuf0H6JZKb\",\"id.orig_h\":\"172.31.1.6\",\"id.orig_p\":3702,\"id.resp_h\":\"172.31.3.121\",\"id.resp_p\":53,\"proto\":\"udp\",\"trans_id\":29667,\"query\":\"26.216.233.219.in-addr.arpa\",\"qclass\":1,\"qclass_name\":\"C_INTERNET\",\"qtype\":12,\"qtype_name\":\"PTR\",\"rcode\":0,\"rcode_name\":\"NOERROR\",\"AA\":false,\"TC\":false,\"RD\":true,\"RA\":true,\"Z\":0,\"answers\":[\"chipnuts.com\"],\"TTLs\":[70203.0],\"rejected\":false}}";
    private static final String EXPECTED_ENTITY_KEY = "172.31.1.6";
    @Test
    public void testGetters() throws IOException {

        LogSource jsonSource = LogSourcesTest.createLogSource(LogSources.ZEEK_SOURCE_NAME);
        DnsRequestEvent dnsRequestEvent = new DnsRequestEvent(jsonSource.ingestEvent(VALID_JSON_LINE));
        Assert.assertEquals("172.31.1.6", dnsRequestEvent.getSourceIp());
        Assert.assertEquals(13512, dnsRequestEvent.getSourcePort());
        Assert.assertEquals("172.31.3.121", dnsRequestEvent.getDestinationIp());
        Assert.assertEquals(53, dnsRequestEvent.getDestinationPort());
        Assert.assertEquals(1588709647572L, dnsRequestEvent.getTimestamp());
        List<String> answerIps = Lists.newArrayList("23.21.126.97","50.17.222.11","107.22.253.140","54.243.169.159","23.23.144.131","23.23.222.251","174.129.202.115","23.21.127.5");
        Assert.assertEquals(answerIps, dnsRequestEvent.getIpAnswers());
        List<String> cities = Lists.newArrayList("Ashburn");
        Assert.assertEquals(cities, dnsRequestEvent.getAnswerCities());
        List<String> countries = Lists.newArrayList("US");
        Assert.assertEquals(countries, dnsRequestEvent.getAnswerCountries());
        Assert.assertEquals("track-front-prod-825173558.us-east-1.elb.amazonaws.com", dnsRequestEvent.getQuery());
        Assert.assertEquals("amazonaws", dnsRequestEvent.getQuerySecondLevelDomain());
        Assert.assertEquals("com", dnsRequestEvent.getQueryTopLevelDomain());
        Assert.assertEquals("NOERROR", dnsRequestEvent.getResponseCode());
        Assert.assertEquals("A", dnsRequestEvent.getQueryType());

    }

    @Test
    public void testDnsFingerprintProfile() throws IOException {
        LogSource jsonSource = LogSourcesTest.createLogSource(LogSources.ZEEK_SOURCE_NAME);
        ProfileGroup<LogEvent> dnsMinuteProfile = DnsRequestEvent.createDnsFingerPrintMinuteProfileGroup();
        AggregatedProfileGroup<LogEvent> dnsHourProfile = DnsRequestEvent.createDnsFingerPrintHourProfileGroup();

        // add 1 event and verify that the profile measurements are correct
        Map<String, Object> expectedMinuteMeasurements = initMinuteProfileMeasurements();
        Map<String, Object> expectedHourMeasurements = initHourProfileMeasurements();

        verifyAddEventToProfile(jsonSource, VALID_JSON_LINE, dnsMinuteProfile, expectedMinuteMeasurements, dnsHourProfile, expectedHourMeasurements);

        // test an NXDOMAIN event
        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P1.name());
        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P2.name());
        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P8.name());
        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P10.name());
        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P12.name());

        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P1.name()), 3.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P2.name()), 2.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P3.name()), 2.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P4.name()), 3.0/2.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P5.name()), 2.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P8.name()), 2.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P10.name()), 2.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P11.name()), 3.0/2.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P12.name()), 1.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.MINUTES_ACTIVE.name()), 2.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P15.name()), 2.0/8.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.MINUTES_ACTIVE.name()), 2.0);

        verifyAddEventToProfile(jsonSource, NXDOMAIN_EVENT, dnsMinuteProfile, expectedMinuteMeasurements, dnsHourProfile, expectedHourMeasurements);

        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P1.name());
        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P2.name());
        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P6.name());
        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P9.name());
        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P10.name());

        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P1.name()), 6.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P2.name()), 3.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P3.name()), 3.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P4.name()), 6.0/3.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P5.name()), 3.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P6.name()), 1.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P9.name()), 2.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P10.name()), 3.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P11.name()), 6.0/3.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P12.name()), 2.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P15.name()), 3.0/8.0);
        incrementMeasurement(expectedHourMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.MINUTES_ACTIVE.name());

        verifyAddEventToProfile(jsonSource, MXQUERY_EVENT, dnsMinuteProfile, expectedMinuteMeasurements, dnsHourProfile, expectedHourMeasurements);

        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P1.name());
        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P2.name());
        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P7.name());

        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P1.name()), 10.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P2.name()), 4.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P3.name()), 4.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P4.name()), 10.0/4.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P5.name()), 4.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P6.name()), 2.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P7.name()), 1.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P11.name()), 10.0/4.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P12.name()), 3.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P15.name()), 4.0/8.0);
        incrementMeasurement(expectedHourMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.MINUTES_ACTIVE.name());

        verifyAddEventToProfile(jsonSource, PTRQUERY_EVENT, dnsMinuteProfile, expectedMinuteMeasurements, dnsHourProfile, expectedHourMeasurements);

        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P1.name());
        incrementMeasurement(expectedMinuteMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.P3.name());

        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P1.name()), 15.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P3.name()), 6.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P4.name()), 15.0/5.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P5.name()), 5.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P6.name()), 3.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P7.name()), 2.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P11.name()), 15.0/4.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P12.name()), 4.0);
        expectedHourMeasurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P15.name()), 4.0/8.0);
        incrementMeasurement(expectedHourMeasurements, DnsRequestEvent.DnsFingerprintProfileMeasurements.MINUTES_ACTIVE.name());

        verifyAddEventToProfile(jsonSource, VALID_JSON_LINE, dnsMinuteProfile, expectedMinuteMeasurements, dnsHourProfile, expectedHourMeasurements);

    }

    private void verifyAddEventToProfile(LogSource jsonSource, String eventText, ProfileGroup<LogEvent> dnsMinuteProfile, Map<String, Object> expectedMinuteMeasurements,
                                         AggregatedProfileGroup<LogEvent> dnsHourProfile, Map<String, Object> expectedHourMeasurements) {
        LogEvent logEvent = jsonSource.ingestEvent(eventText);
        dnsMinuteProfile.add(logEvent);

        ProfileEvent profileEvent = dnsMinuteProfile.getProfileEventResult();
        Assert.assertEquals(expectedMinuteMeasurements, ProfileGroupTest.verifyAndRemoveTimestampsFromActual(profileEvent, logEvent.getTimestamp()));

        LOG.info("Aggregating minute {}", dnsMinuteProfile);
        LOG.info("With hour {}", dnsHourProfile);
        dnsHourProfile.add(dnsMinuteProfile);
        LOG.info("Profile before {}", dnsHourProfile);

        profileEvent = dnsHourProfile.getProfileEventResult();
        Assert.assertEquals(expectedHourMeasurements, ProfileGroupTest.verifyAndRemoveTimestampsFromActual(profileEvent, logEvent.getTimestamp()));

    }

    private void incrementMeasurement(Map<String, Object> measurements, String measurementName) {
        measurements.put(getMeasurementMapKey(measurementName), (Double)measurements.get(getMeasurementMapKey(measurementName)) + 1);
    }

    private Map<String, Object> initMinuteProfileMeasurements() {

        Map<String, Object> measurements = new HashMap<>();
        measurements.put(LogEvent.ERRORS_FIELD, new HashSet<String>());
        measurements.put(ProfileEvent.PROFILE_TYPE_FIELD_NAME, DnsRequestEvent.DNS_FINGERPRINT_MINUTE_PROFILE);
        measurements.put(ProfileEvent.PROFILE_ENTITY_KEY_FILED_NAME, EXPECTED_ENTITY_KEY);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P1.name()), 1.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P2.name()), 1.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P3.name()), 1.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P6.name()), 0.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P7.name()), 0.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P8.name()), 1.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P9.name()), 1.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P10.name()), 1.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P12.name()), 0.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P13.name()), 1.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P14.name()), 1.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.DISTINCT_ANSWERS.name()), 8.0);

        return measurements;
    }

    private Map<String, Object> initHourProfileMeasurements() {

        Map<String, Object> measurements = initMinuteProfileMeasurements();
        measurements.put(ProfileEvent.PROFILE_TYPE_FIELD_NAME, DnsRequestEvent.DNS_FINGERPRINT_HOUR_PROFILE);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.MINUTES_ACTIVE.name()), 1.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P4.name()), 1.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P5.name()), 1.0/1.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P11.name()), 1.0);
        measurements.put(getMeasurementMapKey(DnsRequestEvent.DnsFingerprintProfileMeasurements.P15.name()), 1.0/8.0);

        return measurements;
    }

    private String getMeasurementMapKey(String name) {
        return ProfileEvent.PROFILE_MEASUREMENT_FIELD_NAME_PREFIX.concat(name);
    }
}
