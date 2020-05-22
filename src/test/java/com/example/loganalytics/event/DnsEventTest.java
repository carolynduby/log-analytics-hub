package com.example.loganalytics.event;

import com.example.loganalytics.log.parsing.ParserTest;
import com.example.loganalytics.log.sources.LogSource;
import com.example.loganalytics.log.sources.LogSources;
import com.example.loganalytics.pipeline.profiles.ProfileEvent;
import com.example.loganalytics.profile.ProfileGroup;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("SpellCheckingInspection")
public class DnsEventTest {

    private static final String NXDOMAIN_EVENT= "{\"dns\": {\"ts\":1588709649.439005,\"uid\":\"Cmnfun1hYw3CQeCVHk\",\"id.orig_h\":\"172.31.1.6\",\"id.orig_p\":28000,\"id.resp_h\":\"172.31.1.6\",\"id.resp_p\":53,\"proto\":\"udp\",\"trans_id\":24430,\"query\":\"asia.api.targetingmantra.com\",\"qclass\":1,\"qclass_name\":\"C_INTERNET\",\"qtype\":1,\"qtype_name\":\"A\",\"rcode\":3,\"rcode_name\":\"NXDOMAIN\",\"AA\":false,\"TC\":false,\"RD\":true,\"RA\":false,\"Z\":0,\"rejected\":true}}";
    private static final String VALID_JSON_LINE = "{\"dns\": {\"ts\":1588709647.572953,\"uid\":\"Cwfli23YvRiruLucOh\",\"id.orig_h\":\"172.31.1.6\",\"id.orig_p\":13512,\"id.resp_h\":\"172.31.3.121\",\"id.resp_p\":53,\"proto\":\"udp\",\"trans_id\":41874,\"rtt\":0.002012968063354492,\"query\":\"track-front-prod-825173558.us-east-1.elb.amazonaws.com\",\"qclass\":1,\"qclass_name\":\"C_INTERNET\",\"qtype\":1,\"qtype_name\":\"A\",\"rcode\":0,\"rcode_name\":\"NOERROR\",\"AA\":false,\"TC\":false,\"RD\":true,\"RA\":true,\"Z\":0,\"answers\":[\"23.21.126.97\",\"50.17.222.11\",\"107.22.253.140\",\"54.243.169.159\",\"23.23.144.131\",\"23.23.222.251\",\"174.129.202.115\",\"23.21.127.5\"],\"TTLs\":[30.0,30.0,30.0,30.0,30.0,30.0,30.0,30.0],\"rejected\":false}}";
    private static final String MXQUERY_EVENT = "{\"dns\": {\"ts\":1588709797.884982,\"uid\":\"CxsMYoLlK4WHUgbcb\",\"id.orig_h\":\"172.31.1.6\",\"id.orig_p\":17831,\"id.resp_h\":\"172.31.3.121\",\"id.resp_p\":53,\"proto\":\"udp\",\"trans_id\":43007,\"query\":\"a2047.dspl.akamai.net\",\"qclass\":1,\"qclass_name\":\"C_INTERNET\",\"qtype\":15,\"qtype_name\":\"MX\",\"rcode\":0,\"rcode_name\":\"NOERROR\",\"AA\":false,\"TC\":false,\"RD\":true,\"RA\":false,\"Z\":0,\"rejected\":false}}";
    private static final String PTRQUERY_EVENT = "{\"dns\": {\"ts\":1588709649.896947,\"uid\":\"C17Cvi3CBuf0H6JZKb\",\"id.orig_h\":\"172.31.1.6\",\"id.orig_p\":3702,\"id.resp_h\":\"172.31.3.121\",\"id.resp_p\":53,\"proto\":\"udp\",\"trans_id\":29667,\"query\":\"26.216.233.219.in-addr.arpa\",\"qclass\":1,\"qclass_name\":\"C_INTERNET\",\"qtype\":12,\"qtype_name\":\"PTR\",\"rcode\":0,\"rcode_name\":\"NOERROR\",\"AA\":false,\"TC\":false,\"RD\":true,\"RA\":true,\"Z\":0,\"answers\":[\"chipnuts.com\"],\"TTLs\":[70203.0],\"rejected\":false}}";
    @Test
    public void testGetters() throws IOException {

        LogSource<String> jsonSource = ParserTest.createLogSource(LogSources.ZEEK_SOURCE_NAME);
        DnsRequest dnsRequest = new DnsRequest(jsonSource.ingestEvent(VALID_JSON_LINE));
        Assert.assertEquals("172.31.1.6", dnsRequest.getSourceIp());
        Assert.assertEquals(13512, dnsRequest.getSourcePort());
        Assert.assertEquals("172.31.3.121", dnsRequest.getDestinationIp());
        Assert.assertEquals(53, dnsRequest.getDestinationPort());
        Assert.assertEquals(1588709647572L, dnsRequest.getTimestamp());
        List<String> answerIps = Lists.newArrayList("23.21.126.97","50.17.222.11","107.22.253.140","54.243.169.159","23.23.144.131","23.23.222.251","174.129.202.115","23.21.127.5");
        Assert.assertEquals(answerIps, dnsRequest.getIpAnswers());
        List<String> cities = Lists.newArrayList("Ashburn");
        Assert.assertEquals(cities, dnsRequest.getAnswerCities());
        List<String> countries = Lists.newArrayList("US");
        Assert.assertEquals(countries, dnsRequest.getAnswerCountries());
        Assert.assertEquals("track-front-prod-825173558.us-east-1.elb.amazonaws.com", dnsRequest.getQuery());
        Assert.assertEquals("amazonaws", dnsRequest.getQuerySecondLevelDomain());
        Assert.assertEquals("com", dnsRequest.getQueryTopLevelDomain());
        Assert.assertEquals("NOERROR", dnsRequest.getResponseCode());
        Assert.assertEquals("A", dnsRequest.getQueryType());

    }

    @Test
    public void testDnsFingerprintProfile() throws IOException {
        LogSource<String> jsonSource = ParserTest.createLogSource(LogSources.ZEEK_SOURCE_NAME);
        ProfileGroup dnsProfile = DnsRequest.createDnsFingerPrintProfileGroup();


        // add 1 event and verify that the profile measurements are correct
        Map<String, Double> expectedMeasurements = initProfileMeasurements();
        verifyAddEventToProfile(jsonSource, VALID_JSON_LINE, dnsProfile, expectedMeasurements);

        // test an NXDOMAIN event
        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P1.name());
        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P2.name());
        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P8.name());
        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P10.name());
        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P12.name());
        expectedMeasurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P15.name(), 2.0/8.0);

        verifyAddEventToProfile(jsonSource, NXDOMAIN_EVENT, dnsProfile, expectedMeasurements);

        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P1.name());
        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P2.name());
        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P6.name());
        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P9.name());
        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P10.name());
        expectedMeasurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P15.name(), 3.0/8.0);

        verifyAddEventToProfile(jsonSource, MXQUERY_EVENT, dnsProfile, expectedMeasurements);

        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P1.name());
        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P2.name());
        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P7.name());
        expectedMeasurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P15.name(), 4.0/8.0);

        verifyAddEventToProfile(jsonSource, PTRQUERY_EVENT, dnsProfile, expectedMeasurements);

        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P1.name());
        incrementMeasurement(expectedMeasurements, DnsRequest.DnsFingerprintProfileMeasurements.P3.name());
        expectedMeasurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P11.name(), 5.0/4.0);
        expectedMeasurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P15.name(), 4.0/8.0);

        verifyAddEventToProfile(jsonSource, VALID_JSON_LINE, dnsProfile, expectedMeasurements);

    }

    private void verifyAddEventToProfile(LogSource<String> jsonSource, String eventText, ProfileGroup dnsProfile, Map<String, Double> expectedMeasurements) {
        LogEvent logEvent = jsonSource.ingestEvent(eventText);
        dnsProfile.add(logEvent);

        ProfileEvent profileEvent = dnsProfile.getProfileEventResult();
        Assert.assertEquals(logEvent.getSourceIp(), profileEvent.getEntityKey());
        Assert.assertEquals(expectedMeasurements, profileEvent.getMeasurements());
    }

    private void incrementMeasurement(Map<String, Double> measurements, String measurementName) {
        measurements.put(measurementName, measurements.get(measurementName) + 1);
    }

    private Map<String, Double> initProfileMeasurements() {

        Map<String, Double> measurements = new HashMap<>();
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P1.name(), 1.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P2.name(), 1.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P3.name(), 1.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P6.name(), 0.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P7.name(), 0.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P8.name(), 1.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P9.name(), 1.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P10.name(), 1.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P11.name(), 1.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P12.name(), 0.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P13.name(), 1.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P14.name(), 1.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.P15.name(), 1.0/8.0);
        measurements.put(DnsRequest.DnsFingerprintProfileMeasurements.DISTINCT_ANSWERS_PROFILE_NAME.name(), 8.0);

        return measurements;
    }

}
