package com.example.loganalytics.log.sources;

import com.example.loganalytics.event.DnsRequestEvent;
import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.NetworkEvent;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFilesTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class LogSourcesTest {

    public static LogSource createLogSource(String sourceName) throws IOException {
        LogSources logSources = LogSources.create(EnrichmentReferenceFilesTest.createEnrichmentTestParameters());
        return logSources.getSource(sourceName);
    }

    @Test
    public void testBroText() throws IOException {
        LogSource  broTextParser = createLogSource(LogSources.BROTEXT_SOURCE_NAME);
        String eventString = "1312962414.491994\tCHETdE2UZjza2QQvRi\t147.32.85.30\t49904\t147.32.80.9\t53\tudp\t36158\tprofile.ak.fbcdn.net\t1\tC_INTERNET\t1\tA\t0\tNOERROR\tF\tF\tT\tT\t0\tprofile.ak.facebook.com.edgesuite.net,a1725.l.akamai.net,2.20.182.40,2.20.182.42,2.20.182.10,2.20.182.16,2.20.182.17,2.20.182.19\t4941.000000,12090.000000,2.000000,2.000000,2.000000,2.000000,2.000000,2.000000\tF";
        LogEvent event = broTextParser.ingestEvent(eventString);

        Assert.assertTrue(event.getErrors().isEmpty());
        Assert.assertEquals(1312962414491L, event.getTimestamp());
        Assert.assertEquals("CHETdE2UZjza2QQvRi", event.getField("dns.uid"));
        Assert.assertEquals(eventString, event.getField(LogEvent.ORIGINAL_STRING_FIELD));
        Assert.assertEquals("147.32.85.30", event.getField(NetworkEvent.IP_SRC_ADDR_FIELD));
        Assert.assertEquals(49904L, event.getField(NetworkEvent.IP_SRC_PORT_FIELD));
        Assert.assertEquals("147.32.80.9", event.getField(NetworkEvent.IP_DST_ADDR_FIELD));
        Assert.assertEquals(53L, event.getField(NetworkEvent.IP_DST_PORT_FIELD));
        Assert.assertEquals("udp", event.getField(NetworkEvent.PROTO_FIELD));
        Assert.assertEquals(36158L, event.getField(NetworkEvent.TRANS_ID_FIELD));
        Assert.assertEquals("profile.ak.fbcdn.net", event.getField(DnsRequestEvent.DNS_QUERY_FIELD));
        Assert.assertEquals(1L, event.getField(DnsRequestEvent.DNS_QCLASS_FIELD));
        Assert.assertEquals("C_INTERNET", event.getField(DnsRequestEvent.DNS_QCLASS_NAME_FIELD));
        Assert.assertEquals(1L, event.getField(DnsRequestEvent.DNS_QUERY_TYPE_ID));
        Assert.assertEquals("A", event.getField(DnsRequestEvent.DNS_QUERY_TYPE));
        Assert.assertEquals(0L, event.getField(DnsRequestEvent.DNS_RCODE_FIELD));
        Assert.assertEquals("NOERROR", event.getField(DnsRequestEvent.DNS_RESPONSE_CODE_NAME));
        Assert.assertEquals(false, event.getField(DnsRequestEvent.DNS_AA_FIELD));
        Assert.assertEquals(false, event.getField(DnsRequestEvent.DNS_TC_FIELD));
        Assert.assertEquals(true, event.getField(DnsRequestEvent.DNS_RD_FIELD));
        Assert.assertEquals(true, event.getField(DnsRequestEvent.DNS_RA_FIELD));
        Assert.assertEquals(0L, event.getField(DnsRequestEvent.DNS_Z_FIELD));
        Assert.assertEquals(Arrays.asList("profile.ak.facebook.com.edgesuite.net","a1725.l.akamai.net", "2.20.182.40", "2.20.182.42","2.20.182.10","2.20.182.16","2.20.182.17","2.20.182.19"), event.getField(DnsRequestEvent.DNS_ANSWERS));
        Assert.assertEquals(Arrays.asList(4941.000000,12090.000000,2.000000,2.000000,2.000000,2.000000,2.000000,2.000000), event.getField("dns.TTLs"));
        Assert.assertEquals(false, event.getField(DnsRequestEvent.DNS_REJECTED_FIELD));
        Assert.assertEquals(LogSources.BROTEXT_SOURCE_NAME, event.getSource());
    }
}
