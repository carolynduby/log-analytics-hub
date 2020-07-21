package com.example.loganalytics.log.sources;

import com.example.loganalytics.event.DnsRequestEvent;
import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.NetworkEvent;
import com.example.loganalytics.log.enrichments.FilterListEnrichment;
import com.example.loganalytics.log.enrichments.IpCityCountrySetEnrichment;
import com.example.loganalytics.log.enrichments.IpGeoEnrichment;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceDomainTransformations;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFiles;
import com.example.loganalytics.log.parsing.LogParser;
import com.example.loganalytics.log.parsing.delimitedtext.DelimitedTextParser;
import com.example.loganalytics.log.parsing.conversion.TypeConversion;
import com.example.loganalytics.log.parsing.grok.GrokParser;
import com.example.loganalytics.log.parsing.json.JsonParser;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class LogSources {
    public static final String SQUID_SOURCE_NAME = "squid";
    public static final String ZEEK_SOURCE_NAME = "zeek";
    public static final String BROTEXT_SOURCE_NAME = "brotext";

    private final Map<String, LogSource> sources = new HashMap<>();

    LogSources(IpGeoEnrichment ipGeoEnrichment, IpCityCountrySetEnrichment ipCityCountrySetEnrichment) {
        createSquidGrokParser(ipGeoEnrichment);
        createZeekParser(ipCityCountrySetEnrichment);
        createBroTextParser(ipCityCountrySetEnrichment);
    }

    public static LogSources create(ParameterTool params) throws IOException {
        EnrichmentReferenceFiles enrichmentReferenceFiles = EnrichmentReferenceFiles.create(params);
        IpGeoEnrichment ipGeoEnrichment = IpGeoEnrichment.create(enrichmentReferenceFiles);
        IpCityCountrySetEnrichment ipCityCountrySetEnrichment = IpCityCountrySetEnrichment.create(enrichmentReferenceFiles);
        return new LogSources(ipGeoEnrichment, ipCityCountrySetEnrichment);
    }

    private void createSquidGrokParser(IpGeoEnrichment ipGeoEnrichment) {
        final String squidExpName = "SQUID";
        Map<String, String> grokMap = new HashMap<>();

        //noinspection SpellCheckingInspection
        String squidGrokExpr = "%{NUMBER:timestamp}[^0-9]*%{INT:elapsed} %{IP:ip_src_addr} %{WORD:action}/%{NUMBER:code} %{NUMBER:bytes} %{WORD:method} %{NOTSPACE:url}[^0-9]*(%{IP:ip_dst_addr})?";
        grokMap.put(squidExpName, squidGrokExpr);

        GrokParser squidGrokParser = new GrokParser(squidExpName, grokMap);
        squidGrokParser.addFieldTransformation(LogEvent.TIMESTAMP_FIELD, TypeConversion.stringTimestampToEpoch);

        LogSource squidSource = new LogSource(SQUID_SOURCE_NAME, squidGrokParser);
        sources.put(SQUID_SOURCE_NAME, squidSource);

        squidSource.configureFieldEnrichment(NetworkEvent.IP_DST_ADDR_FIELD, IpGeoEnrichment.GEOCODE_FEATURE, ipGeoEnrichment);
    }

    private void createBroTextParser(IpCityCountrySetEnrichment ipCityCountrySetEnrichment) {
        String[] fieldNames = new String []{ LogEvent.TIMESTAMP_FIELD, DnsRequestEvent.UID_FIELD,
                NetworkEvent.IP_SRC_ADDR_FIELD, NetworkEvent.IP_SRC_PORT_FIELD,
                NetworkEvent.IP_DST_ADDR_FIELD, NetworkEvent.IP_DST_PORT_FIELD,
                NetworkEvent.PROTO_FIELD, NetworkEvent.TRANS_ID_FIELD,
                DnsRequestEvent.DNS_QUERY_FIELD, "dns.qclass", "dns.qclass_name",
                DnsRequestEvent.DNS_QUERY_TYPE_ID, DnsRequestEvent.DNS_QUERY_TYPE,
                "dns.rcode", DnsRequestEvent.DNS_RESPONSE_CODE_NAME,
                "dns.AA", "dns.TC", "dns.RD", "dns.RA", "dns.Z", DnsRequestEvent.DNS_ANSWERS,
                "dns.TTLs", "dns.rejected" };

        LogParser<String> broTextParser = new DelimitedTextParser(fieldNames);
        broTextParser.addFieldTransformation(LogEvent.TIMESTAMP_FIELD, TypeConversion.stringTimestampToEpoch);
        broTextParser.addFieldTransformation(NetworkEvent.IP_SRC_PORT_FIELD, TypeConversion.stringToLong);
        broTextParser.addFieldTransformation(NetworkEvent.IP_DST_PORT_FIELD, TypeConversion.stringToLong);
        broTextParser.addFieldTransformation(NetworkEvent.TRANS_ID_FIELD, TypeConversion.stringToLong);
        broTextParser.addFieldTransformation("dns.qclass", TypeConversion.stringToLong);
        broTextParser.addFieldTransformation(DnsRequestEvent.DNS_QUERY_TYPE_ID, TypeConversion.stringToLong);
        broTextParser.addFieldTransformation("dns.rcode", TypeConversion.stringToLong);
        broTextParser.addFieldTransformation("dns.AA", TypeConversion.stringToBoolean);
        broTextParser.addFieldTransformation("dns.TC", TypeConversion.stringToBoolean);
        broTextParser.addFieldTransformation("dns.RD", TypeConversion.stringToBoolean);
        broTextParser.addFieldTransformation("dns.RA", TypeConversion.stringToBoolean);
        broTextParser.addFieldTransformation("dns.Z", TypeConversion.stringToLong);
        broTextParser.addFieldTransformation(DnsRequestEvent.DNS_ANSWERS, TypeConversion.splitStringList);
        broTextParser.addFieldTransformation("dns.TTLs", TypeConversion.splitDoubleList);
        broTextParser.addFieldTransformation("dns.rejected", TypeConversion.stringToBoolean);

        LogSource broTextSource = new LogSource(BROTEXT_SOURCE_NAME, broTextParser);
        sources.put(BROTEXT_SOURCE_NAME, broTextSource);
        addDnsEnrichments(ipCityCountrySetEnrichment, broTextSource);
    }

    private final Predicate<LogEvent> dnsQueryIsDomainName = logEvent -> {
        String queryType = (String)logEvent.getField(DnsRequestEvent.DNS_QUERY_TYPE);
        return (queryType != null && !queryType.isEmpty() &&
                !queryType.equals("SRV") && !queryType.equals("PTR") && !queryType.equals("URI") &&
                !queryType.equals("SOA"));
    };

    private void createZeekParser(IpCityCountrySetEnrichment ipCityCountrySetEnrichment) {

        LogParser<String> jsonParser = new JsonParser();
        jsonParser.addFieldRename("dns[\\\"id.orig_h\\\"]", NetworkEvent.IP_SRC_ADDR_FIELD);
        jsonParser.addFieldRename("dns[\\\"id.orig_p\\\"]", NetworkEvent.IP_SRC_PORT_FIELD);
        jsonParser.addFieldRename("dns[\\\"id.resp_h\\\"]", NetworkEvent.IP_DST_ADDR_FIELD);
        jsonParser.addFieldRename("dns[\\\"id.resp_p\\\"]", NetworkEvent.IP_DST_PORT_FIELD);
        jsonParser.addFieldRename("dns.ts", LogEvent.TIMESTAMP_FIELD);

        jsonParser.addFieldTransformation(NetworkEvent.IP_SRC_PORT_FIELD, TypeConversion.bigDecimalToLong);
        jsonParser.addFieldTransformation(NetworkEvent.IP_DST_PORT_FIELD, TypeConversion.bigDecimalToLong);
        jsonParser.addFieldTransformation(LogEvent.TIMESTAMP_FIELD, TypeConversion.bigDecimalToEpoch);


        LogSource zeekSource = new LogSource(ZEEK_SOURCE_NAME, jsonParser);
        sources.put(ZEEK_SOURCE_NAME, zeekSource);

        addDnsEnrichments(ipCityCountrySetEnrichment, zeekSource);
    }

    private void addDnsEnrichments(IpCityCountrySetEnrichment ipCityCountrySetEnrichment, LogSource logSource) {
        logSource.configureFieldEnrichment(DnsRequestEvent.DNS_QUERY_FIELD, EnrichmentReferenceDomainTransformations.DOMAIN_BREAKDOWN,
                EnrichmentReferenceDomainTransformations.domainBreakdown, String.class, dnsQueryIsDomainName);
        FilterListEnrichment<String> getIps = new FilterListEnrichment<>(DnsRequestEvent.DNS_FILTERED_IP_ENDING, o -> InetAddressValidator.getInstance().isValidInet4Address(o));
        logSource.configureFieldEnrichment(DnsRequestEvent.DNS_ANSWERS, FilterListEnrichment.FILTER_FEATURE_NAME, getIps,
                List.class, null);
        logSource.configureFieldEnrichment(DnsRequestEvent.DNS_IP_ANSWERS, IpCityCountrySetEnrichment.CITY_COUNTRY_FEATURE_NAME, ipCityCountrySetEnrichment,
                List.class, null);
    }

    public LogSource getSource(String logSourceName) {
        return sources.get(logSourceName);
    }

}
