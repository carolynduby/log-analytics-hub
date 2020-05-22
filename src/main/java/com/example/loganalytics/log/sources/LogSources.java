package com.example.loganalytics.log.sources;

import com.example.loganalytics.event.DnsRequest;
import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.log.enrichments.FilterListEnrichment;
import com.example.loganalytics.log.enrichments.IpCityCountrySetEnrichment;
import com.example.loganalytics.log.enrichments.IpGeoEnrichment;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceDomainTransformations;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFiles;
import com.example.loganalytics.log.parsing.conversion.TypeConversion;
import com.example.loganalytics.log.parsing.grok.GrokParser;
import com.example.loganalytics.log.parsing.json.JsonParser;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class LogSources {
    public static final String SQUID_SOURCE_NAME = "squid";
    public static final String ZEEK_SOURCE_NAME = "zeek";

    private final Map<String, LogSource<String>> sources = new HashMap<>();

    LogSources(IpGeoEnrichment ipGeoEnrichment, IpCityCountrySetEnrichment ipCityCountrySetEnrichment) {
        createSquidGrokParser(ipGeoEnrichment);
        createZeekParser(ipCityCountrySetEnrichment);
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

        LogSource<String> squidSource = new LogSource<>(new GrokParser(squidExpName, grokMap));
        sources.put(SQUID_SOURCE_NAME, squidSource);
        squidSource.configureFieldEnrichment(LogEvent.IP_DST_ADDR_FIELD, IpGeoEnrichment.GEOCODE_FEATURE, ipGeoEnrichment);

    }

    private final Predicate<LogEvent> dnsQueryIsDomainName = logEvent -> {
        String queryType = (String)logEvent.getField(DnsRequest.DNS_QUERY_TYPE);
        return (queryType != null && !queryType.isEmpty() &&
                !queryType.equals("SRV") && !queryType.equals("PTR") && !queryType.equals("URI") &&
                !queryType.equals("SOA"));
    };

    private void createZeekParser(IpCityCountrySetEnrichment ipCityCountrySetEnrichment) {

        Map<String, String> fieldRename = new HashMap<>();
        fieldRename.put("dns[\\\"id.orig_h\\\"]", LogEvent.IP_SRC_ADDR_FIELD);
        fieldRename.put("dns[\\\"id.orig_p\\\"]", LogEvent.IP_SRC_PORT_FIELD);
        fieldRename.put("dns[\\\"id.resp_h\\\"]", LogEvent.IP_DST_ADDR_FIELD);
        fieldRename.put("dns[\\\"id.resp_p\\\"]", LogEvent.IP_DST_PORT_FIELD);
        fieldRename.put("dns.ts", LogEvent.TIMESTAMP_FIELD);

        Map<String, Function<Object, Object>> fieldTypeConversions = new HashMap<>();
        fieldTypeConversions.put(LogEvent.IP_SRC_PORT_FIELD, TypeConversion.bigDecimalToLong);
        fieldTypeConversions.put(LogEvent.IP_DST_PORT_FIELD, TypeConversion.bigDecimalToLong);
        fieldTypeConversions.put(LogEvent.TIMESTAMP_FIELD, TypeConversion.bigDecimalToEpoch);

        LogSource<String> zeekSource = new LogSource<>(new JsonParser(fieldRename, fieldTypeConversions));
        sources.put(ZEEK_SOURCE_NAME, zeekSource);

        zeekSource.configureFieldEnrichment(DnsRequest.DNS_QUERY, EnrichmentReferenceDomainTransformations.DOMAIN_BREAKDOWN,
                EnrichmentReferenceDomainTransformations.domainBreakdown, String.class, dnsQueryIsDomainName);
        FilterListEnrichment<String> getIps = new FilterListEnrichment<>(DnsRequest.DNS_FILTERED_IP_ENDING, o -> InetAddressValidator.getInstance().isValidInet4Address(o));
        zeekSource.configureFieldEnrichment(DnsRequest.DNS_ANSWERS, FilterListEnrichment.FILTER_FEATURE_NAME, getIps,
                List.class, null);
        zeekSource.configureFieldEnrichment(DnsRequest.DNS_IP_ANSWERS, IpCityCountrySetEnrichment.CITY_COUNTRY_FEATURE_NAME, ipCityCountrySetEnrichment,
                List.class, null);
    }

    public LogSource<String> getSource(String logSourceName) {
        return sources.get(logSourceName);
    }

}
