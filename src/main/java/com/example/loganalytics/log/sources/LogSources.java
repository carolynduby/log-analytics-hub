package com.example.loganalytics.log.sources;

import com.example.loganalytics.log.enrichments.IpGeoEnrichment;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFiles;
import com.example.loganalytics.log.parsing.grok.GrokParser;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LogSources {
    public static final String SQUID_SOURCE_NAME = "squid";
    private final Map<String, LogSource<String>> sources = new HashMap<>();

    LogSources(IpGeoEnrichment ipGeoEnrichment) {
        createSquidGrokParser(ipGeoEnrichment);
    }

    public static LogSources create(ParameterTool params) throws IOException {
        EnrichmentReferenceFiles enrichmentReferenceFiles = EnrichmentReferenceFiles.create(params);
        IpGeoEnrichment ipGeoEnrichment = IpGeoEnrichment.create(enrichmentReferenceFiles);
        return new LogSources(ipGeoEnrichment);
    }

    private void createSquidGrokParser(IpGeoEnrichment ipGeoEnrichment) {
        final String squidExpName = "SQUID";
        Map<String, String> grokMap = new HashMap<>();

        String squidGrokExpr = "%{NUMBER:timestamp}[^0-9]*%{INT:elapsed} %{IP:ip_src_addr} %{WORD:action}/%{NUMBER:code} %{NUMBER:bytes} %{WORD:method} %{NOTSPACE:url}[^0-9]*(%{IP:ip_dst_addr})?";
        grokMap.put(squidExpName, squidGrokExpr);

        LogSource<String> squidSource = new LogSource<>(SQUID_SOURCE_NAME, new GrokParser(squidExpName, grokMap));
        sources.put(SQUID_SOURCE_NAME, squidSource);
        squidSource.configureFieldEnrichment("ip_dst_addr", ipGeoEnrichment);
    }

    public LogSource<String> getSource(String logSourceName) {
        return sources.get(logSourceName);
    }

}
