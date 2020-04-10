package com.example.loganalytics.log.enrichments.reference;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class HbaseEnrichmentStream {

    private static ExecutorService pool = Executors.newFixedThreadPool(30,
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                    Thread thread = threadFactory.newThread(runnable);
                    thread.setName("enrich-hbase-ref-" + thread.getName());
                    return thread;
                }

                private final ThreadFactory threadFactory = Executors.defaultThreadFactory();
            });

    public static EnrichmentReferenceData getEnrichmentValue(String enrichmentReferenceData, String fieldValue) {
        //return EnrichmentReferenceData.builder().enrichmentType(enrichmentReferenceData).enrichmentKey(fieldValue).
        //.build();
        return null;
    }

    public EnrichmentReferenceData getEnrichmentReferenceDataFor(String enrichmentReferenceData, String fieldValue) {
        return new EnrichmentHbaseDataSupplier(enrichmentReferenceData, fieldValue).get();
    }

    public void asyncGetEnrichmentReferenceDataFor(String enrichmentReferenceData, String fieldValue, Consumer<EnrichmentReferenceData> callback) {

        CompletableFuture.supplyAsync(new EnrichmentHbaseDataSupplier(enrichmentReferenceData, fieldValue), pool)
                .thenAcceptAsync(callback, com.google.common.util.concurrent.MoreExecutors.directExecutor());
    }

    public Map<String, EnrichmentReferenceData> getEnrichmentReferenceDataFor() {
        return new HashMap<>();
    }

    public Map<Long, EnrichmentReferenceData> getSensorReferenceDataForPartition(
            final int partition,
            final int numPartitions) {
        return new HashMap<>();
    }

    private static class EnrichmentHbaseDataSupplier implements Supplier<EnrichmentReferenceData> {

        private String enrichmentReferenceData;
        private String fieldValue;

        public EnrichmentHbaseDataSupplier(final String enrichmentReferenceData, final String fieldValue) {
            this.enrichmentReferenceData = enrichmentReferenceData;
            this.fieldValue = fieldValue;
        }

        @Override
        public EnrichmentReferenceData get() {
            return getEnrichmentValue(enrichmentReferenceData, fieldValue);
        }


    }
}