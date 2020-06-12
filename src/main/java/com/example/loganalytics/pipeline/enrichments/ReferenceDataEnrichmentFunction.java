package com.example.loganalytics.pipeline.enrichments;

import com.example.loganalytics.event.LogEvent;
import com.example.loganalytics.event.LogEventFieldSpecification;
import com.example.loganalytics.log.enrichments.EnrichmentReferenceDataSupplier;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceDataSource;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceHbase;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@Data
@AllArgsConstructor
public class ReferenceDataEnrichmentFunction extends RichAsyncFunction<LogEvent,LogEvent> {

    private ParameterTool params;
    private String fieldName;
    private String featureName;
    private boolean isRequired;
    private transient EnrichmentReferenceDataSource enrichmentReferenceDataSource;
    private transient LogEventFieldSpecification baseEventFieldSpecification;

    public ReferenceDataEnrichmentFunction(ParameterTool params, String fieldName, String featureName, boolean isRequired) {
        this.params = params;
        this.fieldName = fieldName;
        this.featureName = featureName;
        this.isRequired = isRequired;
    }

    private static final ExecutorService threadPool = Executors.newFixedThreadPool(10,
            new ThreadFactory() {
                private final ThreadFactory threadFactory = Executors.defaultThreadFactory();

                @Override
                public Thread newThread(@Nonnull Runnable runnable) {
                    Thread thread = threadFactory.newThread(runnable);
                    thread.setName("async-enrich-".concat(thread.getName()));

                    return thread;
                }

            });

    @Override
    public void open(Configuration configuration) throws Exception {
        this.enrichmentReferenceDataSource = EnrichmentReferenceHbase.create(params);
        this.baseEventFieldSpecification = new LogEventFieldSpecification(fieldName, featureName, isRequired);
        super.open(configuration);
    }

    public void getEnrichmentDataAsync(LogEvent logEvent, Consumer<Map<String, Object>> callback) {
        String enrichmentKey = logEvent.getField(baseEventFieldSpecification, String.class);
        EnrichmentReferenceDataSupplier referenceDataEnrichment = new EnrichmentReferenceDataSupplier(enrichmentReferenceDataSource, enrichmentKey, baseEventFieldSpecification);
        CompletableFuture.supplyAsync(referenceDataEnrichment, threadPool).
                thenAcceptAsync(callback, MoreExecutors.directExecutor()).
                handle((m, e) -> {
                    logEvent.reportError(baseEventFieldSpecification, e.getMessage());
                    return m;
                });
    }

    @Override
    public void asyncInvoke(LogEvent originalLogEvent, ResultFuture<LogEvent> resultFuture) {
        Consumer<Map<String, Object>> enrichmentConsumer = new Consumer<Map<String, Object>>() {
            private final LogEvent logEvent = originalLogEvent;
            @Override
            public void accept(Map<String, Object> enrichments) {
                logEvent.enrich(baseEventFieldSpecification, enrichments);
                resultFuture.complete(Collections.singletonList(logEvent));
            }
        };

        getEnrichmentDataAsync(originalLogEvent, enrichmentConsumer);
    }
}
