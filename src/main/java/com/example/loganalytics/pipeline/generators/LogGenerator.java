package com.example.loganalytics.pipeline.generators;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LogGenerator implements ParallelSourceFunction<String> {

    /**
     * seconds between generated messages
     */
    private static final int DELAY_BETWEEN_MESSAGES_MS = 300;
    /**
     * Determines if the generator is running or not.
     */
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    /**
     * Number of log entries generated so far.
     */
    private final AtomicLong currentLogEntry = new AtomicLong(0);
    /**
     * Samples of log entries to generate.   Samples should be compatible with Java String.format.  One parameter is provided for current timestamp.
     */
    private final String[] samples;

    public LogGenerator(String[] samples) {
        this.samples = samples;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        long currentTime = Instant.now().getEpochSecond();

        while (isRunning.get()) {
            synchronized (sourceContext.getCheckpointLock()) {
                int logSampleIndex = (int) (currentLogEntry.getAndIncrement() % samples.length);
                currentTime = currentTime + DELAY_BETWEEN_MESSAGES_MS * logSampleIndex;
                sourceContext.collect(String.format(samples[logSampleIndex], currentTime));
            }
            if (DELAY_BETWEEN_MESSAGES_MS > 0) {
                Thread.sleep(DELAY_BETWEEN_MESSAGES_MS);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning.set(false);
    }

}
