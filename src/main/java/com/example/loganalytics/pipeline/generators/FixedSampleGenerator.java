package com.example.loganalytics.pipeline.generators;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

public class FixedSampleGenerator extends LogGenerator {
    private final String[] samples;

    private static final long DELAY_BETWEEN_MESSAGES_MS = 300;

    /**
     * Number of log entries generated so far.
     */
    private final AtomicLong currentLogEntry = new AtomicLong(0);

    public FixedSampleGenerator(String[] samples) {
        this.samples = samples;
    }

    @Override
    public long getDelayMillisBeforeNextEvent() {
        return DELAY_BETWEEN_MESSAGES_MS;
    }

    @Override
    public String[] getNextSamples() {
        int logSampleIndex = (int) (currentLogEntry.getAndIncrement() % samples.length);
        return new String[] {String.format(samples[logSampleIndex], Instant.now().getEpochSecond())};
    }
}
