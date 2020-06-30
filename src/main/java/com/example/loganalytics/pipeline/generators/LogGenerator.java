package com.example.loganalytics.pipeline.generators;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public abstract class LogGenerator implements ParallelSourceFunction<String> {

    /**
     * seconds between generated messages
     */
    private static final int DELAY_BETWEEN_MESSAGES_MS = 300;
    /**
     * Determines if the generator is running or not.
     */
    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        long currentTime = Instant.now().getEpochSecond();

        while (isRunning.get()) {
            synchronized (sourceContext.getCheckpointLock()) {
                for(String sample : getNextSamples()) {
                    sourceContext.collect(sample);
                }
            }

            long delayBeforeNextMessage = getDelayMillisBeforeNextEvent();
            if (delayBeforeNextMessage > 0) {
                Thread.sleep(delayBeforeNextMessage);
            }
        }
    }

    public abstract long getDelayMillisBeforeNextEvent();

    public abstract String[] getNextSamples();

    @Override
    public void cancel() {
        isRunning.set(false);
    }

}
