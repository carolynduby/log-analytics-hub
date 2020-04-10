package com.example.loganalytics.log.parsing;

import com.example.loganalytics.event.LogEvent;

/**
 * Interface for all parsers that transform a raw log line with Input format to a map.
 *
 * @param <Input> Type of raw log event.
 */
public interface LogParser<Input> {
    LogEvent parse(Input rawLog);
}
