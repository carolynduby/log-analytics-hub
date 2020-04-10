package com.example.loganalytics.event.serialization;

import com.example.loganalytics.event.LogEvent;

public interface LogFormat<T> {
    T convert(LogEvent event) throws LogFormatException;
}
