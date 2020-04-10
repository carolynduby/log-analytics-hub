package com.example.loganalytics.event.serialization;

public class LogFormatException extends Exception {
    public LogFormatException(String message) {
        super(message);
    }

    public LogFormatException(String message, Throwable cause) {
        super(message, cause);
    }

    public LogFormatException(Throwable cause) {
        super(cause);
    }
}
