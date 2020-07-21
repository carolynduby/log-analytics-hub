package com.example.loganalytics.event.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;

public class JsonFormat {
    private final  ObjectMapper mapper = new ObjectMapper();

    public String convert(Object event) throws LogFormatException {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            mapper.writeValue(outputStream, event);
            return outputStream.toString();
        } catch (Exception e) {
            throw new LogFormatException(e);
        }
    }

}
