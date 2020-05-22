package com.example.loganalytics.event.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;

public class JsonFormat {

    public String convert(Object event) throws LogFormatException {
        ObjectMapper mapper = new ObjectMapper();

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            mapper.writeValue(outputStream, event);
            return outputStream.toString();
        } catch (Exception e) {
            throw new LogFormatException(e);
        }
    }
}
