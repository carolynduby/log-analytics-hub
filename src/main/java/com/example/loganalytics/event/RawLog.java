package com.example.loganalytics.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@Data
@NoArgsConstructor
public class RawLog {
    private String source;
    private String text;
    private Map<String, Object> metadata;

    public RawLog(String source, String text) {
        this.source = source;
        this.text = text;
        this.metadata = new HashMap<>();
    }
}
