package com.example.loganalytics.profile;

import lombok.AllArgsConstructor;
import lombok.Data;

public class ProfileAccumulatorTest {
    protected static final String TEST_RESULT_NAME = "test_result";

    @Data
    @AllArgsConstructor
    static class TestClass {
        String field;
    }
}
