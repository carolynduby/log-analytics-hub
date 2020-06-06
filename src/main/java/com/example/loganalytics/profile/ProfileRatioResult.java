package com.example.loganalytics.profile;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class ProfileRatioResult {
    private String resultName;
    private String numerator;
    private String denominator;
}
