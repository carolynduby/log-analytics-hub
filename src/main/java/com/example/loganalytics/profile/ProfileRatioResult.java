package com.example.loganalytics.profile;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ProfileRatioResult {
    private String resultName;
    private Profile numerator;
    private Profile denominator;

    public double getResult() {
        double numeratorResult = numerator.getResult();
        double denominatorResult = denominator.getResult();
        double result = 0.0;

        if (denominatorResult != 0.0) {
            result = numeratorResult/denominatorResult;
        }
        return result;
    }
}
