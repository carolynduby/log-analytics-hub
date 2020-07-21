package com.example.loganalytics.log.parsing.conversion;

import com.google.common.base.Splitter;
import com.google.common.primitives.Doubles;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class TypeConversion {

    private static final StringListConversion<Double> stringToDoubleList = new StringListConversion<>(Doubles.stringConverter());
    public static final Function<Object, Object>    bigDecimalToLong = bigDecimalValue -> ((BigDecimal)bigDecimalValue).longValue();

    public static final Function<Object, Object> bigDecimalToEpoch = timeAsObject -> {
        BigDecimal timeAsBigDecimal = (BigDecimal)timeAsObject;
        return convertDoubleToEpoch(timeAsBigDecimal.doubleValue());
    };

    public static final Function<Object, Object> stringTimestampToEpoch = inValue -> {
        return convertDoubleToEpoch(Double.parseDouble((String) inValue));
    };

    public static final Function<Object, Object> stringToLong = inValue -> {
        return Long.parseLong((String) inValue);
    };

    public static final Function<Object, Object> splitStringList = stringValue -> {
        List<String> stringList = new ArrayList<>();
        for(String nextToken : Splitter.on(",").split((String) stringValue)) {
            stringList.add(nextToken);
        }
        return stringList;
    };

    public static final Function<Object, Object> splitDoubleList = stringValue -> {
        return stringToDoubleList.apply(stringValue);
    };

    public static final Function<Object, Object> stringToBoolean = inValue -> {
        String stringValue = (String)inValue;
        return ("t".equalsIgnoreCase(stringValue) || "true".equalsIgnoreCase(stringValue));
    };

    private static long convertDoubleToEpoch(double timestamp) {
        return (long)(timestamp * 1000);
    }

}
