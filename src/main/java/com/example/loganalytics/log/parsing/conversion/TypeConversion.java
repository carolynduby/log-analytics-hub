package com.example.loganalytics.log.parsing.conversion;

import java.math.BigDecimal;
import java.util.function.Function;

public class TypeConversion {

    public static final Function<Object, Object>    bigDecimalToLong = bigDecimalValue -> ((BigDecimal)bigDecimalValue).longValue();

    public static final Function<Object, Object> bigDecimalToEpoch = timeAsObject -> {
        BigDecimal timeAsBigDecimal = (BigDecimal)timeAsObject;
        Double timeAsDouble = timeAsBigDecimal.doubleValue() * 1000;

        return (timeAsDouble.longValue());
    };

}
