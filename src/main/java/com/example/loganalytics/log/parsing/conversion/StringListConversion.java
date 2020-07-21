package com.example.loganalytics.log.parsing.conversion;

import com.google.common.base.Function;
import com.google.common.base.Splitter;

import java.util.ArrayList;
import java.util.List;

public class StringListConversion<ELEMENT> implements Function<Object, List<ELEMENT>> {
    private final  String delimiter;
    private final java.util.function.Function<String, ELEMENT> elementConversion;

    public StringListConversion(String delimiter, java.util.function.Function<String, ELEMENT> elementConversion) {
        this.delimiter = delimiter;
        this.elementConversion = elementConversion;
    }

    public StringListConversion(Function<String, ELEMENT> elementConversion) {
        this(",", elementConversion);
    }

    @Override
    public List<ELEMENT> apply(Object inValue) {
        List<ELEMENT>  transformedList = new ArrayList<>();
        for(String element : Splitter.on(delimiter).splitToList((String)inValue)) {
            transformedList.add(elementConversion.apply(element));
        }
        return  transformedList;
    }
}
