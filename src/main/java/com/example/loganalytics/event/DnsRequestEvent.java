package com.example.loganalytics.event;

import com.example.loganalytics.log.enrichments.FilterListEnrichment;
import com.example.loganalytics.log.enrichments.IpCityCountrySetEnrichment;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceDomainTransformations;
import com.example.loganalytics.profile.AggregatedProfileGroup;
import com.example.loganalytics.profile.ProfileGroup;
import com.example.loganalytics.profile.ProfileGroupMemberAccessor;

import java.util.List;
import java.util.function.Predicate;

public class DnsRequestEvent extends NetworkEvent {

    // field names for DNS Requests
    public static final String DNS_QUERY = "dns.query";
    public static final String DNS_ANSWERS = "dns.answers";
    public static final String DNS_FILTERED_IP_ENDING = "ips";
    public static final String DNS_IP_ANSWERS = String.join(".", DNS_ANSWERS, FilterListEnrichment.FILTER_FEATURE_NAME, DNS_FILTERED_IP_ENDING);
    public static final String DNS_RESPONSE_CODE_NAME = "dns.rcode_name";

    public static final String DNS_ANSWER_CITIES = String.join(".", DNS_IP_ANSWERS, IpCityCountrySetEnrichment.CITY_COUNTRY_FEATURE_NAME, IpCityCountrySetEnrichment.UNIQUE_CITIES_FIELD_ENDING); //dns.answers.filter.ips.city_country.unique_cities
    public static final String DNS_ANSWER_COUNTRIES = String.join(".", DNS_IP_ANSWERS, IpCityCountrySetEnrichment.CITY_COUNTRY_FEATURE_NAME, IpCityCountrySetEnrichment.UNIQUE_COUNTRIES_FIELD_ENDING);
    public static final String DNS_QUERY_TOP_LEVEL_DOMAIN = String.join(".", DNS_QUERY, EnrichmentReferenceDomainTransformations.DOMAIN_BREAKDOWN, EnrichmentReferenceDomainTransformations.TOP_LEVEL_DOMAIN_ENDING);
    public static final String DNS_QUERY_SECOND_LEVEL_DOMAIN = String.join(".", DNS_QUERY, EnrichmentReferenceDomainTransformations.DOMAIN_BREAKDOWN, EnrichmentReferenceDomainTransformations.SECOND_LEVEL_DOMAIN_ENDING);
    public static final String DNS_QUERY_TYPE = "dns.qtype_name";

    public static final String DNS_FINGERPRINT_PROFILE = "dns.fingerprint";
    public static final String DNS_FINGERPRINT_MINUTE_PROFILE = DNS_FINGERPRINT_PROFILE.concat(".").concat("minute");
    public static final String DNS_FINGERPRINT_HOUR_PROFILE = DNS_FINGERPRINT_PROFILE.concat(".").concat("hour");

    private static final LogEventFieldAccessor<String> DNS_QUERY_FIELD_ACCESSOR = new LogEventFieldAccessor<>(DNS_QUERY, DNS_FINGERPRINT_MINUTE_PROFILE);
    private static final LogEventFieldAccessor<String> DNS_QUERY_TYPE_FIELD_ACCESSOR = new LogEventFieldAccessor<>(DNS_QUERY_TYPE, DNS_FINGERPRINT_MINUTE_PROFILE);
    private static final LogEventFieldAccessor<String> IP_SRC_ADDR_FIELD_ACCESSOR = new LogEventFieldAccessor<>(IP_SRC_ADDR_FIELD, DNS_FINGERPRINT_MINUTE_PROFILE);
    private static final LogEventFieldAccessor<String> IP_DST_ADDR_FIELD_ACCESSOR = new LogEventFieldAccessor<>(IP_DST_ADDR_FIELD, DNS_FINGERPRINT_MINUTE_PROFILE);
    private static final LogEventFieldAccessor<String> DNS_QUERY_TOP_LEVEL_DOMAIN_FIELD_ACCESSOR = new LogEventFieldAccessor<>(DNS_QUERY_TOP_LEVEL_DOMAIN, DNS_FINGERPRINT_MINUTE_PROFILE);
    private static final LogEventFieldAccessor<String> DNS_QUERY_SECOND_LEVEL_DOMAIN_FIELD_ACCESSOR = new LogEventFieldAccessor<>(DNS_QUERY_SECOND_LEVEL_DOMAIN, DNS_FINGERPRINT_MINUTE_PROFILE);
    private static final LogEventFieldAccessor<String> DNS_RESPONSE_CODE_NAME_FIELD_ACCESSOR = new LogEventFieldAccessor<>(DNS_RESPONSE_CODE_NAME, DNS_FINGERPRINT_MINUTE_PROFILE);
    private static final LogEventFieldAccessor<List<String>> DNS_ANSWER_CITIES_FIELD_ACCESSOR = new LogEventFieldAccessor<>(DNS_ANSWER_CITIES, DNS_FINGERPRINT_MINUTE_PROFILE);
    private static final LogEventFieldAccessor<List<String>> DNS_ANSWER_COUNTRIES_FIELD_ACCESSOR = new LogEventFieldAccessor<>(DNS_ANSWER_COUNTRIES, DNS_FINGERPRINT_MINUTE_PROFILE);
    private static final LogEventFieldAccessor<List<String>> DNS_IP_ANSWERS_FIELD_ACCESSOR = new LogEventFieldAccessor<>(DNS_IP_ANSWERS, DNS_FINGERPRINT_MINUTE_PROFILE);

    public enum DnsFingerprintProfileMeasurements {
        P1,
        P2,
        P3,
        P4,
        P5,
        P6,
        P7,
        P8,
        P9,
        P10,
        P11,
        P12,
        P13,
        P14,
        P15,
        DISTINCT_ANSWERS,
        MINUTES_ACTIVE
    }


    @SuppressWarnings("Convert2Lambda")
    private static final Predicate<String> isMxQuery = new Predicate<String>() {
        @Override
        public boolean test(String s) {
            return s != null && s.equals("MX");
        }
    };

    @SuppressWarnings("Convert2Lambda")
    private static final Predicate<String> isPtrQuery = new Predicate<String>() {
        @Override
        public boolean test(String s) {
            return s != null && s.equals("PTR");
        }
    };

    @SuppressWarnings("Convert2Lambda")
    private static final Predicate<String> isNxDomain = new Predicate<String>() {
        @Override
        public boolean test(String s) {
            return s != null && s.equals("NXDOMAIN");
        }
    };

    public static ProfileGroup<LogEvent> createDnsFingerPrintMinuteProfileGroup() {

        ProfileGroup<LogEvent> fingerPrintProfileGroup = new ProfileGroup<>(DNS_FINGERPRINT_MINUTE_PROFILE, IP_SRC_ADDR_FIELD_ACCESSOR);

        fingerPrintProfileGroup.addCount(DnsFingerprintProfileMeasurements.P1.name()).
                addCountDistinctString(DnsFingerprintProfileMeasurements.P2.name(), DNS_QUERY_FIELD_ACCESSOR).
                addTopFrequency(DnsFingerprintProfileMeasurements.P3.name(), DNS_QUERY_FIELD_ACCESSOR).
                addCountIf(DnsFingerprintProfileMeasurements.P6.name(), DNS_QUERY_TYPE_FIELD_ACCESSOR, isMxQuery).
                addCountIf(DnsFingerprintProfileMeasurements.P7.name(), DNS_QUERY_TYPE_FIELD_ACCESSOR, isPtrQuery).
                addCountDistinctString(DnsFingerprintProfileMeasurements.P8.name(), IP_DST_ADDR_FIELD_ACCESSOR).
                addCountDistinctString(DnsFingerprintProfileMeasurements.P9.name(), DNS_QUERY_TOP_LEVEL_DOMAIN_FIELD_ACCESSOR).
                addCountDistinctString(DnsFingerprintProfileMeasurements.P10.name(), DNS_QUERY_SECOND_LEVEL_DOMAIN_FIELD_ACCESSOR).
                addCountIf(DnsFingerprintProfileMeasurements.P12.name(), DNS_RESPONSE_CODE_NAME_FIELD_ACCESSOR, isNxDomain).
                addCountDistinctStringList(DnsFingerprintProfileMeasurements.P13.name(), DNS_ANSWER_CITIES_FIELD_ACCESSOR).
                addCountDistinctStringList(DnsFingerprintProfileMeasurements.P14.name(), DNS_ANSWER_COUNTRIES_FIELD_ACCESSOR).
                addCountDistinctStringList(DnsFingerprintProfileMeasurements.DISTINCT_ANSWERS.name(), DNS_IP_ANSWERS_FIELD_ACCESSOR);

        return fingerPrintProfileGroup;

    }

    public static AggregatedProfileGroup<LogEvent> createDnsFingerPrintHourProfileGroup() {

        AggregatedProfileGroup<LogEvent> hourDnsFingerprintProfileGroup = new AggregatedProfileGroup<>(DNS_FINGERPRINT_HOUR_PROFILE, createDnsFingerPrintMinuteProfileGroup());

        hourDnsFingerprintProfileGroup.addCount(DnsFingerprintProfileMeasurements.MINUTES_ACTIVE.name()).
                addMaximum(DnsFingerprintProfileMeasurements.P5.name(), new ProfileGroupMemberAccessor<>(DnsFingerprintProfileMeasurements.P1.name())).
                addRatio(DnsFingerprintProfileMeasurements.P4.name(), DnsFingerprintProfileMeasurements.P1.name(), DnsFingerprintProfileMeasurements.MINUTES_ACTIVE.name()).
                addRatio(DnsFingerprintProfileMeasurements.P11.name(), DnsFingerprintProfileMeasurements.P1.name(), DnsFingerprintProfileMeasurements.P2.name()).
                addRatio(DnsFingerprintProfileMeasurements.P15.name(), DnsFingerprintProfileMeasurements.P2.name(), DnsFingerprintProfileMeasurements.DISTINCT_ANSWERS.name());
        return hourDnsFingerprintProfileGroup;

    }

    public DnsRequestEvent(LogEvent event) {
        super(event.getFields());
    }


    public String getQuery() {
        return (String)getField(DNS_QUERY);
    }

    public String getResponseCode() {
        return (String)getField(DNS_RESPONSE_CODE_NAME);
    }

    public List<String> getIpAnswers() {
        //noinspection unchecked
        return (List<String>)getField(DNS_IP_ANSWERS);
    }

    public List<String>  getAnswerCities() {
        //noinspection unchecked
        return (List<String>)getField(DNS_ANSWER_CITIES);
    }

    public List<String> getAnswerCountries() {
        //noinspection unchecked
        return (List<String>)getField(DNS_ANSWER_COUNTRIES);
    }

    public String getQueryTopLevelDomain() {
        return (String)getField(DNS_QUERY_TOP_LEVEL_DOMAIN);
    }

    public String getQuerySecondLevelDomain() {
        return (String)getField(DNS_QUERY_SECOND_LEVEL_DOMAIN);
    }

    public String getQueryType() {
        return (String)getField(DNS_QUERY_TYPE);
    }

}
