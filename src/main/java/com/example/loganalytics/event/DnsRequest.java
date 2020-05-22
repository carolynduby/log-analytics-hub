package com.example.loganalytics.event;

import com.example.loganalytics.log.enrichments.FilterListEnrichment;
import com.example.loganalytics.log.enrichments.IpCityCountrySetEnrichment;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceDomainTransformations;
import com.example.loganalytics.profile.ProfileGroup;

import java.util.List;
import java.util.function.Predicate;

public class DnsRequest extends LogEvent {

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
        DISTINCT_ANSWERS_PROFILE_NAME
    }

    public static ProfileGroup createDnsFingerPrintProfileGroup() {
        ProfileGroup fingerPrintProfileGroup = new ProfileGroup("dns.fingerprint", IP_SRC_ADDR_FIELD);

        fingerPrintProfileGroup.addCount(DnsFingerprintProfileMeasurements.P1.name(), DNS_QUERY).
                addCountDistinct(DnsFingerprintProfileMeasurements.P2.name(), DNS_QUERY, false).
                addTopFrequency(DnsFingerprintProfileMeasurements.P3.name(), DNS_QUERY).
                // set P4 and P5
                addCountIf(DnsFingerprintProfileMeasurements.P6.name(), DNS_QUERY_TYPE, Predicate.isEqual("MX")).
                addCountIf(DnsFingerprintProfileMeasurements.P7.name(), DNS_QUERY_TYPE, Predicate.isEqual("PTR")).
                addCountDistinct(DnsFingerprintProfileMeasurements.P8.name(), IP_DST_ADDR_FIELD, false).
                addCountDistinct(DnsFingerprintProfileMeasurements.P9.name(), DNS_QUERY_TOP_LEVEL_DOMAIN, false).
                addCountDistinct(DnsFingerprintProfileMeasurements.P10.name(), DNS_QUERY_SECOND_LEVEL_DOMAIN, false).
                addRatio(DnsFingerprintProfileMeasurements.P11.name(), DnsFingerprintProfileMeasurements.P1.name(), DnsFingerprintProfileMeasurements.P2.name()).
                addCountIf(DnsFingerprintProfileMeasurements.P12.name(), DNS_RESPONSE_CODE_NAME, Predicate.isEqual("NXDOMAIN")).
                addCountDistinct(DnsFingerprintProfileMeasurements.P13.name(), DNS_ANSWER_CITIES, true).
                addCountDistinct(DnsFingerprintProfileMeasurements.P14.name(), DNS_ANSWER_COUNTRIES, true).
                addCountDistinct(DnsFingerprintProfileMeasurements.DISTINCT_ANSWERS_PROFILE_NAME.name(), DNS_IP_ANSWERS, true).
                addRatio(DnsFingerprintProfileMeasurements.P15.name(), DnsFingerprintProfileMeasurements.P2.name(), DnsFingerprintProfileMeasurements.DISTINCT_ANSWERS_PROFILE_NAME.name());
        return fingerPrintProfileGroup;

    }

    public DnsRequest(LogEvent event) {
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
