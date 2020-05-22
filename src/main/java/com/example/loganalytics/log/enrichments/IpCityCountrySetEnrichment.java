package com.example.loganalytics.log.enrichments;

import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceDataSource;
import com.example.loganalytics.log.enrichments.reference.EnrichmentReferenceFiles;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Country;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class IpCityCountrySetEnrichment implements EnrichmentReferenceDataSource {
    public static final String UNIQUE_CITIES_FIELD_ENDING = "unique_cities";
    public static final String UNIQUE_COUNTRIES_FIELD_ENDING = "unique_countries";
    public static final String CITY_COUNTRY_FEATURE_NAME = "city_country";

    private final DatabaseReader cityDatabase;


    private IpCityCountrySetEnrichment(DatabaseReader cityDatabase) {
        this.cityDatabase = cityDatabase;
    }

    public static IpCityCountrySetEnrichment create(EnrichmentReferenceFiles referenceFiles) throws IOException {
        return new IpCityCountrySetEnrichment(referenceFiles.getGeoCityDatabase());
    }

    @Override
    public Map<String, Object> lookup(String enrichmentReferenceData, Object fieldValue) throws Exception {
        Map<String, Object> fields = new HashMap<>();

        if (fieldValue != null) {
            // the enrichment framework checks types before it calls the enrichment
            @SuppressWarnings("unchecked")
            List<String> fieldArray = (List<String>)(fieldValue);
            Set<String> cities = new HashSet<>();
            Set<String> countries = new HashSet<>();
            for (String element : fieldArray) {
                InetAddress ipAddress = InetAddress.getByName(element);
                cityDatabase.tryCity(ipAddress).ifPresent(response -> addLocations(cities, countries, response));
            }
            fields.put(UNIQUE_CITIES_FIELD_ENDING, new ArrayList<>(cities));
            fields.put(UNIQUE_COUNTRIES_FIELD_ENDING, new ArrayList<>(countries));
        }

        return fields;

    }

    private void addLocations(Set<String> cities, Set<String> countries, CityResponse cityResponse) {
        City city = cityResponse.getCity();
        if (city != null && StringUtils.isNotBlank(city.getName())) {
            cities.add(city.getName());
        }

        Country country = cityResponse.getCountry();
        if (country != null && StringUtils.isNotBlank(country.getIsoCode())) {
            countries.add(country.getIsoCode());
        }
    }
}
